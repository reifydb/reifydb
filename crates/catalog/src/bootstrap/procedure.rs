// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::PathBuf,
	sync::atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{ExternWasmModuleId, Procedure, ProcedureParam},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType};

use super::ensure_namespace;
use crate::{Result, cache::CatalogCache, catalog::Catalog};

static EPHEMERAL_ID: AtomicU64 = AtomicU64::new(ProcedureId::SYSTEM_RESERVED_START);

#[derive(Debug, Clone)]
pub enum EphemeralProcedureDescriptor {
	InProcess {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
	},
	ExternC {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
		library_path: PathBuf,
		entry_symbol: String,
	},
	ExternWasm {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
		module_id: ExternWasmModuleId,
	},
}

pub fn load_ephemeral_procedures(
	catalog: &CatalogCache,
	descriptors: Vec<EphemeralProcedureDescriptor>,
	version: CommitVersion,
) -> Result<()> {
	EPHEMERAL_ID.store(ProcedureId::SYSTEM_RESERVED_START, Ordering::SeqCst);

	let mut to_clear = Vec::new();
	for entry in catalog.procedures.iter() {
		if let Some(p) = entry.value().get_latest()
			&& !p.is_persistent()
		{
			to_clear.push(p.id());
		}
	}
	for id in to_clear {
		catalog.set_procedure(id, version, None);
	}

	for desc in descriptors {
		let id = ProcedureId::ephemeral(EPHEMERAL_ID.fetch_add(1, Ordering::SeqCst));
		let proc = match desc {
			EphemeralProcedureDescriptor::InProcess {
				namespace,
				name,
				params,
				return_type,
				handler_name,
			} => Procedure::InProcess {
				id,
				namespace,
				name,
				params,
				return_type,
				handler_name,
			},
			EphemeralProcedureDescriptor::ExternC {
				namespace,
				name,
				params,
				return_type,
				handler_name,
				library_path,
				entry_symbol,
			} => Procedure::ExternC {
				id,
				namespace,
				name,
				params,
				return_type,
				handler_name,
				library_path,
				entry_symbol,
			},
			EphemeralProcedureDescriptor::ExternWasm {
				namespace,
				name,
				params,
				return_type,
				handler_name,
				module_id,
			} => Procedure::ExternWasm {
				id,
				namespace,
				name,
				params,
				return_type,
				handler_name,
				module_id,
			},
		};
		catalog.set_procedure(id, version, Some(proc));
	}

	Ok(())
}

pub fn bootstrap_system_procedures(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone());

	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_PROCEDURES,
		"system::procedures",
		"procedures",
		NamespaceId::SYSTEM,
	)?;

	let rql_namespace =
		ensure_namespace(&catalog_api, &mut admin, NamespaceId::RQL, "rql", "rql", NamespaceId::ROOT)?;

	let rql_query_param = || ProcedureParam {
		name: "query".to_string(),
		param_type: TypeConstraint::unconstrained(ValueType::Utf8),
	};

	let graphql_namespace = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::GRAPHQL,
		"graphql",
		"graphql",
		NamespaceId::ROOT,
	)?;

	let descriptors = vec![
		EphemeralProcedureDescriptor::InProcess {
			namespace: ensure_namespace(
				&catalog_api,
				&mut admin,
				NamespaceId::SYSTEM_CONFIG,
				"system::config",
				"config",
				NamespaceId::SYSTEM,
			)?,
			name: "set".to_string(),
			params: vec![
				ProcedureParam {
					name: "key".to_string(),
					param_type: TypeConstraint::unconstrained(ValueType::Utf8),
				},
				ProcedureParam {
					name: "value".to_string(),
					param_type: TypeConstraint::unconstrained(ValueType::Any),
				},
			],
			return_type: None,
			handler_name: "system::config::set".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: ensure_namespace(
				&catalog_api,
				&mut admin,
				NamespaceId::STORAGE,
				"storage",
				"storage",
				NamespaceId::ROOT,
			)?,
			name: "advance".to_string(),
			params: vec![
				ProcedureParam {
					name: "objects".to_string(),
					param_type: TypeConstraint::unconstrained(ValueType::Any),
				},
				ProcedureParam {
					name: "complete_through".to_string(),
					param_type: TypeConstraint::unconstrained(ValueType::DateTime),
				},
			],
			return_type: None,
			handler_name: "storage::advance".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: rql_namespace,
			name: "tokenize".to_string(),
			params: vec![rql_query_param()],
			return_type: None,
			handler_name: "rql::tokenize".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: rql_namespace,
			name: "ast".to_string(),
			params: vec![rql_query_param()],
			return_type: None,
			handler_name: "rql::ast".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: rql_namespace,
			name: "logical".to_string(),
			params: vec![rql_query_param()],
			return_type: None,
			handler_name: "rql::logical".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: rql_namespace,
			name: "explain".to_string(),
			params: vec![rql_query_param()],
			return_type: None,
			handler_name: "rql::explain".to_string(),
		},
		EphemeralProcedureDescriptor::InProcess {
			namespace: graphql_namespace,
			name: "explain".to_string(),
			params: vec![ProcedureParam {
				name: "query".to_string(),
				param_type: TypeConstraint::unconstrained(ValueType::Utf8),
			}],
			return_type: None,
			handler_name: "graphql::explain".to_string(),
		},
	];

	let commit_version = admin.commit()?;

	load_ephemeral_procedures(catalog, descriptors, commit_version)?;

	Ok(())
}
