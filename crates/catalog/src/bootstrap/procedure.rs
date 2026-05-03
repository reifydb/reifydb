// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	path::PathBuf,
	sync::atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, WasmModuleId},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_type::value::{constraint::TypeConstraint, identity::IdentityId, r#type::Type};

use super::ensure_namespace;
use crate::{Result, cache::CatalogCache, catalog::Catalog};

static EPHEMERAL_ID: AtomicU64 = AtomicU64::new(ProcedureId::SYSTEM_RESERVED_START);

#[derive(Debug, Clone)]
pub enum EphemeralProcedureDescriptor {
	Native {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
	},
	Ffi {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
		library_path: PathBuf,
		entry_symbol: String,
	},
	Wasm {
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
		module_id: WasmModuleId,
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
			EphemeralProcedureDescriptor::Native {
				namespace,
				name,
				params,
				return_type,
				native_name,
			} => Procedure::Native {
				id,
				namespace,
				name,
				params,
				return_type,
				native_name,
			},
			EphemeralProcedureDescriptor::Ffi {
				namespace,
				name,
				params,
				return_type,
				native_name,
				library_path,
				entry_symbol,
			} => Procedure::Ffi {
				id,
				namespace,
				name,
				params,
				return_type,
				native_name,
				library_path,
				entry_symbol,
			},
			EphemeralProcedureDescriptor::Wasm {
				namespace,
				name,
				params,
				return_type,
				native_name,
				module_id,
			} => Procedure::Wasm {
				id,
				namespace,
				name,
				params,
				return_type,
				native_name,
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

	let descriptors = vec![EphemeralProcedureDescriptor::Native {
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
				param_type: TypeConstraint::unconstrained(Type::Utf8),
			},
			ProcedureParam {
				name: "value".to_string(),
				param_type: TypeConstraint::unconstrained(Type::Any),
			},
		],
		return_type: None,
		native_name: "system::config::set".to_string(),
	}];

	let commit_version = admin.commit()?;

	load_ephemeral_procedures(catalog, descriptors, commit_version)?;

	Ok(())
}
