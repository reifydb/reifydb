// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf};

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
use crate::{Result, cache::CatalogCache, catalog::Catalog, error::CatalogError};

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

impl EphemeralProcedureDescriptor {
	pub fn namespace(&self) -> NamespaceId {
		match self {
			Self::InProcess {
				namespace,
				..
			}
			| Self::ExternC {
				namespace,
				..
			}
			| Self::ExternWasm {
				namespace,
				..
			} => *namespace,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			Self::InProcess {
				name,
				..
			}
			| Self::ExternC {
				name,
				..
			}
			| Self::ExternWasm {
				name,
				..
			} => name,
		}
	}
}

pub fn load_ephemeral_procedures(
	catalog: &CatalogCache,
	descriptors: Vec<EphemeralProcedureDescriptor>,
	version: CommitVersion,
) -> Result<()> {
	let mut seen: HashMap<ProcedureId, String> = HashMap::new();

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
		let id = ProcedureId::ephemeral_of(desc.namespace(), desc.name());
		let qualified = format!("{}::{}", desc.namespace(), desc.name());
		if let Some(first) = seen.insert(id, qualified.clone()) {
			return Err(CatalogError::EphemeralProcedureIdCollision {
				first,
				second: qualified,
				id: id.into(),
			}
			.into());
		}

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

#[cfg(test)]
mod tests {
	use super::*;

	fn descriptor(namespace: NamespaceId, name: &str) -> EphemeralProcedureDescriptor {
		EphemeralProcedureDescriptor::InProcess {
			namespace,
			name: name.to_string(),
			params: vec![],
			return_type: None,
			handler_name: format!("test::{}", name),
		}
	}

	#[test]
	fn ephemeral_id_is_stable_when_a_descriptor_is_inserted_ahead() {
		let catalog = CatalogCache::new();
		let ns = NamespaceId(9001);

		load_ephemeral_procedures(&catalog, vec![descriptor(ns, "refund")], CommitVersion(1)).unwrap();
		let before = catalog.find_procedure_by_name(ns, "refund").unwrap().id();

		load_ephemeral_procedures(
			&catalog,
			vec![descriptor(ns, "audit"), descriptor(ns, "refund")],
			CommitVersion(2),
		)
		.unwrap();
		let after = catalog.find_procedure_by_name(ns, "refund").unwrap().id();

		assert_eq!(
			before, after,
			"a persisted binding stores this id; if it moves, the binding dispatches to whichever procedure inherited the slot"
		);
	}

	#[test]
	fn ephemeral_ids_are_stable_across_a_reordered_load() {
		let catalog = CatalogCache::new();
		let ns = NamespaceId(9002);
		let names = ["refund", "audit", "settle"];

		load_ephemeral_procedures(
			&catalog,
			names.iter().map(|n| descriptor(ns, n)).collect(),
			CommitVersion(1),
		)
		.unwrap();
		let before: Vec<_> =
			names.iter().map(|n| catalog.find_procedure_by_name(ns, n).unwrap().id()).collect();

		load_ephemeral_procedures(
			&catalog,
			names.iter().rev().map(|n| descriptor(ns, n)).collect(),
			CommitVersion(2),
		)
		.unwrap();
		let after: Vec<_> = names.iter().map(|n| catalog.find_procedure_by_name(ns, n).unwrap().id()).collect();

		assert_eq!(before, after, "registration order must not decide an id that outlives the process");
	}

	#[test]
	fn a_descriptor_dropped_from_the_load_stops_resolving() {
		let catalog = CatalogCache::new();
		let ns = NamespaceId(9003);

		load_ephemeral_procedures(
			&catalog,
			vec![descriptor(ns, "refund"), descriptor(ns, "audit")],
			CommitVersion(1),
		)
		.unwrap();
		let refund = catalog.find_procedure_by_name(ns, "refund").unwrap().id();

		load_ephemeral_procedures(&catalog, vec![descriptor(ns, "audit")], CommitVersion(2)).unwrap();

		assert!(
			catalog.find_procedure(refund).is_none(),
			"a binding whose procedure is gone must fail to resolve, never land on the survivor"
		);
		assert_ne!(
			catalog.find_procedure_by_name(ns, "audit").unwrap().id(),
			refund,
			"the surviving procedure must not inherit the removed id"
		);
	}

	#[test]
	fn reloading_the_same_descriptors_is_idempotent() {
		let catalog = CatalogCache::new();
		let ns = NamespaceId(9004);
		let load = || vec![descriptor(ns, "refund"), descriptor(ns, "audit")];

		load_ephemeral_procedures(&catalog, load(), CommitVersion(1)).unwrap();
		let first = catalog.find_procedure_by_name(ns, "refund").unwrap();

		load_ephemeral_procedures(&catalog, load(), CommitVersion(2)).unwrap();
		let second = catalog.find_procedure_by_name(ns, "refund").unwrap();

		assert_eq!(first, second, "an unchanged registration set must not churn the cache on reload");
	}

	#[test]
	fn registering_one_name_twice_fails_loudly() {
		let catalog = CatalogCache::new();
		let ns = NamespaceId(9005);

		let err = load_ephemeral_procedures(
			&catalog,
			vec![descriptor(ns, "refund"), descriptor(ns, "refund")],
			CommitVersion(1),
		)
		.expect_err("a duplicate registration must fail rather than leave an ambiguous name lookup");

		assert!(err.to_string().contains("refund"), "the failure must name the colliding procedure: {}", err);
	}
}
