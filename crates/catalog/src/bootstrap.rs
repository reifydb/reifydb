// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		id::NamespaceId,
		procedure::{ProcedureParamDef, ProcedureTrigger},
	},
};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, r#type::Type},
};

use crate::{
	Result,
	catalog::{Catalog, namespace::NamespaceToCreate, procedure::ProcedureToCreate},
	materialized::{MaterializedCatalog, load::MaterializedCatalogLoader},
	schema::{SchemaRegistry, load::SchemaRegistryLoader},
};

/// Load all catalog data from storage into MaterializedCatalog.
pub fn load_materialized_catalog(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
) -> Result<()> {
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	MaterializedCatalogLoader::load_all(&mut Transaction::Query(&mut qt), catalog)?;
	Ok(())
}

/// Write registered config defaults to storage for keys not yet stored.
pub fn bootstrap_config_defaults(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	eventbus: &EventBus,
) -> Result<()> {
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
	)?;
	MaterializedCatalogLoader::bootstrap_missing_defaults(&mut admin, catalog)?;
	admin.commit()?;
	Ok(())
}

/// Create `system::config` namespace and `system::config::set` procedure.
///
/// Called on every startup since procedures are not persisted to storage.
pub fn bootstrap_system_procedures(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	schema_registry: &SchemaRegistry,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone(), schema_registry.clone());
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
	)?;

	// Ensure the system::config sub-namespace exists (persisted to storage).
	// On first boot it won't exist; on subsequent boots it's already loaded into
	// MaterializedCatalog by load_namespaces.
	let ns_id = match catalog_api.find_namespace_by_path(&mut Transaction::Admin(&mut admin), "system::config")? {
		Some(ns) => ns.id(),
		None => {
			let ns = catalog_api.create_namespace(
				&mut admin,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "system::config".to_string(),
					local_name: "config".to_string(),
					parent_id: NamespaceId(1),
					grpc: None,
				},
			)?;
			ns.id()
		}
	};

	// Procedures are not persisted to storage, so create the procedure on every startup.
	// The ID is allocated from the sequence (persistent) but the procedure data itself
	// lives only in MaterializedCatalog for this session.
	let proc_def = catalog_api.create_procedure(
		&mut admin,
		ProcedureToCreate {
			name: Fragment::internal("set"),
			namespace: ns_id,
			params: vec![
				ProcedureParamDef {
					name: "key".to_string(),
					param_type: TypeConstraint::unconstrained(Type::Utf8),
				},
				ProcedureParamDef {
					name: "value".to_string(),
					param_type: TypeConstraint::unconstrained(Type::Utf8),
				},
			],
			return_type: None,
			body: String::new(),
			trigger: ProcedureTrigger::NativeCall {
				native_name: "system::config::set".to_string(),
			},
			is_test: false,
		},
	)?;

	let commit_version = admin.commit()?;

	// Procedures are not loaded from storage at startup, so update MaterializedCatalog
	// directly so this procedure is visible to CALL statements in the current session.
	catalog.set_procedure(proc_def.id, commit_version, Some(proc_def));

	Ok(())
}

/// Load schemas from storage into SchemaRegistry.
pub fn load_schema_registry(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	registry: &SchemaRegistry,
) -> Result<()> {
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	SchemaRegistryLoader::load_all(&mut Transaction::Query(&mut qt), registry)?;
	Ok(())
}
