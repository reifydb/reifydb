// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		id::NamespaceId,
		procedure::{ProcedureParam, ProcedureTrigger},
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
	CatalogStore, Result,
	catalog::{Catalog, namespace::NamespaceToCreate, procedure::ProcedureToCreate},
	materialized::{
		MaterializedCatalog,
		load::{MaterializedCatalogLoader, identity::load_identities},
	},
	shape::{RowShapeRegistry, load::RowShapeRegistryLoader},
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
pub fn bootstrap_configaults(
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
	row_shape_registry: &RowShapeRegistry,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone(), row_shape_registry.clone());
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
					token: None,
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
				ProcedureParam {
					name: "key".to_string(),
					param_type: TypeConstraint::unconstrained(Type::Utf8),
				},
				ProcedureParam {
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

/// Bootstrap the root identity in the catalog.
///
/// Creates an identity named "root" with `IdentityId::root()`.
/// This makes root a real catalog identity that can have authentication attached
/// (e.g., `CREATE AUTHENTICATION FOR root { method: token; token: '...' }`).
///
/// Skips creation if the root identity already exists.
pub fn bootstrap_root_identity(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	eventbus: &EventBus,
) -> Result<()> {
	// Check if root identity already exists via storage scan
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	if CatalogStore::find_identity_by_name(&mut Transaction::Query(&mut qt), "root")?.is_some() {
		return Ok(());
	}
	drop(qt);

	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
	)?;

	CatalogStore::create_identity_with_id(&mut admin, "root", IdentityId::root())?;
	admin.commit()?;

	// Reload materialized catalog to pick up the new identity
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	load_identities(&mut Transaction::Query(&mut qt), catalog)?;

	Ok(())
}

/// Load shapes from storage into RowShapeRegistry.
pub fn load_shape_registry(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	registry: &RowShapeRegistry,
) -> Result<()> {
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	RowShapeRegistryLoader::load_all(&mut Transaction::Query(&mut qt), registry)?;
	Ok(())
}
