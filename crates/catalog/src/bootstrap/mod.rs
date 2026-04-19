// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Bootstrap the catalog: load materialized state from storage, then ensure
//! system-owned objects (root identity, system procedures, metric ring buffers)
//! exist. Skipped on replicas — they receive these via replication.

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
use reifydb_transaction::{
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_type::value::identity::IdentityId;
use tracing::info;

use crate::{
	Result,
	catalog::{Catalog, namespace::NamespaceToCreate},
	materialized::{MaterializedCatalog, load::MaterializedCatalogLoader},
};

pub mod binding;
pub mod identity;
pub mod metric;
pub mod procedure;

/// Bootstrap system objects: root identity, system procedures, metric ring buffers.
///
/// Must be called AFTER `load_materialized_catalog()`.
/// Callers that need CDC to capture bootstrap commits should ensure the CDC
/// producer is active before calling this function.
///
/// Skipped on replicas (they receive system objects via replication).
pub fn bootstrap_system_objects(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	eventbus: &EventBus,
) -> Result<()> {
	identity::bootstrap_root_identity(multi, single, catalog, eventbus)?;
	procedure::bootstrap_system_procedures(multi, single, catalog, eventbus)?;
	binding::bootstrap_system_bindings(multi, single, catalog, eventbus)?;
	metric::bootstrap_metric_ringbuffers(multi, single, catalog, eventbus)?;
	Ok(())
}

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

/// Find a namespace by its full `::`-separated path, or create it with the given id
/// and parent if it doesn't exist. Returns the resolved namespace id.
pub(crate) fn ensure_namespace(
	catalog_api: &Catalog,
	admin: &mut AdminTransaction,
	id: NamespaceId,
	path: &str,
	local_name: &str,
	parent_id: NamespaceId,
) -> Result<NamespaceId> {
	if let Some(ns) = catalog_api.find_namespace_by_path(&mut Transaction::Admin(admin), path)? {
		return Ok(ns.id());
	}

	let ns = catalog_api.create_namespace_with_id(
		admin,
		id,
		NamespaceToCreate {
			namespace_fragment: None,
			name: path.to_string(),
			local_name: local_name.to_string(),
			parent_id,
			token: None,
			grpc: None,
		},
	)?;
	info!("Created {} namespace", path);
	Ok(ns.id())
}
