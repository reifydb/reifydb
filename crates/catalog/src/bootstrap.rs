// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		id::{ColumnId, NamespaceId, ProcedureId, RingBufferId},
		procedure::{ProcedureParam, ProcedureTrigger},
	},
};
use reifydb_runtime::context::clock::Clock;
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
use tracing::info;

use crate::{
	CatalogStore, Result,
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		procedure::ProcedureToCreate,
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
	},
	materialized::{
		MaterializedCatalog,
		load::{MaterializedCatalogLoader, identity::load_identities},
	},
};

/// Bootstrap the entire database: load catalog, create system objects.
///
/// This is the single entry point for all database bootstrapping.
/// Called during `DatabaseBuilder::build()`.
pub fn bootstrap_database(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	eventbus: &EventBus,
) -> Result<()> {
	load_materialized_catalog(multi, single, catalog)?;
	bootstrap_root_identity(multi, single, catalog, eventbus)?;
	bootstrap_system_procedures(multi, single, catalog, eventbus)?;
	bootstrap_metric_ringbuffers(multi, single, catalog, eventbus)?;
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

/// Create `system::config` namespace and `system::config::set` procedure.
///
/// Called on every startup since procedures are not persisted to storage.
pub fn bootstrap_system_procedures(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
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

	// Ensure the system::config sub-namespace exists (persisted to storage).
	// On first boot it won't exist; on subsequent boots it's already loaded into
	// MaterializedCatalog by load_namespaces.
	let ns_id = match catalog_api.find_namespace_by_path(&mut Transaction::Admin(&mut admin), "system::config")? {
		Some(ns) => ns.id(),
		None => {
			let ns = catalog_api.create_namespace_with_id(
				&mut admin,
				NamespaceId::SYSTEM_CONFIG,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "system::config".to_string(),
					local_name: "config".to_string(),
					parent_id: NamespaceId::SYSTEM,
					token: None,
					grpc: None,
				},
			)?;
			ns.id()
		}
	};

	// Procedures are not persisted to storage, so create the procedure on every startup.
	// The procedure data lives only in MaterializedCatalog for this session.
	let proc_def = catalog_api.create_procedure_with_id(
		&mut admin,
		ProcedureId::SYSTEM_CONFIG_SET,
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
		Clock::Real,
	)?;

	CatalogStore::create_identity_with_id(&mut admin, "root", IdentityId::root())?;
	admin.commit()?;

	// Reload materialized catalog to pick up the new identity
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	load_identities(&mut Transaction::Query(&mut qt), catalog)?;

	Ok(())
}

/// Bootstrap the `system::metrics` namespace and its ring buffers.
///
/// Idempotent: skips creation if the namespace or ring buffers already exist.
fn bootstrap_metric_ringbuffers(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
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

	// Find or create the system::metrics namespace
	let ns_id = match catalog_api.find_namespace_by_path(&mut Transaction::Admin(&mut admin), "system::metrics")? {
		Some(ns) => ns.id(),
		None => {
			let ns = catalog_api.create_namespace_with_id(
				&mut admin,
				NamespaceId::SYSTEM_METRICS,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "system::metrics".to_string(),
					local_name: "metrics".to_string(),
					parent_id: NamespaceId::SYSTEM,
					token: None,
					grpc: None,
				},
			)?;
			info!("Created system::metrics namespace");
			ns.id()
		}
	};

	// Create request_history ring buffer if it doesn't exist
	if catalog_api.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "request_history")?.is_none()
	{
		catalog_api.create_ringbuffer_with_id(
			&mut admin,
			RingBufferId::REQUEST_HISTORY,
			metric_request_history_schema(ns_id),
			&[
				ColumnId::REQUEST_HISTORY_TIMESTAMP,
				ColumnId::REQUEST_HISTORY_OPERATION,
				ColumnId::REQUEST_HISTORY_FINGERPRINT,
				ColumnId::REQUEST_HISTORY_TOTAL_DURATION,
				ColumnId::REQUEST_HISTORY_COMPUTE_DURATION,
				ColumnId::REQUEST_HISTORY_SUCCESS,
				ColumnId::REQUEST_HISTORY_STATEMENT_COUNT,
				ColumnId::REQUEST_HISTORY_NORMALIZED_RQL,
			],
		)?;
		info!("Created system::metrics::request_history ring buffer");
	}

	// Create statement_stats ring buffer if it doesn't exist
	if catalog_api.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "statement_stats")?.is_none()
	{
		catalog_api.create_ringbuffer_with_id(
			&mut admin,
			RingBufferId::STATEMENT_STATS,
			metric_statement_stats_schema(ns_id),
			&[
				ColumnId::STATEMENT_STATS_SNAPSHOT_TIMESTAMP,
				ColumnId::STATEMENT_STATS_FINGERPRINT,
				ColumnId::STATEMENT_STATS_NORMALIZED_RQL,
				ColumnId::STATEMENT_STATS_CALLS,
				ColumnId::STATEMENT_STATS_TOTAL_DURATION,
				ColumnId::STATEMENT_STATS_MEAN_DURATION,
				ColumnId::STATEMENT_STATS_MAX_DURATION,
				ColumnId::STATEMENT_STATS_MIN_DURATION,
				ColumnId::STATEMENT_STATS_TOTAL_ROWS,
				ColumnId::STATEMENT_STATS_ERRORS,
			],
		)?;
		info!("Created system::metrics::statement_stats ring buffer");
	}

	admin.commit()?;

	Ok(())
}

const REQUEST_HISTORY_CAPACITY: u64 = 10_000;
const STATEMENT_STATS_CAPACITY: u64 = 5_000;

fn metric_col(name: &str, ty: Type) -> RingBufferColumnToCreate {
	RingBufferColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn metric_request_history_schema(namespace: NamespaceId) -> RingBufferToCreate {
	RingBufferToCreate {
		name: Fragment::internal("request_history"),
		namespace,
		columns: vec![
			metric_col("timestamp", Type::DateTime),
			metric_col("operation", Type::Utf8),
			metric_col("fingerprint", Type::Utf8),
			metric_col("total_duration", Type::Duration),
			metric_col("compute_duration", Type::Duration),
			metric_col("success", Type::Boolean),
			metric_col("statement_count", Type::Int8),
			metric_col("normalized_rql", Type::Utf8),
		],
		capacity: REQUEST_HISTORY_CAPACITY,
		partition_by: vec![],
	}
}

fn metric_statement_stats_schema(namespace: NamespaceId) -> RingBufferToCreate {
	RingBufferToCreate {
		name: Fragment::internal("statement_stats"),
		namespace,
		columns: vec![
			metric_col("snapshot_timestamp", Type::DateTime),
			metric_col("fingerprint", Type::Utf8),
			metric_col("normalized_rql", Type::Utf8),
			metric_col("calls", Type::Int8),
			metric_col("total_duration", Type::Duration),
			metric_col("mean_duration", Type::Duration),
			metric_col("max_duration", Type::Duration),
			metric_col("min_duration", Type::Duration),
			metric_col("total_rows", Type::Int8),
			metric_col("errors", Type::Int8),
		],
		capacity: STATEMENT_STATS_CAPACITY,
		partition_by: vec![],
	}
}
