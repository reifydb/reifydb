// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, RingBufferId},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, r#type::Type},
};
use tracing::info;

use super::ensure_namespace;
use crate::{
	Result,
	catalog::{
		Catalog,
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
	},
	materialized::MaterializedCatalog,
};

const REQUEST_HISTORY_CAPACITY: u64 = 10_000;
const STATEMENT_STATS_CAPACITY: u64 = 5_000;

/// Bootstrap the `system::metrics` namespace and its ring buffers.
///
/// Idempotent: skips creation if the namespace or ring buffers already exist.
pub fn bootstrap_metric_ringbuffers(
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

	let ns_id = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS,
		"system::metrics",
		"metrics",
		NamespaceId::SYSTEM,
	)?;
	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_STORAGE,
		"system::metrics::storage",
		"storage",
		NamespaceId::SYSTEM_METRICS,
	)?;
	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_CDC,
		"system::metrics::cdc",
		"cdc",
		NamespaceId::SYSTEM_METRICS,
	)?;

	if catalog_api.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "request_history")?.is_none()
	{
		catalog_api.create_ringbuffer_with_id(
			&mut admin,
			RingBufferId::REQUEST_HISTORY,
			request_history_schema(ns_id),
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

	if catalog_api.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "statement_stats")?.is_none()
	{
		catalog_api.create_ringbuffer_with_id(
			&mut admin,
			RingBufferId::STATEMENT_STATS,
			statement_stats_schema(ns_id),
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

fn request_history_schema(namespace: NamespaceId) -> RingBufferToCreate {
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
		underlying: false,
	}
}

fn statement_stats_schema(namespace: NamespaceId) -> RingBufferToCreate {
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
		underlying: false,
	}
}
