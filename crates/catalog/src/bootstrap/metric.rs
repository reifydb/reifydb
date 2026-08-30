// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	event::EventBus,
	interface::catalog::{
		id::{ColumnId, NamespaceId, RingBufferId, SeriesId},
		series::{SeriesKey, TimestampPrecision},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};
use tracing::info;

use super::{ensure_namespace, series_col};
use crate::{
	Result,
	cache::CatalogCache,
	catalog::{
		Catalog,
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
		series::{SeriesColumnToCreate, SeriesToCreate},
	},
};

const REQUEST_HISTORY_CAPACITY: u64 = 10_000;
const STATEMENT_STATS_CAPACITY: u64 = 5_000;

pub fn bootstrap_metric_ringbuffers(
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

	let ns_id = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS,
		"system::metrics",
		"metrics",
		NamespaceId::SYSTEM,
	)?;
	let storage_ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_STORAGE,
		"system::metrics::storage",
		"storage",
		NamespaceId::SYSTEM_METRICS,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		storage_ns,
		"system::metrics::storage",
		SeriesId::STORAGE_SNAPSHOTS,
		storage_snapshot_columns(),
		&ColumnId::STORAGE_SNAPSHOTS_COLUMNS,
	)?;
	let cdc_ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_CDC,
		"system::metrics::cdc",
		"cdc",
		NamespaceId::SYSTEM_METRICS,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		cdc_ns,
		"system::metrics::cdc",
		SeriesId::CDC_SNAPSHOTS,
		cdc_snapshot_columns(),
		&ColumnId::CDC_SNAPSHOTS_COLUMNS,
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

fn metric_col(name: &str, ty: ValueType) -> RingBufferColumnToCreate {
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
			metric_col("timestamp", ValueType::DateTime),
			metric_col("operation", ValueType::Utf8),
			metric_col("fingerprint", ValueType::Utf8),
			metric_col("total_duration", ValueType::Duration),
			metric_col("compute_duration", ValueType::Duration),
			metric_col("success", ValueType::Boolean),
			metric_col("statement_count", ValueType::Int8),
			metric_col("normalized_rql", ValueType::Utf8),
		],
		capacity: REQUEST_HISTORY_CAPACITY,
		partition_by: vec![],
		time: TimeSource::Processing,
	}
}

fn statement_stats_schema(namespace: NamespaceId) -> RingBufferToCreate {
	RingBufferToCreate {
		name: Fragment::internal("statement_stats"),
		namespace,
		columns: vec![
			metric_col("snapshot_timestamp", ValueType::DateTime),
			metric_col("fingerprint", ValueType::Utf8),
			metric_col("normalized_rql", ValueType::Utf8),
			metric_col("calls", ValueType::Int8),
			metric_col("total_duration", ValueType::Duration),
			metric_col("mean_duration", ValueType::Duration),
			metric_col("max_duration", ValueType::Duration),
			metric_col("min_duration", ValueType::Duration),
			metric_col("total_rows", ValueType::Int8),
			metric_col("errors", ValueType::Int8),
		],
		capacity: STATEMENT_STATS_CAPACITY,
		partition_by: vec![],
		time: TimeSource::Processing,
	}
}

pub(super) fn ensure_snapshot_series(
	catalog_api: &Catalog,
	admin: &mut AdminTransaction,
	ns: NamespaceId,
	path: &str,
	series_id: SeriesId,
	columns: Vec<SeriesColumnToCreate>,
	column_ids: &[ColumnId],
) -> Result<()> {
	if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut *admin), ns, "snapshots")?.is_none() {
		catalog_api.create_series_with_id(
			&mut *admin,
			series_id,
			SeriesToCreate {
				name: Fragment::internal("snapshots"),
				namespace: ns,
				columns,
				tag: None,
				key: SeriesKey::DateTime {
					column: "ts".to_string(),
					precision: TimestampPrecision::Millisecond,
				},
				partition_by: vec![],
				time: TimeSource::Processing,
			},
			column_ids,
		)?;
		info!("Created {path}::snapshots series");
	}
	Ok(())
}

fn storage_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("object_kind", ValueType::Utf8),
		series_col("id", ValueType::Uint8),
		series_col("namespace_id", ValueType::Uint8),
		series_col("tier", ValueType::Utf8),
		series_col("estimated_live_key_bytes", ValueType::Uint8),
		series_col("estimated_live_value_bytes", ValueType::Uint8),
		series_col("estimated_live_bytes", ValueType::Uint8),
		series_col("estimated_live_count", ValueType::Uint8),
		series_col("estimated_superseded_key_bytes", ValueType::Uint8),
		series_col("estimated_superseded_value_bytes", ValueType::Uint8),
		series_col("estimated_superseded_bytes", ValueType::Uint8),
		series_col("estimated_superseded_count", ValueType::Uint8),
		series_col("estimated_total_bytes", ValueType::Uint8),
	]
}

fn cdc_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("object_kind", ValueType::Utf8),
		series_col("id", ValueType::Uint8),
		series_col("namespace_id", ValueType::Uint8),
		series_col("key_bytes", ValueType::Uint8),
		series_col("value_bytes", ValueType::Uint8),
		series_col("total_bytes", ValueType::Uint8),
		series_col("count", ValueType::Uint8),
	]
}
