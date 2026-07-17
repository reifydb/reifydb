// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
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

use super::ensure_namespace;
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

const STORAGE_PRIMITIVE_NAMESPACES: [(NamespaceId, &str, &str, SeriesId, &[ColumnId]); 9] = [
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_TABLE,
		"system::metrics::storage::table",
		"table",
		SeriesId::STORAGE_TABLE_SNAPSHOTS,
		&ColumnId::STORAGE_TABLE_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_VIEW,
		"system::metrics::storage::view",
		"view",
		SeriesId::STORAGE_VIEW_SNAPSHOTS,
		&ColumnId::STORAGE_VIEW_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_TABLE_VIRTUAL,
		"system::metrics::storage::table_virtual",
		"table_virtual",
		SeriesId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS,
		&ColumnId::STORAGE_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_RINGBUFFER,
		"system::metrics::storage::ringbuffer",
		"ringbuffer",
		SeriesId::STORAGE_RINGBUFFER_SNAPSHOTS,
		&ColumnId::STORAGE_RINGBUFFER_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_DICTIONARY,
		"system::metrics::storage::dictionary",
		"dictionary",
		SeriesId::STORAGE_DICTIONARY_SNAPSHOTS,
		&ColumnId::STORAGE_DICTIONARY_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_SERIES,
		"system::metrics::storage::series",
		"series",
		SeriesId::STORAGE_SERIES_SNAPSHOTS,
		&ColumnId::STORAGE_SERIES_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_FLOW,
		"system::metrics::storage::flow",
		"flow",
		SeriesId::STORAGE_FLOW_SNAPSHOTS,
		&ColumnId::STORAGE_FLOW_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_FLOW_NODE,
		"system::metrics::storage::flow_node",
		"flow_node",
		SeriesId::STORAGE_FLOW_NODE_SNAPSHOTS,
		&ColumnId::STORAGE_FLOW_NODE_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORAGE_SYSTEM,
		"system::metrics::storage::system",
		"system",
		SeriesId::STORAGE_SYSTEM_SNAPSHOTS,
		&ColumnId::STORAGE_SYSTEM_SNAPSHOTS_COLUMNS,
	),
];

const CDC_PRIMITIVE_NAMESPACES: [(NamespaceId, &str, &str, SeriesId, &[ColumnId]); 9] = [
	(
		NamespaceId::SYSTEM_METRICS_CDC_TABLE,
		"system::metrics::cdc::table",
		"table",
		SeriesId::CDC_TABLE_SNAPSHOTS,
		&ColumnId::CDC_TABLE_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_VIEW,
		"system::metrics::cdc::view",
		"view",
		SeriesId::CDC_VIEW_SNAPSHOTS,
		&ColumnId::CDC_VIEW_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_TABLE_VIRTUAL,
		"system::metrics::cdc::table_virtual",
		"table_virtual",
		SeriesId::CDC_TABLE_VIRTUAL_SNAPSHOTS,
		&ColumnId::CDC_TABLE_VIRTUAL_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_RINGBUFFER,
		"system::metrics::cdc::ringbuffer",
		"ringbuffer",
		SeriesId::CDC_RINGBUFFER_SNAPSHOTS,
		&ColumnId::CDC_RINGBUFFER_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_DICTIONARY,
		"system::metrics::cdc::dictionary",
		"dictionary",
		SeriesId::CDC_DICTIONARY_SNAPSHOTS,
		&ColumnId::CDC_DICTIONARY_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_SERIES,
		"system::metrics::cdc::series",
		"series",
		SeriesId::CDC_SERIES_SNAPSHOTS,
		&ColumnId::CDC_SERIES_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_FLOW,
		"system::metrics::cdc::flow",
		"flow",
		SeriesId::CDC_FLOW_SNAPSHOTS,
		&ColumnId::CDC_FLOW_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_FLOW_NODE,
		"system::metrics::cdc::flow_node",
		"flow_node",
		SeriesId::CDC_FLOW_NODE_SNAPSHOTS,
		&ColumnId::CDC_FLOW_NODE_SNAPSHOTS_COLUMNS,
	),
	(
		NamespaceId::SYSTEM_METRICS_CDC_SYSTEM,
		"system::metrics::cdc::system",
		"system",
		SeriesId::CDC_SYSTEM_SNAPSHOTS,
		&ColumnId::CDC_SYSTEM_SNAPSHOTS_COLUMNS,
	),
];

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

	for (id, path, local_name, series_id, column_ids) in STORAGE_PRIMITIVE_NAMESPACES {
		let ns = ensure_namespace(
			&catalog_api,
			&mut admin,
			id,
			path,
			local_name,
			NamespaceId::SYSTEM_METRICS_STORAGE,
		)?;
		ensure_snapshot_series(
			&catalog_api,
			&mut admin,
			ns,
			path,
			series_id,
			storage_snapshot_columns(),
			column_ids,
		)?;
	}
	for (id, path, local_name, series_id, column_ids) in CDC_PRIMITIVE_NAMESPACES {
		let ns = ensure_namespace(
			&catalog_api,
			&mut admin,
			id,
			path,
			local_name,
			NamespaceId::SYSTEM_METRICS_CDC,
		)?;
		ensure_snapshot_series(
			&catalog_api,
			&mut admin,
			ns,
			path,
			series_id,
			cdc_snapshot_columns(),
			column_ids,
		)?;
	}

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
		underlying: false,
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
		underlying: false,
	}
}

fn ensure_snapshot_series(
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
				underlying: false,
			},
			column_ids,
		)?;
		info!("Created {path}::snapshots series");
	}
	Ok(())
}

fn series_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn storage_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("id", ValueType::Uint8),
		series_col("namespace_id", ValueType::Uint8),
		series_col("tier", ValueType::Utf8),
		series_col("current_key_bytes", ValueType::Uint8),
		series_col("current_value_bytes", ValueType::Uint8),
		series_col("current_total_bytes", ValueType::Uint8),
		series_col("current_count", ValueType::Uint8),
		series_col("historical_key_bytes", ValueType::Uint8),
		series_col("historical_value_bytes", ValueType::Uint8),
		series_col("historical_total_bytes", ValueType::Uint8),
		series_col("historical_count", ValueType::Uint8),
		series_col("total_bytes", ValueType::Uint8),
	]
}

fn cdc_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("id", ValueType::Uint8),
		series_col("namespace_id", ValueType::Uint8),
		series_col("key_bytes", ValueType::Uint8),
		series_col("value_bytes", ValueType::Uint8),
		series_col("total_bytes", ValueType::Uint8),
		series_col("count", ValueType::Uint8),
	]
}
