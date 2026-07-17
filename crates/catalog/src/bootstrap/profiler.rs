// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		id::{ColumnId, NamespaceId, SeriesId},
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
		series::{SeriesColumnToCreate, SeriesToCreate},
	},
};

const PROFILER_CATEGORIES: [(&str, NamespaceId, SeriesId, &[ColumnId]); 21] = [
	(
		"query",
		NamespaceId::SYSTEM_METRICS_PROFILER_QUERY,
		SeriesId::PROFILER_QUERY_SNAPSHOTS,
		&ColumnId::PROFILER_QUERY_SNAPSHOTS_COLUMNS,
	),
	(
		"txn",
		NamespaceId::SYSTEM_METRICS_PROFILER_TXN,
		SeriesId::PROFILER_TXN_SNAPSHOTS,
		&ColumnId::PROFILER_TXN_SNAPSHOTS_COLUMNS,
	),
	(
		"storage",
		NamespaceId::SYSTEM_METRICS_PROFILER_STORAGE,
		SeriesId::PROFILER_STORAGE_SNAPSHOTS,
		&ColumnId::PROFILER_STORAGE_SNAPSHOTS_COLUMNS,
	),
	(
		"plan",
		NamespaceId::SYSTEM_METRICS_PROFILER_PLAN,
		SeriesId::PROFILER_PLAN_SNAPSHOTS,
		&ColumnId::PROFILER_PLAN_SNAPSHOTS_COLUMNS,
	),
	(
		"cdc",
		NamespaceId::SYSTEM_METRICS_PROFILER_CDC,
		SeriesId::PROFILER_CDC_SNAPSHOTS,
		&ColumnId::PROFILER_CDC_SNAPSHOTS_COLUMNS,
	),
	(
		"flow",
		NamespaceId::SYSTEM_METRICS_PROFILER_FLOW,
		SeriesId::PROFILER_FLOW_SNAPSHOTS,
		&ColumnId::PROFILER_FLOW_SNAPSHOTS_COLUMNS,
	),
	(
		"subscription",
		NamespaceId::SYSTEM_METRICS_PROFILER_SUBSCRIPTION,
		SeriesId::PROFILER_SUBSCRIPTION_SNAPSHOTS,
		&ColumnId::PROFILER_SUBSCRIPTION_SNAPSHOTS_COLUMNS,
	),
	(
		"server",
		NamespaceId::SYSTEM_METRICS_PROFILER_SERVER,
		SeriesId::PROFILER_SERVER_SNAPSHOTS,
		&ColumnId::PROFILER_SERVER_SNAPSHOTS_COLUMNS,
	),
	(
		"wire",
		NamespaceId::SYSTEM_METRICS_PROFILER_WIRE,
		SeriesId::PROFILER_WIRE_SNAPSHOTS,
		&ColumnId::PROFILER_WIRE_SNAPSHOTS_COLUMNS,
	),
	(
		"auth",
		NamespaceId::SYSTEM_METRICS_PROFILER_AUTH,
		SeriesId::PROFILER_AUTH_SNAPSHOTS,
		&ColumnId::PROFILER_AUTH_SNAPSHOTS_COLUMNS,
	),
	(
		"catalog",
		NamespaceId::SYSTEM_METRICS_PROFILER_CATALOG,
		SeriesId::PROFILER_CATALOG_SNAPSHOTS,
		&ColumnId::PROFILER_CATALOG_SNAPSHOTS_COLUMNS,
	),
	(
		"engine",
		NamespaceId::SYSTEM_METRICS_PROFILER_ENGINE,
		SeriesId::PROFILER_ENGINE_SNAPSHOTS,
		&ColumnId::PROFILER_ENGINE_SNAPSHOTS_COLUMNS,
	),
	(
		"mutate",
		NamespaceId::SYSTEM_METRICS_PROFILER_MUTATE,
		SeriesId::PROFILER_MUTATE_SNAPSHOTS,
		&ColumnId::PROFILER_MUTATE_SNAPSHOTS_COLUMNS,
	),
	(
		"transport",
		NamespaceId::SYSTEM_METRICS_PROFILER_TRANSPORT,
		SeriesId::PROFILER_TRANSPORT_SNAPSHOTS,
		&ColumnId::PROFILER_TRANSPORT_SNAPSHOTS_COLUMNS,
	),
	(
		"task",
		NamespaceId::SYSTEM_METRICS_PROFILER_TASK,
		SeriesId::PROFILER_TASK_SNAPSHOTS,
		&ColumnId::PROFILER_TASK_SNAPSHOTS_COLUMNS,
	),
	(
		"policy",
		NamespaceId::SYSTEM_METRICS_PROFILER_POLICY,
		SeriesId::PROFILER_POLICY_SNAPSHOTS,
		&ColumnId::PROFILER_POLICY_SNAPSHOTS_COLUMNS,
	),
	(
		"ffi",
		NamespaceId::SYSTEM_METRICS_PROFILER_FFI,
		SeriesId::PROFILER_FFI_SNAPSHOTS,
		&ColumnId::PROFILER_FFI_SNAPSHOTS_COLUMNS,
	),
	(
		"cache",
		NamespaceId::SYSTEM_METRICS_PROFILER_CACHE,
		SeriesId::PROFILER_CACHE_SNAPSHOTS,
		&ColumnId::PROFILER_CACHE_SNAPSHOTS_COLUMNS,
	),
	(
		"shape",
		NamespaceId::SYSTEM_METRICS_PROFILER_SHAPE,
		SeriesId::PROFILER_SHAPE_SNAPSHOTS,
		&ColumnId::PROFILER_SHAPE_SNAPSHOTS_COLUMNS,
	),
	(
		"api",
		NamespaceId::SYSTEM_METRICS_PROFILER_API,
		SeriesId::PROFILER_API_SNAPSHOTS,
		&ColumnId::PROFILER_API_SNAPSHOTS_COLUMNS,
	),
	(
		"actor",
		NamespaceId::SYSTEM_METRICS_PROFILER_ACTOR,
		SeriesId::PROFILER_ACTOR_SNAPSHOTS,
		&ColumnId::PROFILER_ACTOR_SNAPSHOTS_COLUMNS,
	),
];

pub fn bootstrap_profiler(
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
		NamespaceId::SYSTEM_METRICS_PROFILER,
		"system::metrics::profiler",
		"profiler",
		NamespaceId::SYSTEM_METRICS,
	)?;

	for (category_name, category_namespace, series_id, column_ids) in PROFILER_CATEGORIES {
		let ns_path = format!("system::metrics::profiler::{category_name}");
		let ns_id = ensure_namespace(
			&catalog_api,
			&mut admin,
			category_namespace,
			&ns_path,
			category_name,
			NamespaceId::SYSTEM_METRICS_PROFILER,
		)?;

		if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut admin), ns_id, "snapshots")?.is_none() {
			catalog_api.create_series_with_id(
				&mut admin,
				series_id,
				SeriesToCreate {
					name: Fragment::internal("snapshots"),
					namespace: ns_id,
					columns: profiler_snapshot_columns(),
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
			info!("Created {ns_path}::snapshots series");
		}
	}

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_PROFILER_CATEGORIES,
		"system::metrics::profiler::categories",
		"categories",
		NamespaceId::SYSTEM_METRICS_PROFILER,
	)?;

	admin.commit()?;
	Ok(())
}

fn profiler_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn profiler_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		profiler_col("ts", ValueType::DateTime),
		profiler_col("span_name", ValueType::Utf8),
		profiler_col("dim_1", ValueType::Utf8),
		profiler_col("dim_2", ValueType::Utf8),
		profiler_col("calls", ValueType::Uint8),
		profiler_col("total", ValueType::Duration),
		profiler_col("min", ValueType::Duration),
		profiler_col("max", ValueType::Duration),
		profiler_col("p50", ValueType::Duration),
		profiler_col("p60", ValueType::Duration),
		profiler_col("p70", ValueType::Duration),
		profiler_col("p75", ValueType::Duration),
		profiler_col("p80", ValueType::Duration),
		profiler_col("p85", ValueType::Duration),
		profiler_col("p90", ValueType::Duration),
		profiler_col("p95", ValueType::Duration),
		profiler_col("p98", ValueType::Duration),
		profiler_col("p99", ValueType::Duration),
		profiler_col("extra_0", ValueType::Uint8),
		profiler_col("extra_1", ValueType::Uint8),
		profiler_col("extra_2", ValueType::Uint8),
		profiler_col("extra_3", ValueType::Uint8),
	]
}
