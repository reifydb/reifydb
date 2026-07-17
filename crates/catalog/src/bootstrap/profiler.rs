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

	let spans_ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_PROFILER_SPANS,
		"system::metrics::profiler::spans",
		"spans",
		NamespaceId::SYSTEM_METRICS_PROFILER,
	)?;

	if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut admin), spans_ns, "snapshots")?.is_none() {
		catalog_api.create_series_with_id(
			&mut admin,
			SeriesId::PROFILER_SPANS_SNAPSHOTS,
			SeriesToCreate {
				name: Fragment::internal("snapshots"),
				namespace: spans_ns,
				columns: spans_snapshot_columns(),
				tag: None,
				key: SeriesKey::DateTime {
					column: "ts".to_string(),
					precision: TimestampPrecision::Millisecond,
				},
				partition_by: vec![],
				underlying: false,
			},
			&ColumnId::PROFILER_SPANS_SNAPSHOTS_COLUMNS,
		)?;
		info!("Created system::metrics::profiler::spans::snapshots series");
	}

	admin.commit()?;
	Ok(())
}

fn spans_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn spans_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		spans_col("ts", ValueType::DateTime),
		spans_col("category", ValueType::Utf8),
		spans_col("span_name", ValueType::Utf8),
		spans_col("dim_1", ValueType::Utf8),
		spans_col("dim_2", ValueType::Utf8),
		spans_col("calls", ValueType::Uint8),
		spans_col("total", ValueType::Duration),
		spans_col("min", ValueType::Duration),
		spans_col("max", ValueType::Duration),
		spans_col("p50", ValueType::Duration),
		spans_col("p60", ValueType::Duration),
		spans_col("p70", ValueType::Duration),
		spans_col("p75", ValueType::Duration),
		spans_col("p80", ValueType::Duration),
		spans_col("p85", ValueType::Duration),
		spans_col("p90", ValueType::Duration),
		spans_col("p95", ValueType::Duration),
		spans_col("p98", ValueType::Duration),
		spans_col("p99", ValueType::Duration),
		spans_col("extra_0", ValueType::Uint8),
		spans_col("extra_1", ValueType::Uint8),
		spans_col("extra_2", ValueType::Uint8),
		spans_col("extra_3", ValueType::Uint8),
	]
}
