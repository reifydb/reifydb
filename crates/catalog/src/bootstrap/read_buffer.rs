// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
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
	value::{identity::IdentityId, value_type::ValueType},
};
use tracing::info;

use super::{ensure_namespace, series_col};
use crate::{
	Result,
	cache::CatalogCache,
	catalog::{
		Catalog,
		series::{SeriesColumnToCreate, SeriesToCreate},
	},
};

pub fn bootstrap_read_buffer(
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

	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_READ_BUFFER,
		"system::metrics::read_buffer",
		"read_buffer",
		NamespaceId::SYSTEM_METRICS,
	)?;

	if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut admin), ns, "snapshots")?.is_none() {
		catalog_api.create_series_with_id(
			&mut admin,
			SeriesId::READ_BUFFER_SNAPSHOTS,
			SeriesToCreate {
				name: Fragment::internal("snapshots"),
				namespace: ns,
				columns: read_buffer_snapshot_columns(),
				tag: None,
				key: SeriesKey::DateTime {
					column: "ts".to_string(),
					precision: TimestampPrecision::Millisecond,
				},
				partition_by: vec![],
				time: TimeSource::Processing,
			},
			&ColumnId::READ_BUFFER_SNAPSHOTS_COLUMNS,
		)?;
		info!("Created system::metrics::read_buffer::snapshots series");
	}

	admin.commit()?;
	Ok(())
}

fn read_buffer_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("shard", ValueType::Uint2),
		series_col("used", ValueType::Uint8),
		series_col("limit", ValueType::Uint8),
		series_col("pages", ValueType::Uint8),
		series_col("page_cap", ValueType::Uint8),
		series_col("payload", ValueType::Uint8),
		series_col("entries", ValueType::Uint8),
		series_col("hot_pages", ValueType::Uint8),
		series_col("complete_pages", ValueType::Uint8),
		series_col("blocked_pages", ValueType::Uint8),
		series_col("warming", ValueType::Uint8),
		series_col("warms_started", ValueType::Uint8),
		series_col("warms_completed", ValueType::Uint8),
		series_col("warms_dirty_aborted", ValueType::Uint8),
		series_col("warms_aborted", ValueType::Uint8),
		series_col("pages_warm_blocked", ValueType::Uint8),
		series_col("pages_evicted", ValueType::Uint8),
		series_col("complete_pages_invalidated", ValueType::Uint8),
		series_col("point_hits", ValueType::Uint8),
		series_col("previous_hits", ValueType::Uint8),
		series_col("point_misses", ValueType::Uint8),
		series_col("range_served", ValueType::Uint8),
		series_col("range_gaps", ValueType::Uint8),
	]
}
