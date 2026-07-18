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

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_READ_BUFFER,
		"system::metrics::read_buffer",
		"read_buffer",
		NamespaceId::SYSTEM_METRICS,
	)?;

	let domains = [
		(
			NamespaceId::SYSTEM_METRICS_READ_BUFFER_SHARDS,
			"system::metrics::read_buffer::shards",
			"shards",
			SeriesId::READ_BUFFER_SHARDS_SNAPSHOTS,
			shards_snapshot_columns(),
			&ColumnId::READ_BUFFER_SHARDS_SNAPSHOTS_COLUMNS[..],
		),
		(
			NamespaceId::SYSTEM_METRICS_READ_BUFFER_WARMS,
			"system::metrics::read_buffer::warms",
			"warms",
			SeriesId::READ_BUFFER_WARMS_SNAPSHOTS,
			warms_snapshot_columns(),
			&ColumnId::READ_BUFFER_WARMS_SNAPSHOTS_COLUMNS[..],
		),
		(
			NamespaceId::SYSTEM_METRICS_READ_BUFFER_READS,
			"system::metrics::read_buffer::reads",
			"reads",
			SeriesId::READ_BUFFER_READS_SNAPSHOTS,
			reads_snapshot_columns(),
			&ColumnId::READ_BUFFER_READS_SNAPSHOTS_COLUMNS[..],
		),
	];

	for (namespace_id, path, local_name, series_id, columns, column_ids) in domains {
		let ns = ensure_namespace(
			&catalog_api,
			&mut admin,
			namespace_id,
			path,
			local_name,
			NamespaceId::SYSTEM_METRICS_READ_BUFFER,
		)?;

		if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut admin), ns, "snapshots")?.is_none() {
			catalog_api.create_series_with_id(
				&mut admin,
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
	}

	admin.commit()?;
	Ok(())
}

fn read_buffer_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn shards_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		read_buffer_col("ts", ValueType::DateTime),
		read_buffer_col("domain", ValueType::Utf8),
		read_buffer_col("shard", ValueType::Uint2),
		read_buffer_col("used", ValueType::Uint8),
		read_buffer_col("limit", ValueType::Uint8),
		read_buffer_col("pages", ValueType::Uint8),
		read_buffer_col("page_cap", ValueType::Uint8),
		read_buffer_col("payload", ValueType::Uint8),
		read_buffer_col("entries", ValueType::Uint8),
		read_buffer_col("hot_pages", ValueType::Uint8),
		read_buffer_col("complete_pages", ValueType::Uint8),
		read_buffer_col("blocked_pages", ValueType::Uint8),
		read_buffer_col("warming", ValueType::Uint8),
	]
}

fn warms_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		read_buffer_col("ts", ValueType::DateTime),
		read_buffer_col("domain", ValueType::Utf8),
		read_buffer_col("shard", ValueType::Uint2),
		read_buffer_col("started", ValueType::Uint8),
		read_buffer_col("completed", ValueType::Uint8),
		read_buffer_col("dirty_aborted", ValueType::Uint8),
		read_buffer_col("aborted", ValueType::Uint8),
		read_buffer_col("blocked_marks", ValueType::Uint8),
		read_buffer_col("evicted_pages", ValueType::Uint8),
		read_buffer_col("invalidated_complete_pages", ValueType::Uint8),
	]
}

fn reads_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		read_buffer_col("ts", ValueType::DateTime),
		read_buffer_col("domain", ValueType::Utf8),
		read_buffer_col("shard", ValueType::Uint2),
		read_buffer_col("point_hits", ValueType::Uint8),
		read_buffer_col("previous_hits", ValueType::Uint8),
		read_buffer_col("point_misses", ValueType::Uint8),
		read_buffer_col("range_served", ValueType::Uint8),
		read_buffer_col("range_gaps", ValueType::Uint8),
	]
}
