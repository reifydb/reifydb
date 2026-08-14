// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, SeriesId},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::value::{identity::IdentityId, value_type::ValueType};

use super::{ensure_namespace, metric::ensure_snapshot_series, series_col};
use crate::{
	Result,
	cache::CatalogCache,
	catalog::{Catalog, series::SeriesColumnToCreate},
};

pub fn bootstrap_flow(
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
		NamespaceId::SYSTEM_METRICS_FLOW,
		"system::metrics::flow",
		"flow",
		NamespaceId::SYSTEM_METRICS,
	)?;
	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_FLOW_STATE,
		"system::metrics::flow::state",
		"state",
		NamespaceId::SYSTEM_METRICS_FLOW,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		ns,
		"system::metrics::flow::state",
		SeriesId::FLOW_STATE_SNAPSHOTS,
		flow_state_snapshot_columns(),
		&ColumnId::FLOW_STATE_SNAPSHOTS_COLUMNS,
	)?;

	admin.commit()?;
	Ok(())
}

fn flow_state_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("operator", ValueType::Uint8),
		series_col("keyspace", ValueType::Utf8),
		series_col("phase", ValueType::Utf8),
		series_col("keys", ValueType::Uint8),
		series_col("key_bytes", ValueType::Uint8),
		series_col("value_bytes", ValueType::Uint8),
		series_col("total_bytes", ValueType::Uint8),
	]
}
