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

pub fn bootstrap_epoch(
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
		NamespaceId::SYSTEM_METRICS_EPOCH,
		"system::metrics::epoch",
		"epoch",
		NamespaceId::SYSTEM_METRICS,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		ns,
		"system::metrics::epoch",
		SeriesId::EPOCH_SNAPSHOTS,
		epoch_snapshot_columns(),
		&ColumnId::EPOCH_SNAPSHOTS_COLUMNS,
	)?;

	admin.commit()?;
	Ok(())
}

fn epoch_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("samples", ValueType::Uint8),
		series_col("durable_samples", ValueType::Uint8),
		series_col("coverage", ValueType::Duration),
		series_col("guaranteed_coverage", ValueType::Duration),
		series_col("pruned", ValueType::Uint8),
		series_col("floor_none_returns", ValueType::Uint8),
	]
}
