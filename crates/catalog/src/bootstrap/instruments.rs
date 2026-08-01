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

pub fn bootstrap_instruments(
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
		NamespaceId::SYSTEM_METRICS_INSTRUMENTS,
		"system::metrics::instruments",
		"instruments",
		NamespaceId::SYSTEM_METRICS,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		ns,
		"system::metrics::instruments",
		SeriesId::INSTRUMENTS_SNAPSHOTS,
		instruments_snapshot_columns(),
		&ColumnId::INSTRUMENTS_SNAPSHOTS_COLUMNS,
	)?;

	admin.commit()?;
	Ok(())
}

fn instruments_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("scope", ValueType::Utf8),
		series_col("metric", ValueType::Utf8),
		series_col("value", ValueType::Float8),
		series_col("unit", ValueType::Utf8),
		series_col("kind", ValueType::Utf8),
	]
}
