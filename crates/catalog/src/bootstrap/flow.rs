// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, SeriesId},
	marker::latency_digest_type,
};
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::value::value_type::ValueType;

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
	runtime: &RuntimeContext,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone());
	let mut admin = AdminTransaction::new(
		multi.begin_command(multi.generate_transaction_id())?,
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		Arc::new(TransactionContext::system(runtime.clone())),
	);

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_FLOW,
		"system::flow",
		"flow",
		NamespaceId::SYSTEM,
	)?;
	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_FLOW_OPERATOR,
		"system::flow::operator",
		"operator",
		NamespaceId::SYSTEM_FLOW,
	)?;
	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_FLOW_OPERATOR_LIBRARY,
		"system::flow::operator::library",
		"library",
		NamespaceId::SYSTEM_FLOW_OPERATOR,
	)?;
	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRIC_FLOW,
		"system::metric::flow",
		"flow",
		NamespaceId::SYSTEM_METRIC,
	)?;
	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRIC_FLOW_STATE,
		"system::metric::flow::state",
		"state",
		NamespaceId::SYSTEM_METRIC_FLOW,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		ns,
		"system::metric::flow::state",
		SeriesId::FLOW_STATE_SNAPSHOTS,
		flow_state_snapshot_columns(),
		&ColumnId::FLOW_STATE_SNAPSHOTS_COLUMNS,
	)?;
	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRIC_FLOW_LATENCY,
		"system::metric::flow::latency",
		"latency",
		NamespaceId::SYSTEM_METRIC_FLOW,
	)?;
	ensure_snapshot_series(
		&catalog_api,
		&mut admin,
		ns,
		"system::metric::flow::latency",
		SeriesId::FLOW_LATENCY_SNAPSHOTS,
		flow_latency_snapshot_columns(),
		&ColumnId::FLOW_LATENCY_SNAPSHOTS_COLUMNS,
	)?;

	admin.commit()?;
	Ok(())
}

fn flow_state_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("operator_id", ValueType::Uint8),
		series_col("name", ValueType::Utf8),
		series_col("unmanaged_because", ValueType::Utf8),
		series_col("keyspace", ValueType::Utf8),
		series_col("phase", ValueType::Utf8),
		series_col("keys", ValueType::Uint8),
		series_col("key_bytes", ValueType::Uint8),
		series_col("value_bytes", ValueType::Uint8),
		series_col("total_bytes", ValueType::Uint8),
	]
}

fn flow_latency_snapshot_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		series_col("ts", ValueType::DateTime),
		series_col("view_id", ValueType::Uint8),
		series_col("name", ValueType::Utf8),
		series_col("samples", ValueType::Uint8),
		series_col("p50", ValueType::Duration),
		series_col("p90", ValueType::Duration),
		series_col("p100", ValueType::Duration),
		series_col("digest", latency_digest_type()),
		series_col("hop_p50", ValueType::Duration),
		series_col("hop_p90", ValueType::Duration),
		series_col("hop_p100", ValueType::Duration),
		series_col("hop_digest", latency_digest_type()),
	]
}
