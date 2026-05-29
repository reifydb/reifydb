// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		id::NamespaceId,
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

const PROFILER_CATEGORIES: [(&str, NamespaceId); 6] = [
	("query", NamespaceId::SYSTEM_METRICS_PROFILER_QUERY),
	("txn", NamespaceId::SYSTEM_METRICS_PROFILER_TXN),
	("storage", NamespaceId::SYSTEM_METRICS_PROFILER_STORAGE),
	("plan", NamespaceId::SYSTEM_METRICS_PROFILER_PLAN),
	("cdc", NamespaceId::SYSTEM_METRICS_PROFILER_CDC),
	("flow", NamespaceId::SYSTEM_METRICS_PROFILER_FLOW),
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

	for (category_name, category_namespace) in PROFILER_CATEGORIES {
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
			catalog_api.create_series(
				&mut admin,
				SeriesToCreate {
					name: Fragment::internal("snapshots"),
					namespace: ns_id,
					columns: profiler_snapshot_columns(),
					tag: None,
					key: SeriesKey::DateTime {
						column: "ts".to_string(),
						precision: TimestampPrecision::Millisecond,
					},
					underlying: false,
				},
			)?;
			info!("Created {ns_path}::snapshots series");
		}
	}

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
