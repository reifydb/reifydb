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

pub fn bootstrap_runtime(
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
		NamespaceId::SYSTEM_METRICS_RUNTIME,
		"system::metrics::runtime",
		"runtime",
		NamespaceId::SYSTEM_METRICS,
	)?;

	for (ns_id, path, local_name) in RUNTIME_DOMAINS {
		let ns = ensure_namespace(
			&catalog_api,
			&mut admin,
			ns_id,
			path,
			local_name,
			NamespaceId::SYSTEM_METRICS_RUNTIME,
		)?;

		if catalog_api.find_series_by_name(&mut Transaction::Admin(&mut admin), ns, "snapshots")?.is_none() {
			catalog_api.create_series(
				&mut admin,
				SeriesToCreate {
					name: Fragment::internal("snapshots"),
					namespace: ns,
					columns: runtime_columns(),
					tag: None,
					key: SeriesKey::DateTime {
						column: "ts".to_string(),
						precision: TimestampPrecision::Millisecond,
					},
					underlying: false,
				},
			)?;
			info!("Created {path}::snapshots series");
		}
	}

	admin.commit()?;
	Ok(())
}

const RUNTIME_DOMAINS: [(NamespaceId, &str, &str); 2] = [
	(NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY, "system::metrics::runtime::memory", "memory"),
	(NamespaceId::SYSTEM_METRICS_RUNTIME_WATERMARKS, "system::metrics::runtime::watermarks", "watermarks"),
];

fn runtime_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

fn runtime_columns() -> Vec<SeriesColumnToCreate> {
	vec![
		runtime_col("ts", ValueType::DateTime),
		runtime_col("scope", ValueType::Utf8),
		runtime_col("metric", ValueType::Utf8),
		runtime_col("value", ValueType::Float8),
		runtime_col("unit", ValueType::Utf8),
	]
}
