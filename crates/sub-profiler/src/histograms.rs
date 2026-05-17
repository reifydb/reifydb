// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Static per-category latency histograms. One `LazyLock<Histogram>` per `ProfileCategory`; all six are registered
//! with `STATIC_REGISTRY` exactly once at subsystem start so they show up in the existing metric snapshot/OTLP
//! pipeline without further wiring. Boundaries are curated per category and tunable via the configurator.

use std::sync::{LazyLock, Once};

use reifydb_metric::{histogram::Histogram, registry::STATIC_REGISTRY};
use reifydb_profiler::category::ProfileCategory;

static QUERY_BOUNDS: &[f64] = &[
	100.0,
	250.0,
	500.0,
	1_000.0,
	2_500.0,
	5_000.0,
	10_000.0,
	25_000.0,
	50_000.0,
	100_000.0,
	250_000.0,
	500_000.0,
	1_000_000.0,
	2_500_000.0,
	5_000_000.0,
	10_000_000.0,
];

static TXN_BOUNDS: &[f64] = &[
	10.0,
	25.0,
	50.0,
	100.0,
	250.0,
	500.0,
	1_000.0,
	2_500.0,
	5_000.0,
	10_000.0,
	25_000.0,
	50_000.0,
	100_000.0,
	250_000.0,
	500_000.0,
	1_000_000.0,
];

static STORAGE_BOUNDS: &[f64] = &[
	1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0,
	100_000.0,
];

static PLAN_BOUNDS: &[f64] =
	&[10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0];

static CDC_BOUNDS: &[f64] =
	&[10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0];

static FLOW_BOUNDS: &[f64] = &[
	10.0,
	25.0,
	50.0,
	100.0,
	250.0,
	500.0,
	1_000.0,
	2_500.0,
	5_000.0,
	10_000.0,
	25_000.0,
	50_000.0,
	100_000.0,
	250_000.0,
	500_000.0,
	1_000_000.0,
];

pub static PROFILE_QUERY_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profile.query.duration_us", "Profile category Query duration (us)", QUERY_BOUNDS)
});

pub static PROFILE_TXN_HIST: LazyLock<Histogram> =
	LazyLock::new(|| Histogram::new("profile.txn.duration_us", "Profile category Txn duration (us)", TXN_BOUNDS));

pub static PROFILE_STORAGE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profile.storage.duration_us", "Profile category Storage duration (us)", STORAGE_BOUNDS)
});

pub static PROFILE_PLAN_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profile.plan.duration_us", "Profile category Plan duration (us)", PLAN_BOUNDS)
});

pub static PROFILE_CDC_HIST: LazyLock<Histogram> =
	LazyLock::new(|| Histogram::new("profile.cdc.duration_us", "Profile category Cdc duration (us)", CDC_BOUNDS));

pub static PROFILE_FLOW_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profile.flow.duration_us", "Profile category Flow duration (us)", FLOW_BOUNDS)
});

pub fn histogram_for(category: ProfileCategory) -> &'static Histogram {
	match category {
		ProfileCategory::Query => &PROFILE_QUERY_HIST,
		ProfileCategory::Txn => &PROFILE_TXN_HIST,
		ProfileCategory::Storage => &PROFILE_STORAGE_HIST,
		ProfileCategory::Plan => &PROFILE_PLAN_HIST,
		ProfileCategory::Cdc => &PROFILE_CDC_HIST,
		ProfileCategory::Flow => &PROFILE_FLOW_HIST,
	}
}

static REGISTERED: Once = Once::new();

pub fn register_all() {
	REGISTERED.call_once(|| {
		STATIC_REGISTRY.register_histogram(&PROFILE_QUERY_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILE_TXN_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILE_STORAGE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILE_PLAN_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILE_CDC_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILE_FLOW_HIST);
	});
}

#[cfg(test)]
mod tests {
	use reifydb_profiler::category::ALL_CATEGORIES;

	use super::*;

	#[test]
	fn each_category_has_distinct_histogram() {
		let names: Vec<&str> = ALL_CATEGORIES.iter().map(|c| histogram_for(*c).name).collect();
		let mut unique = names.clone();
		unique.sort();
		unique.dedup();
		assert_eq!(unique.len(), names.len());
	}

	#[test]
	fn register_all_is_idempotent() {
		register_all();
		register_all();
	}
}
