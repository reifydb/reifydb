// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{LazyLock, Once};

use reifydb_metric::{counter::Counter, gauge::Gauge, histogram::Histogram, registry::STATIC_REGISTRY};
use reifydb_profiler::category::ProfilerCategory;

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

pub static PROFILER_QUERY_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.query.duration_us", "Profiler category Query duration (us)", QUERY_BOUNDS)
});

pub static PROFILER_TXN_HIST: LazyLock<Histogram> =
	LazyLock::new(|| Histogram::new("profiler.txn.duration_us", "Profiler category Txn duration (us)", TXN_BOUNDS));

pub static PROFILER_STORAGE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.storage.duration_us", "Profiler category Storage duration (us)", STORAGE_BOUNDS)
});

pub static PROFILER_PLAN_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.plan.duration_us", "Profiler category Plan duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_CDC_HIST: LazyLock<Histogram> =
	LazyLock::new(|| Histogram::new("profiler.cdc.duration_us", "Profiler category Cdc duration (us)", CDC_BOUNDS));

pub static PROFILER_FLOW_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.flow.duration_us", "Profiler category Flow duration (us)", FLOW_BOUNDS)
});

pub static PROFILER_SUBSCRIPTION_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.subscription.duration_us", "Profiler category Subscription duration (us)", FLOW_BOUNDS)
});

pub static PROFILER_SERVER_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.server.duration_us", "Profiler category Server duration (us)", FLOW_BOUNDS)
});

pub static PROFILER_WIRE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.wire.duration_us", "Profiler category Wire duration (us)", STORAGE_BOUNDS)
});

pub static PROFILER_AUTH_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.auth.duration_us", "Profiler category Auth duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_CATALOG_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.catalog.duration_us", "Profiler category Catalog duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_ENGINE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.engine.duration_us", "Profiler category Engine duration (us)", QUERY_BOUNDS)
});

pub static PROFILER_MUTATE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.mutate.duration_us", "Profiler category Mutate duration (us)", TXN_BOUNDS)
});

pub static PROFILER_TRANSPORT_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.transport.duration_us", "Profiler category Transport duration (us)", QUERY_BOUNDS)
});

pub static PROFILER_TASK_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.task.duration_us", "Profiler category Task duration (us)", FLOW_BOUNDS)
});

pub static PROFILER_POLICY_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.policy.duration_us", "Profiler category Policy duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_FFI_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.ffi.duration_us", "Profiler category Ffi duration (us)", STORAGE_BOUNDS)
});

pub static PROFILER_CACHE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.cache.duration_us", "Profiler category Cache duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_SHAPE_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.shape.duration_us", "Profiler category Shape duration (us)", PLAN_BOUNDS)
});

pub static PROFILER_API_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.api.duration_us", "Profiler category Api duration (us)", QUERY_BOUNDS)
});

pub static PROFILER_ACTOR_HIST: LazyLock<Histogram> = LazyLock::new(|| {
	Histogram::new("profiler.actor.duration_us", "Profiler category Actor duration (us)", FLOW_BOUNDS)
});

pub fn histogram_for(category: ProfilerCategory) -> &'static Histogram {
	match category {
		ProfilerCategory::Query => &PROFILER_QUERY_HIST,
		ProfilerCategory::Txn => &PROFILER_TXN_HIST,
		ProfilerCategory::Storage => &PROFILER_STORAGE_HIST,
		ProfilerCategory::Plan => &PROFILER_PLAN_HIST,
		ProfilerCategory::Cdc => &PROFILER_CDC_HIST,
		ProfilerCategory::Flow => &PROFILER_FLOW_HIST,
		ProfilerCategory::Subscription => &PROFILER_SUBSCRIPTION_HIST,
		ProfilerCategory::Server => &PROFILER_SERVER_HIST,
		ProfilerCategory::Wire => &PROFILER_WIRE_HIST,
		ProfilerCategory::Auth => &PROFILER_AUTH_HIST,
		ProfilerCategory::Catalog => &PROFILER_CATALOG_HIST,
		ProfilerCategory::Engine => &PROFILER_ENGINE_HIST,
		ProfilerCategory::Mutate => &PROFILER_MUTATE_HIST,
		ProfilerCategory::Transport => &PROFILER_TRANSPORT_HIST,
		ProfilerCategory::Task => &PROFILER_TASK_HIST,
		ProfilerCategory::Policy => &PROFILER_POLICY_HIST,
		ProfilerCategory::Ffi => &PROFILER_FFI_HIST,
		ProfilerCategory::Cache => &PROFILER_CACHE_HIST,
		ProfilerCategory::Shape => &PROFILER_SHAPE_HIST,
		ProfilerCategory::Api => &PROFILER_API_HIST,
		ProfilerCategory::Actor => &PROFILER_ACTOR_HIST,
	}
}

pub static PROFILER_ACCUMULATOR_SIZE: Gauge = Gauge::new(
	"profiler.accumulator.size",
	"Current number of distinct (category, callsite, dimensions) records held by the profile accumulator",
);

pub static PROFILER_ACCUMULATOR_CAPACITY: Gauge = Gauge::new(
	"profiler.accumulator.capacity",
	"Configured maximum number of records the profile accumulator will hold",
);

pub static PROFILER_ACCUMULATOR_EVICTIONS: Counter = Counter::new(
	"profiler.accumulator.evictions_total",
	"Number of records evicted by the LFU policy because the accumulator capacity was reached",
);

pub static PROFILER_SNAPSHOT_LAST_FLUSH_RECORDS: Gauge = Gauge::new(
	"profiler.snapshot.last_flush_records",
	"Number of aggregate records persisted in the most recent successful snapshot flush",
);

pub static PROFILER_SNAPSHOT_LAST_FLUSH_TS_MS: Gauge = Gauge::new(
	"profiler.snapshot.last_flush_ts_ms",
	"Wall-clock timestamp (ms since epoch) of the most recent successful snapshot flush",
);

pub static PROFILER_SNAPSHOT_FLUSH_ERRORS: Counter = Counter::new(
	"profiler.snapshot.flush_errors_total",
	"Number of snapshot flush attempts that failed during bulk insert into the history Series",
);

static REGISTERED: Once = Once::new();

pub fn register_all() {
	REGISTERED.call_once(|| {
		STATIC_REGISTRY.register_histogram(&PROFILER_QUERY_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_TXN_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_STORAGE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_PLAN_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_CDC_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_FLOW_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_SUBSCRIPTION_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_SERVER_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_WIRE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_AUTH_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_CATALOG_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_ENGINE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_MUTATE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_TRANSPORT_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_TASK_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_POLICY_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_FFI_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_CACHE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_SHAPE_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_API_HIST);
		STATIC_REGISTRY.register_histogram(&PROFILER_ACTOR_HIST);

		STATIC_REGISTRY.register_gauge(&PROFILER_ACCUMULATOR_SIZE);
		STATIC_REGISTRY.register_gauge(&PROFILER_ACCUMULATOR_CAPACITY);
		STATIC_REGISTRY.register_counter(&PROFILER_ACCUMULATOR_EVICTIONS);

		STATIC_REGISTRY.register_gauge(&PROFILER_SNAPSHOT_LAST_FLUSH_RECORDS);
		STATIC_REGISTRY.register_gauge(&PROFILER_SNAPSHOT_LAST_FLUSH_TS_MS);
		STATIC_REGISTRY.register_counter(&PROFILER_SNAPSHOT_FLUSH_ERRORS);
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
