// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::metrics::{
	instruments::{counter::Counter, gauge::Gauge, histogram::Histogram},
	registry::MetricsRegistry,
	report::MetricsReporter,
	sample::ReadingKind,
};
use reifydb_profiler::category::{ALL_CATEGORIES, ProfilerCategory};

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

pub struct ProfilerInstruments {
	query: Arc<Histogram>,
	txn: Arc<Histogram>,
	storage: Arc<Histogram>,
	plan: Arc<Histogram>,
	cdc: Arc<Histogram>,
	flow: Arc<Histogram>,
	subscription: Arc<Histogram>,
	server: Arc<Histogram>,
	wire: Arc<Histogram>,
	auth: Arc<Histogram>,
	catalog: Arc<Histogram>,
	engine: Arc<Histogram>,
	mutate: Arc<Histogram>,
	transport: Arc<Histogram>,
	task: Arc<Histogram>,
	policy: Arc<Histogram>,
	ffi: Arc<Histogram>,
	cache: Arc<Histogram>,
	shape: Arc<Histogram>,
	api: Arc<Histogram>,
	actor: Arc<Histogram>,
	pub accumulator_size: Arc<Gauge>,
	pub accumulator_capacity: Arc<Gauge>,
	pub accumulator_evictions: Arc<Counter>,
}

fn duration_histogram(name: &'static str, help: &'static str, boundaries: &'static [f64]) -> Arc<Histogram> {
	Arc::new(Histogram::new(name, help, ReadingKind::Duration, boundaries))
}

impl ProfilerInstruments {
	pub fn new() -> Self {
		Self {
			query: duration_histogram(
				"profiler.query.duration_us",
				"Profiler category Query duration (us)",
				QUERY_BOUNDS,
			),
			txn: duration_histogram(
				"profiler.txn.duration_us",
				"Profiler category Txn duration (us)",
				TXN_BOUNDS,
			),
			storage: duration_histogram(
				"profiler.storage.duration_us",
				"Profiler category Storage duration (us)",
				STORAGE_BOUNDS,
			),
			plan: duration_histogram(
				"profiler.plan.duration_us",
				"Profiler category Plan duration (us)",
				PLAN_BOUNDS,
			),
			cdc: duration_histogram(
				"profiler.cdc.duration_us",
				"Profiler category Cdc duration (us)",
				CDC_BOUNDS,
			),
			flow: duration_histogram(
				"profiler.flow.duration_us",
				"Profiler category Flow duration (us)",
				FLOW_BOUNDS,
			),
			subscription: duration_histogram(
				"profiler.subscription.duration_us",
				"Profiler category Subscription duration (us)",
				FLOW_BOUNDS,
			),
			server: duration_histogram(
				"profiler.server.duration_us",
				"Profiler category Server duration (us)",
				FLOW_BOUNDS,
			),
			wire: duration_histogram(
				"profiler.wire.duration_us",
				"Profiler category Wire duration (us)",
				STORAGE_BOUNDS,
			),
			auth: duration_histogram(
				"profiler.auth.duration_us",
				"Profiler category Auth duration (us)",
				PLAN_BOUNDS,
			),
			catalog: duration_histogram(
				"profiler.catalog.duration_us",
				"Profiler category Catalog duration (us)",
				PLAN_BOUNDS,
			),
			engine: duration_histogram(
				"profiler.engine.duration_us",
				"Profiler category Engine duration (us)",
				QUERY_BOUNDS,
			),
			mutate: duration_histogram(
				"profiler.mutate.duration_us",
				"Profiler category Mutate duration (us)",
				TXN_BOUNDS,
			),
			transport: duration_histogram(
				"profiler.transport.duration_us",
				"Profiler category Transport duration (us)",
				QUERY_BOUNDS,
			),
			task: duration_histogram(
				"profiler.task.duration_us",
				"Profiler category Task duration (us)",
				FLOW_BOUNDS,
			),
			policy: duration_histogram(
				"profiler.policy.duration_us",
				"Profiler category Policy duration (us)",
				PLAN_BOUNDS,
			),
			ffi: duration_histogram(
				"profiler.ffi.duration_us",
				"Profiler category Ffi duration (us)",
				STORAGE_BOUNDS,
			),
			cache: duration_histogram(
				"profiler.cache.duration_us",
				"Profiler category Cache duration (us)",
				PLAN_BOUNDS,
			),
			shape: duration_histogram(
				"profiler.shape.duration_us",
				"Profiler category Shape duration (us)",
				PLAN_BOUNDS,
			),
			api: duration_histogram(
				"profiler.api.duration_us",
				"Profiler category Api duration (us)",
				QUERY_BOUNDS,
			),
			actor: duration_histogram(
				"profiler.actor.duration_us",
				"Profiler category Actor duration (us)",
				FLOW_BOUNDS,
			),
			accumulator_size: Arc::new(Gauge::new(
				"profiler.accumulator.size",
				"Current number of distinct (category, callsite, dimensions) records held by the profile accumulator",
				ReadingKind::Count,
			)),
			accumulator_capacity: Arc::new(Gauge::new(
				"profiler.accumulator.capacity",
				"Configured maximum number of records the profile accumulator will hold",
				ReadingKind::Count,
			)),
			accumulator_evictions: Arc::new(Counter::new(
				"profiler.accumulator.evictions_total",
				"Number of records evicted by the LFU policy because the accumulator capacity was reached",
			)),
		}
	}

	pub fn histogram_for(&self, category: ProfilerCategory) -> &Arc<Histogram> {
		match category {
			ProfilerCategory::Query => &self.query,
			ProfilerCategory::Txn => &self.txn,
			ProfilerCategory::Storage => &self.storage,
			ProfilerCategory::Plan => &self.plan,
			ProfilerCategory::Cdc => &self.cdc,
			ProfilerCategory::Flow => &self.flow,
			ProfilerCategory::Subscription => &self.subscription,
			ProfilerCategory::Server => &self.server,
			ProfilerCategory::Wire => &self.wire,
			ProfilerCategory::Auth => &self.auth,
			ProfilerCategory::Catalog => &self.catalog,
			ProfilerCategory::Engine => &self.engine,
			ProfilerCategory::Mutate => &self.mutate,
			ProfilerCategory::Transport => &self.transport,
			ProfilerCategory::Task => &self.task,
			ProfilerCategory::Policy => &self.policy,
			ProfilerCategory::Ffi => &self.ffi,
			ProfilerCategory::Cache => &self.cache,
			ProfilerCategory::Shape => &self.shape,
			ProfilerCategory::Api => &self.api,
			ProfilerCategory::Actor => &self.actor,
		}
	}

	pub fn register_into(&self, registry: &MetricsRegistry) {
		for category in ALL_CATEGORIES {
			registry.register_reporter(Arc::clone(self.histogram_for(category)) as Arc<dyn MetricsReporter>);
		}
		registry.register_reporter(Arc::clone(&self.accumulator_size) as Arc<dyn MetricsReporter>);
		registry.register_reporter(Arc::clone(&self.accumulator_capacity) as Arc<dyn MetricsReporter>);
		registry.register_reporter(Arc::clone(&self.accumulator_evictions) as Arc<dyn MetricsReporter>);
	}
}

impl Default for ProfilerInstruments {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_profiler::category::ALL_CATEGORIES;

	use super::*;

	#[test]
	fn each_category_has_distinct_histogram() {
		let instruments = ProfilerInstruments::new();
		let names: Vec<&str> = ALL_CATEGORIES.iter().map(|c| instruments.histogram_for(*c).name).collect();
		let mut unique = names.clone();
		unique.sort();
		unique.dedup();
		assert_eq!(unique.len(), names.len());
	}

	#[test]
	fn register_into_exports_every_instrument_exactly_once() {
		// Registration must cover every instrument, or a category silently vanishes from
		// the instruments surface.
		let instruments = ProfilerInstruments::new();
		let registry = MetricsRegistry::new();
		instruments.register_into(&registry);
		let samples = registry.read_reporters();
		assert_eq!(samples.len(), ALL_CATEGORIES.len() * 6 + 3);
	}

	#[test]
	fn two_instances_do_not_share_state() {
		// Instruments are per-database: observations must never blend across instances.
		let a = ProfilerInstruments::new();
		let b = ProfilerInstruments::new();
		a.histogram_for(ProfilerCategory::Query).observe(100.0);
		assert_eq!(a.histogram_for(ProfilerCategory::Query).count(), 1);
		assert_eq!(b.histogram_for(ProfilerCategory::Query).count(), 0);
	}
}
