// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use reifydb_runtime::sync::mutex::Mutex;

use crate::{MetricId, counter::Counter, gauge::Gauge, histogram::Histogram, snapshot::MetricSnapshot};

/// Registry for metrics backed by `'static` references.
pub struct StaticMetricRegistry {
	counters: Mutex<Vec<&'static Counter>>,
	gauges: Mutex<Vec<&'static Gauge>>,
	histograms: Mutex<Vec<&'static Histogram>>,
}

impl Default for StaticMetricRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl StaticMetricRegistry {
	pub fn new() -> Self {
		Self {
			counters: Mutex::new(Vec::new()),
			gauges: Mutex::new(Vec::new()),
			histograms: Mutex::new(Vec::new()),
		}
	}

	pub fn register_counter(&self, counter: &'static Counter) {
		self.counters.lock().push(counter);
	}

	pub fn register_gauge(&self, gauge: &'static Gauge) {
		self.gauges.lock().push(gauge);
	}

	pub fn register_histogram(&self, histogram: &'static Histogram) {
		self.histograms.lock().push(histogram);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<MetricSnapshot> {
		let counters = self.counters.lock();
		let gauges = self.gauges.lock();
		let histograms = self.histograms.lock();
		let mut out = Vec::with_capacity(counters.len() + gauges.len() + histograms.len());

		for c in counters.iter() {
			out.push(MetricSnapshot::Counter(c.snapshot()));
		}
		for g in gauges.iter() {
			out.push(MetricSnapshot::Gauge(g.snapshot()));
		}
		for h in histograms.iter() {
			out.push(MetricSnapshot::Histogram(Box::new(h.snapshot())));
		}

		out
	}
}

/// Registry for per-object metrics.
pub struct MetricRegistry {
	gauges: DashMap<MetricId, Arc<Gauge>>,
}

impl Default for MetricRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl MetricRegistry {
	pub fn new() -> Self {
		Self {
			gauges: DashMap::new(),
		}
	}

	pub fn register_gauge(&self, id: MetricId, gauge: Arc<Gauge>) {
		self.gauges.insert(id, gauge);
	}

	pub fn get_gauge(&self, id: &MetricId) -> Option<Arc<Gauge>> {
		self.gauges.get(id).map(|r| r.value().clone())
	}

	pub fn unregister_gauge(&self, id: &MetricId) {
		self.gauges.remove(id);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<(MetricId, MetricSnapshot)> {
		self.gauges
			.iter()
			.map(|r| {
				let (id, gauge) = r.pair();
				(*id, MetricSnapshot::Gauge(gauge.snapshot()))
			})
			.collect()
	}
}

/// Global static-metric registry singleton.
pub static STATIC_REGISTRY: LazyLock<StaticMetricRegistry> = LazyLock::new(StaticMetricRegistry::new);
