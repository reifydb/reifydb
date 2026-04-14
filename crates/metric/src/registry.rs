// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock, RwLock};

use dashmap::DashMap;

use crate::{MetricId, counter::Counter, gauge::Gauge, histogram::Histogram, snapshot::MetricSnapshot};

/// Static registry for fixed system metrics.
pub struct SystemMetricRegistry {
	counters: RwLock<Vec<&'static Counter>>,
	gauges: RwLock<Vec<&'static Gauge>>,
	histograms: RwLock<Vec<&'static Histogram>>,
}

impl Default for SystemMetricRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemMetricRegistry {
	pub fn new() -> Self {
		Self {
			counters: RwLock::new(Vec::new()),
			gauges: RwLock::new(Vec::new()),
			histograms: RwLock::new(Vec::new()),
		}
	}

	pub fn register_counter(&self, counter: &'static Counter) {
		self.counters.write().unwrap().push(counter);
	}

	pub fn register_gauge(&self, gauge: &'static Gauge) {
		self.gauges.write().unwrap().push(gauge);
	}

	pub fn register_histogram(&self, histogram: &'static Histogram) {
		self.histograms.write().unwrap().push(histogram);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<MetricSnapshot> {
		let counters = self.counters.read().unwrap();
		let gauges = self.gauges.read().unwrap();
		let histograms = self.histograms.read().unwrap();
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

/// Global system metric registry singleton.
pub static SYSTEM_REGISTRY: LazyLock<SystemMetricRegistry> = LazyLock::new(SystemMetricRegistry::new);
