// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{LazyLock, RwLock};

use crate::{counter::Counter, gauge::Gauge, histogram::Histogram, snapshot::MetricSnapshot};

/// Central registry of all metrics primitives.
///
/// Counters/gauges register themselves; the collector snapshots them on Tick.
pub struct MetricRegistry {
	counters: RwLock<Vec<&'static Counter>>,
	gauges: RwLock<Vec<&'static Gauge>>,
	histograms: RwLock<Vec<&'static Histogram>>,
}

impl Default for MetricRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl MetricRegistry {
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

/// Global metric registry singleton.
pub static REGISTRY: LazyLock<MetricRegistry> = LazyLock::new(MetricRegistry::new);
