// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use reifydb_runtime::sync::mutex::Mutex;

use crate::{
	MetricId,
	counter::Counter,
	gauge::Gauge,
	histogram::Histogram,
	snapshot::{MetricSnapshot, TakeSnapshot},
};

pub struct StaticMetricRegistry {
	sources: Mutex<Vec<&'static dyn TakeSnapshot>>,
}

impl Default for StaticMetricRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl StaticMetricRegistry {
	pub fn new() -> Self {
		Self {
			sources: Mutex::new(Vec::new()),
		}
	}

	pub fn register_counter(&self, counter: &'static Counter) {
		self.sources.lock().push(counter);
	}

	pub fn register_gauge(&self, gauge: &'static Gauge) {
		self.sources.lock().push(gauge);
	}

	pub fn register_histogram(&self, histogram: &'static Histogram) {
		self.sources.lock().push(histogram);
	}

	pub fn register(&self, source: &'static dyn TakeSnapshot) {
		self.sources.lock().push(source);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<MetricSnapshot> {
		self.sources.lock().iter().map(|s| s.snapshot()).collect()
	}
}

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

pub static STATIC_REGISTRY: LazyLock<StaticMetricRegistry> = LazyLock::new(StaticMetricRegistry::new);
