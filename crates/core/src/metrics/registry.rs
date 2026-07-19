// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::sync::mutex::Mutex;

use crate::metrics::{collect::MetricsCollector, report::MetricsReporter, sample::MetricsSample};

#[derive(Clone)]
pub struct MetricsRegistry {
	collectors: Arc<Mutex<Vec<Arc<dyn MetricsCollector>>>>,
	reporters: Arc<Mutex<Vec<Arc<dyn MetricsReporter>>>>,
}

impl MetricsRegistry {
	pub fn new() -> Self {
		Self {
			collectors: Arc::new(Mutex::new(Vec::new())),
			reporters: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register_collector(&self, collector: Arc<dyn MetricsCollector>) {
		self.collectors.lock().push(collector);
	}

	pub fn register_collectors(&self, collectors: impl IntoIterator<Item = Arc<dyn MetricsCollector>>) {
		self.collectors.lock().extend(collectors);
	}

	pub fn register_reporter(&self, reporter: Arc<dyn MetricsReporter>) {
		self.reporters.lock().push(reporter);
	}

	pub fn collect(&self) -> Vec<MetricsSample> {
		let collectors: Vec<Arc<dyn MetricsCollector>> = self.collectors.lock().clone();
		let mut out = Vec::new();
		for collector in &collectors {
			collector.collect(&mut out);
		}
		out
	}

	pub fn read_reporters(&self) -> Vec<MetricsSample> {
		let reporters: Vec<Arc<dyn MetricsReporter>> = self.reporters.lock().clone();
		let mut out = Vec::new();
		for reporter in &reporters {
			reporter.read(&mut out);
		}
		out
	}

	pub fn named_heap_bytes(&self) -> u64 {
		self.collect().iter().filter_map(|sample| sample.reading.heap_bytes()).sum()
	}
}

impl Default for MetricsRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_value::byte_size::ByteSize;

	use super::MetricsRegistry;
	use crate::metrics::{
		collect::MetricsCollector,
		report::MetricsReporter,
		sample::{MetricsSample, Reading},
	};

	struct Fixed {
		scope: &'static str,
		bytes: u64,
	}

	impl MetricsCollector for Fixed {
		fn collect(&self, out: &mut Vec<MetricsSample>) {
			out.push(MetricsSample::heap(self.scope, "resident_bytes", ByteSize::from_bytes(self.bytes)));
		}
	}

	struct Mixed;

	impl MetricsCollector for Mixed {
		fn collect(&self, out: &mut Vec<MetricsSample>) {
			out.push(MetricsSample::heap("mixed", "state_bytes", ByteSize::from_bytes(100)));
			out.push(MetricsSample::bytes("mixed", "payload_bytes", ByteSize::from_bytes(40)));
			out.push(MetricsSample::count("mixed", "entries", 7));
			out.push(MetricsSample::ratio("mixed", "hit_ratio", 0.5));
		}
	}

	struct Pushed;

	impl MetricsReporter for Pushed {
		fn read(&self, out: &mut Vec<MetricsSample>) {
			out.push(MetricsSample::count("instrument", "value", 3));
		}
	}

	#[test]
	fn collect_gathers_samples_from_every_registered_collector() {
		let registry = MetricsRegistry::new();
		registry.register_collector(Arc::new(Fixed {
			scope: "a",
			bytes: 10,
		}));
		registry.register_collector(Arc::new(Fixed {
			scope: "b",
			bytes: 20,
		}));

		let samples = registry.collect();
		assert_eq!(samples.len(), 2, "every registered collector must contribute its samples");
		let a = samples.iter().find(|s| s.scope == "a").expect("collector a must appear");
		assert_eq!(a.metric, "resident_bytes");
		assert_eq!(a.reading, Reading::Heap(ByteSize::from_bytes(10)));
		assert_eq!(a.reading.unit(), "bytes");
		assert!(
			samples.iter().any(|s| s.scope == "b" && s.reading.as_f64() == 20.0),
			"collector b must appear"
		);
	}

	#[test]
	fn register_collectors_adds_every_collector() {
		let registry = MetricsRegistry::new();
		let collectors: Vec<Arc<dyn MetricsCollector>> = vec![
			Arc::new(Fixed {
				scope: "x",
				bytes: 1,
			}),
			Arc::new(Fixed {
				scope: "y",
				bytes: 2,
			}),
		];
		registry.register_collectors(collectors);
		assert_eq!(registry.collect().len(), 2, "register_collectors must add all collectors at once");
	}

	#[test]
	fn a_clone_shares_the_same_collector_list() {
		let registry = MetricsRegistry::new();
		let clone = registry.clone();
		clone.register_collector(Arc::new(Fixed {
			scope: "shared",
			bytes: 5,
		}));
		assert_eq!(
			registry.collect().len(),
			1,
			"a clone must observe collectors registered through the other handle (shared Arc backing)"
		);
	}

	#[test]
	fn an_empty_registry_collects_nothing() {
		assert!(MetricsRegistry::new().collect().is_empty());
		assert!(MetricsRegistry::new().read_reporters().is_empty());
	}

	#[test]
	fn collectors_and_reporters_stay_on_separate_paths() {
		// collect() serves the pull side, read_reporters() the push side; blending them
		// would let instrument values leak into named_heap_bytes.
		let registry = MetricsRegistry::new();
		registry.register_collector(Arc::new(Fixed {
			scope: "pull",
			bytes: 10,
		}));
		registry.register_reporter(Arc::new(Pushed));

		let collected = registry.collect();
		assert_eq!(collected.len(), 1, "collect() must poll collectors only");
		assert_eq!(collected[0].scope, "pull");

		let read = registry.read_reporters();
		assert_eq!(read.len(), 1, "read_reporters() must read instruments only");
		assert_eq!(read[0].scope, "instrument");
	}

	#[test]
	fn named_heap_bytes_sums_only_heap_readings() {
		// Only Heap readings may count toward the reconciliation numerator, or dark_bytes
		// understates and hides real leaks.
		let registry = MetricsRegistry::new();
		registry.register_collector(Arc::new(Mixed));
		registry.register_collector(Arc::new(Fixed {
			scope: "store",
			bytes: 900,
		}));
		registry.register_reporter(Arc::new(Pushed));
		assert_eq!(
			registry.named_heap_bytes(),
			1000,
			"only Heap readings (100 + 900) count; payload bytes, counts, ratios, and instrument values must be excluded"
		);
	}
}
