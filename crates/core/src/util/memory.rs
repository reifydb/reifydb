// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::sync::mutex::Mutex;

pub struct MemorySample {
	pub scope: &'static str,
	pub metric: &'static str,
	pub value: f64,
	pub unit: &'static str,
}

impl MemorySample {
	pub fn new(scope: &'static str, metric: &'static str, value: f64, unit: &'static str) -> Self {
		Self {
			scope,
			metric,
			value,
			unit,
		}
	}
}

pub trait MemoryReporter: Send + Sync {
	fn report(&self, out: &mut Vec<MemorySample>);
}

#[derive(Clone)]
pub struct MemoryRegistry {
	inner: Arc<Mutex<Vec<Arc<dyn MemoryReporter>>>>,
}

impl MemoryRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register(&self, reporter: Arc<dyn MemoryReporter>) {
		self.inner.lock().push(reporter);
	}

	pub fn register_all(&self, reporters: impl IntoIterator<Item = Arc<dyn MemoryReporter>>) {
		self.inner.lock().extend(reporters);
	}

	pub fn collect(&self) -> Vec<MemorySample> {
		let reporters: Vec<Arc<dyn MemoryReporter>> = self.inner.lock().clone();
		let mut out = Vec::new();
		for reporter in &reporters {
			reporter.report(&mut out);
		}
		out
	}
}

impl Default for MemoryRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::{MemoryRegistry, MemoryReporter, MemorySample};

	struct Fixed {
		scope: &'static str,
		bytes: f64,
	}

	impl MemoryReporter for Fixed {
		fn report(&self, out: &mut Vec<MemorySample>) {
			out.push(MemorySample::new(self.scope, "resident_bytes", self.bytes, "bytes"));
		}
	}

	#[test]
	fn collect_gathers_samples_from_every_registered_reporter() {
		let registry = MemoryRegistry::new();
		registry.register(Arc::new(Fixed {
			scope: "a",
			bytes: 10.0,
		}));
		registry.register(Arc::new(Fixed {
			scope: "b",
			bytes: 20.0,
		}));

		let samples = registry.collect();
		assert_eq!(samples.len(), 2, "every registered reporter must contribute its samples");
		let a = samples.iter().find(|s| s.scope == "a").expect("reporter a must appear");
		assert_eq!(a.metric, "resident_bytes");
		assert_eq!(a.value, 10.0);
		assert_eq!(a.unit, "bytes");
		assert!(samples.iter().any(|s| s.scope == "b" && s.value == 20.0), "reporter b must appear");
	}

	#[test]
	fn register_all_adds_every_reporter() {
		let registry = MemoryRegistry::new();
		let reporters: Vec<Arc<dyn MemoryReporter>> = vec![
			Arc::new(Fixed {
				scope: "x",
				bytes: 1.0,
			}),
			Arc::new(Fixed {
				scope: "y",
				bytes: 2.0,
			}),
		];
		registry.register_all(reporters);
		assert_eq!(registry.collect().len(), 2, "register_all must add all reporters at once");
	}

	#[test]
	fn a_clone_shares_the_same_reporter_list() {
		let registry = MemoryRegistry::new();
		let clone = registry.clone();
		clone.register(Arc::new(Fixed {
			scope: "shared",
			bytes: 5.0,
		}));
		assert_eq!(
			registry.collect().len(),
			1,
			"a clone must observe reporters registered through the other handle (shared Arc backing)"
		);
	}

	#[test]
	fn an_empty_registry_collects_nothing() {
		assert!(MemoryRegistry::new().collect().is_empty());
	}
}
