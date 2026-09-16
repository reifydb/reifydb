// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	interface::catalog::flow::FlowId,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Clone, Default)]
pub struct FlowHealthRegistry {
	poisoned: Arc<Mutex<BTreeMap<FlowId, String>>>,
	stalled: Arc<Mutex<BTreeSet<FlowId>>>,
	stalls: Arc<AtomicU64>,
}

impl FlowHealthRegistry {
	pub fn new() -> Self {
		Self {
			poisoned: Arc::new(Mutex::new(BTreeMap::new())),
			stalled: Arc::new(Mutex::new(BTreeSet::new())),
			stalls: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn mark_poisoned(&self, flow_id: FlowId, reason: String) {
		self.poisoned.lock().insert(flow_id, reason);
	}

	pub fn mark_stalled(&self, flow_id: FlowId) {
		if self.stalled.lock().insert(flow_id) {
			self.stalls.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn clear_stall(&self, flow_id: FlowId) {
		self.stalled.lock().remove(&flow_id);
	}

	pub fn clear(&self, flow_id: FlowId) {
		self.poisoned.lock().remove(&flow_id);
		self.stalled.lock().remove(&flow_id);
	}

	pub fn poisoned(&self) -> Vec<(FlowId, String)> {
		self.poisoned.lock().iter().map(|(id, reason)| (*id, reason.clone())).collect()
	}

	pub fn stalled_count(&self) -> usize {
		self.stalled.lock().len()
	}

	pub fn stalls(&self) -> u64 {
		self.stalls.load(Ordering::Relaxed)
	}
}

impl MetricsCollector for FlowHealthRegistry {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::count("flow_health", "poisoned", self.poisoned.lock().len() as u64));
		out.push(MetricsSample::count("flow_health", "stalled", self.stalled_count() as u64));
		out.push(MetricsSample::counter("flow_health", "stalls", self.stalls()));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn poison_records_and_clear_removes_ordered_by_flow_id() {
		let health = FlowHealthRegistry::new();
		assert!(health.poisoned().is_empty());

		health.mark_poisoned(FlowId(7), "boom".to_string());
		health.mark_poisoned(FlowId(2), "kaboom".to_string());

		// Ordered by flow id, not insertion order.
		assert_eq!(health.poisoned(), vec![(FlowId(2), "kaboom".to_string()), (FlowId(7), "boom".to_string())]);

		health.clear(FlowId(2));
		assert_eq!(health.poisoned(), vec![(FlowId(7), "boom".to_string())]);

		health.clear(FlowId(7));
		assert!(health.poisoned().is_empty());
	}

	#[test]
	fn re_poison_overwrites_reason() {
		let health = FlowHealthRegistry::new();
		health.mark_poisoned(FlowId(1), "first".to_string());
		health.mark_poisoned(FlowId(1), "second".to_string());
		assert_eq!(health.poisoned(), vec![(FlowId(1), "second".to_string())]);
	}

	#[test]
	fn clones_share_the_same_map() {
		let a = FlowHealthRegistry::new();
		let b = a.clone();
		a.mark_poisoned(FlowId(3), "x".to_string());
		assert_eq!(b.poisoned().len(), 1);
	}
}
