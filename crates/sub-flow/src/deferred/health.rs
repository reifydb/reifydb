// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Clone, Default)]
pub struct FlowHealthRegistry {
	poisoned: Arc<Mutex<BTreeMap<FlowId, String>>>,
}

impl FlowHealthRegistry {
	pub fn new() -> Self {
		Self {
			poisoned: Arc::new(Mutex::new(BTreeMap::new())),
		}
	}

	pub fn mark_poisoned(&self, flow_id: FlowId, reason: String) {
		self.poisoned.lock().insert(flow_id, reason);
	}

	pub fn clear(&self, flow_id: FlowId) {
		self.poisoned.lock().remove(&flow_id);
	}

	pub fn poisoned(&self) -> Vec<(FlowId, String)> {
		self.poisoned.lock().iter().map(|(id, reason)| (*id, reason.clone())).collect()
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
