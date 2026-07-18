// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	util::memory::{MemoryReporter, MemorySample, OperatorSample},
};
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Clone)]
pub struct OperatorSampleRegistry {
	inner: Arc<Mutex<HashMap<FlowNodeId, OperatorSample>>>,
}

impl OperatorSampleRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn record(&self, node: FlowNodeId, sample: OperatorSample) {
		self.inner.lock().insert(node, sample);
	}

	pub fn forget(&self, node: FlowNodeId) {
		self.inner.lock().remove(&node);
	}

	pub fn snapshot(&self) -> Vec<(FlowNodeId, OperatorSample)> {
		let mut out: Vec<(FlowNodeId, OperatorSample)> =
			self.inner.lock().iter().map(|(node, sample)| (*node, *sample)).collect();
		out.sort_by_key(|(node, _)| *node);
		out
	}
}

impl Default for OperatorSampleRegistry {
	fn default() -> Self {
		Self::new()
	}
}

pub struct OperatorSampleReporter {
	registry: OperatorSampleRegistry,
}

impl OperatorSampleReporter {
	pub fn new(registry: OperatorSampleRegistry) -> Self {
		Self {
			registry,
		}
	}
}

pub(crate) fn push_operator_samples(out: &mut Vec<MemorySample>, node: FlowNodeId, sample: &OperatorSample) {
	if let Some(memory) = sample.memory {
		out.push(MemorySample::new(
			format!("flow_node::{node}"),
			"window_state_entries",
			memory.entries.as_u64() as f64,
			"count",
		));
		out.push(MemorySample::new(
			format!("flow_node::{node}"),
			"window_state_bytes",
			memory.bytes.as_bytes() as f64,
			"bytes",
		));
	}
	if let Some(memory) = sample.row_number_cache {
		out.push(MemorySample::new(
			format!("flow_node::{node}"),
			"row_number_cache_entries",
			memory.entries.as_u64() as f64,
			"count",
		));
		out.push(MemorySample::new(
			format!("flow_node::{node}"),
			"row_number_cache_bytes",
			memory.bytes.as_bytes() as f64,
			"bytes",
		));
	}
}

impl MemoryReporter for OperatorSampleReporter {
	fn report(&self, out: &mut Vec<MemorySample>) {
		for (node, sample) in self.registry.snapshot() {
			push_operator_samples(out, node, &sample);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		util::memory::{MemoryReporter, OperatorSample, StateMemory},
	};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::{OperatorSampleRegistry, OperatorSampleReporter};

	fn memory_sample(entries: u64, bytes: u64) -> OperatorSample {
		OperatorSample::with_memory(StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes)))
	}

	#[test]
	fn snapshot_returns_recorded_samples_sorted_by_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(2), memory_sample(7, 700));
		registry.record(FlowNodeId(1), memory_sample(3, 300));

		assert_eq!(
			registry.snapshot(),
			vec![(FlowNodeId(1), memory_sample(3, 300)), (FlowNodeId(2), memory_sample(7, 700))],
			"snapshot must be ordered by node so the metric log is stable across runs"
		);
	}

	#[test]
	fn record_overwrites_the_previous_sample_for_a_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(5), memory_sample(1, 10));
		registry.record(FlowNodeId(5), memory_sample(2, 20));

		assert_eq!(
			registry.snapshot(),
			vec![(FlowNodeId(5), memory_sample(2, 20))],
			"a fresh sample must supersede the stale one, not accumulate"
		);
	}

	#[test]
	fn forget_removes_a_stopped_operators_sample() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(1), memory_sample(3, 300));
		registry.record(FlowNodeId(2), memory_sample(7, 700));
		registry.forget(FlowNodeId(2));

		assert_eq!(
			registry.snapshot(),
			vec![(FlowNodeId(1), memory_sample(3, 300))],
			"a forgotten node must vanish so a stopped flow stops reporting stale memory"
		);
	}

	#[test]
	fn a_clone_shares_the_same_backing_map() {
		let registry = OperatorSampleRegistry::new();
		let clone = registry.clone();
		clone.record(FlowNodeId(9), memory_sample(1, 1));

		assert_eq!(
			registry.snapshot().len(),
			1,
			"a clone must observe records made through the other handle (shared Arc backing)"
		);
	}

	#[test]
	fn reporter_emits_entries_and_bytes_per_flow_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(7), memory_sample(4, 4096));

		let reporter = OperatorSampleReporter::new(registry);
		let mut out = Vec::new();
		reporter.report(&mut out);

		assert_eq!(out.len(), 2, "a memory sample must produce exactly the entries and bytes metrics");
		assert_eq!(out[0].scope, "flow_node::7");
		assert_eq!(out[0].metric, "window_state_entries");
		assert_eq!(out[0].value, 4.0);
		assert_eq!(out[0].unit, "count");
		assert_eq!(out[1].scope, "flow_node::7");
		assert_eq!(out[1].metric, "window_state_bytes");
		assert_eq!(out[1].value, 4096.0);
		assert_eq!(out[1].unit, "bytes");
	}

	#[test]
	fn reporter_skips_a_sample_with_no_memory() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(7), OperatorSample::default());

		let reporter = OperatorSampleReporter::new(registry);
		let mut out = Vec::new();
		reporter.report(&mut out);

		assert!(out.is_empty(), "a sample carrying no memory must not emit phantom zero rows");
	}

	#[test]
	fn reporter_emits_row_number_cache_after_window_state() {
		// A windowed aggregate carries both window state and an in-process row-number
		// cache. The cache duplicates persisted state and must surface as its own metric
		// pair rather than being folded into window_state or left unaccounted.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::with_memory(StateMemory::new(Count::new(4), ByteSize::from_bytes(4096)))
			.with_row_number_cache(StateMemory::new(Count::new(9), ByteSize::from_bytes(900)));
		registry.record(FlowNodeId(7), sample);

		let reporter = OperatorSampleReporter::new(registry);
		let mut out = Vec::new();
		reporter.report(&mut out);

		assert_eq!(out.len(), 4, "both the window-state pair and the row-number-cache pair must emit");
		assert_eq!(out[2].metric, "row_number_cache_entries");
		assert_eq!(out[2].value, 9.0);
		assert_eq!(out[2].unit, "count");
		assert_eq!(out[3].metric, "row_number_cache_bytes");
		assert_eq!(out[3].value, 900.0);
		assert_eq!(out[3].unit, "bytes");
	}

	#[test]
	fn reporter_emits_row_number_cache_without_window_state() {
		// Join and distinct have no window state but do carry a row-number cache; it must
		// report even when memory is None, or their in-process footprint stays dark.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::default()
			.with_row_number_cache(StateMemory::new(Count::new(2), ByteSize::from_bytes(64)));
		registry.record(FlowNodeId(3), sample);

		let reporter = OperatorSampleReporter::new(registry);
		let mut out = Vec::new();
		reporter.report(&mut out);

		assert_eq!(out.len(), 2, "a row-number cache with no window state still emits its own pair");
		assert_eq!(out[0].metric, "row_number_cache_entries");
		assert_eq!(out[0].value, 2.0);
		assert_eq!(out[1].metric, "row_number_cache_bytes");
		assert_eq!(out[1].value, 64.0);
	}
}
