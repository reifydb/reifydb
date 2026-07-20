// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	metrics::{collect::MetricsCollector, heap::OperatorSample, sample::MetricsSample},
	state::budget::OperatorStateBudgetHandle,
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

pub struct OperatorSampleCollector {
	registry: OperatorSampleRegistry,
}

impl OperatorSampleCollector {
	pub fn new(registry: OperatorSampleRegistry) -> Self {
		Self {
			registry,
		}
	}
}

pub(crate) fn push_operator_samples(out: &mut Vec<MetricsSample>, node: FlowNodeId, sample: &OperatorSample) {
	if let Some(memory) = sample.memory {
		out.push(MetricsSample::count(format!("flow_node::{node}"), "state_entries", memory.entries.as_u64()));
		out.push(MetricsSample::bytes(format!("flow_node::{node}"), "state_resident_bytes", memory.bytes));
	}
	if let Some(memory) = sample.dirty_memory {
		out.push(MetricsSample::count(
			format!("flow_node::{node}"),
			"state_dirty_entries",
			memory.entries.as_u64(),
		));
		out.push(MetricsSample::bytes(format!("flow_node::{node}"), "state_dirty_bytes", memory.bytes));
	}
	if let Some(memory) = sample.row_number_cache {
		out.push(MetricsSample::count(
			format!("flow_node::{node}"),
			"row_number_cache_entries",
			memory.entries.as_u64(),
		));
		out.push(MetricsSample::heap(format!("flow_node::{node}"), "row_number_cache_bytes", memory.bytes));
	}
}

impl MetricsCollector for OperatorSampleCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		for (node, sample) in self.registry.snapshot() {
			push_operator_samples(out, node, &sample);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		metrics::{
			collect::MetricsCollector,
			heap::{OperatorSample, StateMemory},
		},
	};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::{OperatorSampleCollector, OperatorSampleRegistry};

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
	fn collector_emits_entries_and_bytes_per_flow_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(7), memory_sample(4, 4096));

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert_eq!(out.len(), 2, "a memory sample must produce exactly the entries and bytes metrics");
		assert_eq!(out[0].scope, "flow_node::7");
		assert_eq!(out[0].metric, "state_entries");
		assert_eq!(out[0].reading.as_f64(), 4.0);
		assert_eq!(out[0].reading.unit(), "count");
		assert_eq!(out[1].scope, "flow_node::7");
		assert_eq!(out[1].metric, "state_resident_bytes");
		assert_eq!(out[1].reading.as_f64(), 4096.0);
		assert_eq!(out[1].reading.unit(), "bytes");
		assert_eq!(
			out[1].reading.heap_bytes(),
			None,
			"per-node state must not read as heap: the budget collector's operator_state cached_bytes \
			 is the single heap emitter, and a second one would double-count the same bytes in the \
			 named-bytes reconciliation"
		);
	}

	#[test]
	fn collector_emits_dirty_state_separately_from_resident() {
		// Dirty bytes are uncommitted operator state held in transaction slots. They are charged
		// to the budget on top of resident bytes, so folding them into state_resident_bytes would
		// hide soft overage, which is the one condition this metric pair exists to make visible.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::with_memory(StateMemory::new(Count::new(4), ByteSize::from_bytes(4096)))
			.with_dirty_memory(StateMemory::new(Count::new(1), ByteSize::from_bytes(512)));
		registry.record(FlowNodeId(7), sample);

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert_eq!(out.len(), 4, "the resident pair and the dirty pair must both emit");
		assert_eq!(out[2].metric, "state_dirty_entries");
		assert_eq!(out[2].reading.as_f64(), 1.0);
		assert_eq!(out[2].reading.unit(), "count");
		assert_eq!(out[3].metric, "state_dirty_bytes");
		assert_eq!(out[3].reading.as_f64(), 512.0);
		assert_eq!(
			out[3].reading.heap_bytes(),
			None,
			"dirty bytes are already counted inside the budget's cached_bytes heap reading"
		);
	}

	#[test]
	fn collector_skips_a_sample_with_no_memory() {
		let registry = OperatorSampleRegistry::new();
		registry.record(FlowNodeId(7), OperatorSample::default());

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert!(out.is_empty(), "a sample carrying no memory must not emit phantom zero rows");
	}

	#[test]
	fn collector_emits_row_number_cache_after_window_state() {
		// A windowed aggregate carries both window state and an in-process row-number
		// cache. The cache duplicates persisted state and must surface as its own metric
		// pair rather than being folded into window_state or left unaccounted.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::with_memory(StateMemory::new(Count::new(4), ByteSize::from_bytes(4096)))
			.with_row_number_cache(StateMemory::new(Count::new(9), ByteSize::from_bytes(900)));
		registry.record(FlowNodeId(7), sample);

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert_eq!(out.len(), 4, "both the window-state pair and the row-number-cache pair must emit");
		assert_eq!(out[2].metric, "row_number_cache_entries");
		assert_eq!(out[2].reading.as_f64(), 9.0);
		assert_eq!(out[2].reading.unit(), "count");
		assert_eq!(out[3].metric, "row_number_cache_bytes");
		assert_eq!(
			out[3].reading.heap_bytes(),
			Some(900),
			"the row-number cache is owned heap and must participate in the named-bytes reconciliation"
		);
	}

	#[test]
	fn collector_emits_row_number_cache_without_window_state() {
		// Join and distinct have no window state but do carry a row-number cache; it must
		// report even when memory is None, or their in-process footprint stays dark.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::default()
			.with_row_number_cache(StateMemory::new(Count::new(2), ByteSize::from_bytes(64)));
		registry.record(FlowNodeId(3), sample);

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert_eq!(out.len(), 2, "a row-number cache with no window state still emits its own pair");
		assert_eq!(out[0].metric, "row_number_cache_entries");
		assert_eq!(out[0].reading.as_f64(), 2.0);
		assert_eq!(out[1].metric, "row_number_cache_bytes");
		assert_eq!(out[1].reading.heap_bytes(), Some(64));
	}
}

pub struct OperatorStateBudgetCollector {
	budget: OperatorStateBudgetHandle,
}

impl OperatorStateBudgetCollector {
	pub fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			budget,
		}
	}
}

impl MetricsCollector for OperatorStateBudgetCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let snapshot = self.budget.snapshot();
		let cached = snapshot.resident.saturating_add(snapshot.dirty);
		out.push(MetricsSample::heap("operator_state", "cached_bytes", cached));
		out.push(MetricsSample::bytes("operator_state", "budget_bytes", snapshot.budget));
		out.push(MetricsSample::bytes("operator_state", "resident_bytes", snapshot.resident));
		out.push(MetricsSample::bytes("operator_state", "dirty_bytes", snapshot.dirty));
		out.push(MetricsSample::bytes("operator_state", "in_flight_bytes", snapshot.in_flight));
		out.push(MetricsSample::bytes("operator_state", "leased_bytes", snapshot.leased));
		out.push(MetricsSample::count("operator_state", "silent_leases", self.budget.silent_leases().as_u64()));
		out.push(MetricsSample::bytes("operator_state", "overage_bytes", snapshot.overage()));
		out.push(MetricsSample::count("operator_state", "evictions", self.budget.evictions().as_u64()));
	}
}
