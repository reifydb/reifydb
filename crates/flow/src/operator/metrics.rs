// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	metrics::{
		collect::MetricsCollector,
		heap::OperatorSample,
		operator::{ROW_NUMBER_CACHE_BYTES, STATE_RESIDENT_BYTES},
		sample::MetricsSample,
	},
};
use reifydb_runtime::sync::mutex::Mutex;

use crate::transaction::row_number::RowNumberProvider;

#[derive(Clone)]
pub struct OperatorSampleRegistry {
	inner: Arc<Mutex<HashMap<OperatorId, OperatorSample>>>,
}

impl OperatorSampleRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn record(&self, operator: OperatorId, sample: OperatorSample) {
		self.inner.lock().insert(operator, sample);
	}

	pub fn forget(&self, operator: OperatorId) {
		self.inner.lock().remove(&operator);
	}

	pub fn snapshot(&self) -> Vec<(OperatorId, OperatorSample)> {
		let mut out: Vec<(OperatorId, OperatorSample)> =
			self.inner.lock().iter().map(|(operator, sample)| (*operator, *sample)).collect();
		out.sort_by_key(|(operator, _)| *operator);
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

pub(crate) fn push_operator_samples(out: &mut Vec<MetricsSample>, operator: OperatorId, sample: &OperatorSample) {
	if let Some(memory) = sample.memory {
		out.push(MetricsSample::count(
			format!("flow_node::{operator}"),
			"state_entries",
			memory.entries.as_u64(),
		));
		out.push(MetricsSample::bytes(format!("flow_node::{operator}"), STATE_RESIDENT_BYTES, memory.bytes));
	}
	if let Some(memory) = sample.row_number_cache {
		out.push(MetricsSample::count(
			format!("flow_node::{operator}"),
			"row_number_cache_entries",
			memory.entries.as_u64(),
		));
		out.push(MetricsSample::heap(format!("flow_node::{operator}"), ROW_NUMBER_CACHE_BYTES, memory.bytes));
	}
}

impl MetricsCollector for OperatorSampleCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		for (operator, sample) in self.registry.snapshot() {
			push_operator_samples(out, operator, &sample);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
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
		registry.record(OperatorId(2), memory_sample(7, 700));
		registry.record(OperatorId(1), memory_sample(3, 300));

		assert_eq!(
			registry.snapshot(),
			vec![(OperatorId(1), memory_sample(3, 300)), (OperatorId(2), memory_sample(7, 700))],
			"snapshot must be ordered by operator so the metric log is stable across runs"
		);
	}

	#[test]
	fn record_overwrites_the_previous_sample_for_a_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(OperatorId(5), memory_sample(1, 10));
		registry.record(OperatorId(5), memory_sample(2, 20));

		assert_eq!(
			registry.snapshot(),
			vec![(OperatorId(5), memory_sample(2, 20))],
			"a fresh sample must supersede the stale one, not accumulate"
		);
	}

	#[test]
	fn forget_removes_a_stopped_operators_sample() {
		let registry = OperatorSampleRegistry::new();
		registry.record(OperatorId(1), memory_sample(3, 300));
		registry.record(OperatorId(2), memory_sample(7, 700));
		registry.forget(OperatorId(2));

		assert_eq!(
			registry.snapshot(),
			vec![(OperatorId(1), memory_sample(3, 300))],
			"a forgotten operator must vanish so a stopped flow stops reporting stale memory"
		);
	}

	#[test]
	fn a_clone_shares_the_same_backing_map() {
		let registry = OperatorSampleRegistry::new();
		let clone = registry.clone();
		clone.record(OperatorId(9), memory_sample(1, 1));

		assert_eq!(
			registry.snapshot().len(),
			1,
			"a clone must observe records made through the other handle (shared Arc backing)"
		);
	}

	#[test]
	fn collector_emits_entries_and_bytes_per_flow_node() {
		let registry = OperatorSampleRegistry::new();
		registry.record(OperatorId(7), memory_sample(4, 4096));

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
			"per-operator state must not read as heap: the budget collector's operator_state cached_bytes \
			 is the single heap emitter, and a second one would double-count the same bytes in the \
			 named-bytes reconciliation"
		);
	}

	#[test]
	fn collector_skips_a_sample_with_no_memory() {
		let registry = OperatorSampleRegistry::new();
		registry.record(OperatorId(7), OperatorSample::default());

		let collector = OperatorSampleCollector::new(registry);
		let mut out = Vec::new();
		collector.collect(&mut out);

		assert!(out.is_empty(), "a sample carrying no memory must not emit phantom zero rows");
	}

	#[test]
	fn collector_emits_row_number_cache_after_window_state() {
		// The row-number cache duplicates persisted state, so it needs its own pair rather than
		// being folded into window_state or left unaccounted.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::with_memory(StateMemory::new(Count::new(4), ByteSize::from_bytes(4096)))
			.with_row_number_cache(StateMemory::new(Count::new(9), ByteSize::from_bytes(900)));
		registry.record(OperatorId(7), sample);

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
		// Join and distinct carry a row-number cache but no window state, so the pair must emit
		// with memory None or their in-process footprint stays dark.
		let registry = OperatorSampleRegistry::new();
		let sample = OperatorSample::default()
			.with_row_number_cache(StateMemory::new(Count::new(2), ByteSize::from_bytes(64)));
		registry.record(OperatorId(3), sample);

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

pub struct RowNumberMetricsCollector {
	provider: RowNumberProvider,
}

impl RowNumberMetricsCollector {
	pub fn new(provider: RowNumberProvider) -> Self {
		Self {
			provider,
		}
	}
}

impl MetricsCollector for RowNumberMetricsCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		for (operator, sample) in self.provider.samples() {
			let scope = format!("flow_node::{operator}");
			if sample.cache.entries.as_u64() > 0 || sample.cache.bytes.as_bytes() > 0 {
				out.push(MetricsSample::count(
					scope.clone(),
					"row_number_cache_entries",
					sample.cache.entries.as_u64(),
				));
				out.push(MetricsSample::heap(
					scope.clone(),
					ROW_NUMBER_CACHE_BYTES,
					sample.cache.bytes,
				));
			}
			out.push(MetricsSample::count(
				scope.clone(),
				"row_number_values_complete",
				sample.completeness.values_complete as u64,
			));
			out.push(MetricsSample::count(
				scope.clone(),
				"row_number_membership_complete",
				sample.completeness.membership_complete as u64,
			));
			for (metric, count) in [
				("row_number_absences_served", sample.completeness.absences_served),
				("row_number_revocations", sample.completeness.revocations),
			] {
				if count.as_u64() > 0 {
					out.push(MetricsSample::counter(scope.clone(), metric, count.as_u64()));
				}
			}
		}
	}
}
