// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	metrics::{
		collect::MetricsCollector,
		heap::{DropCounts, OperatorSample},
		operator::{
			DROPPED_CHANGES_PAST_IMMUTABLE, DROPPED_INSERTS_PAST_SEAL, DROPPED_REFUSED_SESSION_ROWS,
			DROPPED_REMOVES_OF_FORGOTTEN_DISTINCT, DROPPED_REMOVES_PAST_SEAL,
			DROPPED_ROWS_IN_SEALED_WINDOWS, DROPPED_UPDATES_PAST_SEAL, ROW_NUMBER_CACHE_BYTES,
			STATE_RESIDENT_BYTES,
		},
		sample::MetricsSample,
	},
};
use reifydb_runtime::sync::mutex::Mutex;

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

	pub fn unregister(&self, operator: OperatorId) {
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
	push_drop_counts(out, operator, &sample.drops);
}

fn push_drop_counts(out: &mut Vec<MetricsSample>, operator: OperatorId, drops: &DropCounts) {
	let counts = [
		(DROPPED_INSERTS_PAST_SEAL, drops.inserts_past_seal),
		(DROPPED_UPDATES_PAST_SEAL, drops.updates_past_seal),
		(DROPPED_REMOVES_PAST_SEAL, drops.removes_past_seal),
		(DROPPED_ROWS_IN_SEALED_WINDOWS, drops.rows_in_sealed_windows),
		(DROPPED_CHANGES_PAST_IMMUTABLE, drops.changes_past_immutable),
		(DROPPED_REFUSED_SESSION_ROWS, drops.refused_session_rows),
		(DROPPED_REMOVES_OF_FORGOTTEN_DISTINCT, drops.removes_of_forgotten_distinct),
	];
	for (metric, count) in counts {
		if let Some(count) = count {
			out.push(MetricsSample::counter(format!("flow_node::{operator}"), metric, count));
		}
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
			heap::{DropCounts, OperatorSample, StateMemory},
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
	fn unregister_removes_a_stopped_operators_sample() {
		let registry = OperatorSampleRegistry::new();
		registry.record(OperatorId(1), memory_sample(3, 300));
		registry.record(OperatorId(2), memory_sample(7, 700));
		registry.unregister(OperatorId(2));

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
	fn a_drop_count_reaches_metrics_under_its_reason_name() {
		// each reason must land under its own name, or a dashboard blames the wrong kind of late input
		let registry = OperatorSampleRegistry::new();
		registry.record(
			OperatorId(9),
			OperatorSample::default().with_drops(DropCounts {
				inserts_past_seal: Some(1),
				updates_past_seal: Some(2),
				removes_past_seal: Some(3),
				rows_in_sealed_windows: Some(4),
				changes_past_immutable: Some(5),
				refused_session_rows: Some(6),
				removes_of_forgotten_distinct: Some(7),
			}),
		);

		let mut out = Vec::new();
		OperatorSampleCollector::new(registry).collect(&mut out);

		let emitted: Vec<(&str, &str, f64)> =
			out.iter().map(|sample| (&*sample.scope, sample.metric, sample.reading.as_f64())).collect();
		assert_eq!(
			emitted,
			vec![
				("flow_node::9", "dropped_inserts_past_seal", 1.0),
				("flow_node::9", "dropped_updates_past_seal", 2.0),
				("flow_node::9", "dropped_removes_past_seal", 3.0),
				("flow_node::9", "dropped_rows_in_sealed_windows", 4.0),
				("flow_node::9", "dropped_changes_past_immutable", 5.0),
				("flow_node::9", "dropped_refused_session_rows", 6.0),
				("flow_node::9", "dropped_removes_of_forgotten_distinct", 7.0),
			]
		);
	}

	#[test]
	fn a_reason_the_operator_does_not_have_emits_no_metric() {
		// a reason the operator has must show even at zero, and one it lacks must never show as a fake zero
		let registry = OperatorSampleRegistry::new();
		registry.record(
			OperatorId(9),
			OperatorSample::default().with_drops(DropCounts {
				inserts_past_seal: Some(0),
				..DropCounts::default()
			}),
		);

		let mut out = Vec::new();
		OperatorSampleCollector::new(registry).collect(&mut out);

		let emitted: Vec<(&str, f64)> =
			out.iter().map(|sample| (sample.metric, sample.reading.as_f64())).collect();
		assert_eq!(emitted, vec![("dropped_inserts_past_seal", 0.0)]);
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
