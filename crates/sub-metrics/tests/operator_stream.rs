// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `Domain::Operators` is the complete per-operator view: the engine's disk payload plus every
//! registered operator collector. Both the `::current` vtable and `SampleReader` read it through
//! `collect_operators`, so this pins the one definition they share. When the per-operator metrics
//! lived in `runtime::memory` instead, a consumer reading this domain saw only disk and silently
//! reported zero memory for every operator.

use std::sync::Arc;

use reifydb_core::metrics::{collect::MetricsCollector, registry::MetricsRegistry, sample::MetricsSample};
use reifydb_sub_metrics::domains::runtime::{Domain, SampleReader, collect::Collectors};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::byte_size::ByteSize;

struct OperatorState;

impl MetricsCollector for OperatorState {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::bytes("flow_node::7", "state_resident_bytes", ByteSize::from_bytes(4096)));
	}
}

struct OperatorHeap;

impl MetricsCollector for OperatorHeap {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap("flow_node::7", "group_cache_bytes", ByteSize::from_bytes(4096)));
	}
}

struct ProcessWide;

impl MetricsCollector for ProcessWide {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap("commit_buffer", "current_bytes", ByteSize::from_bytes(2048)));
	}
}

fn reader_with(registry: MetricsRegistry) -> SampleReader {
	let engine = TestEngine::new();
	SampleReader::new(Collectors {
		engine: (*engine).clone(),
		registry,
	})
}

#[test]
fn the_operators_domain_carries_registered_operator_collectors() {
	// Without this, the domain degrades to disk-only and every consumer reports operators as
	// holding no memory at all, which is indistinguishable from operators that are genuinely idle.
	let registry = MetricsRegistry::new();
	registry.register_operator_collector(Arc::new(OperatorState));

	let samples = reader_with(registry).samples_for(Domain::Operators);

	let state = samples
		.iter()
		.find(|sample| sample.metric == "state_resident_bytes")
		.expect("the operators domain must carry per-operator state, not only disk payload");
	assert_eq!(state.scope, "flow_node::7", "per-operator samples stay scoped to their operator");
	assert_eq!(state.reading.as_f64(), 4096.0);
}

#[test]
fn the_operators_domain_excludes_process_wide_collectors() {
	// The split is the whole point of the merge: runtime::memory answers "where is process memory",
	// runtime::operators answers "which operator holds it". Leaking subsystem totals into the
	// operator stream would make them look like an operator named commit_buffer.
	let registry = MetricsRegistry::new();
	registry.register_collector(Arc::new(ProcessWide));

	let samples = reader_with(registry).samples_for(Domain::Operators);

	assert!(
		!samples.iter().any(|sample| sample.scope == "commit_buffer"),
		"a process-wide collector must not appear in the operator stream"
	);
}

#[test]
fn the_memory_domain_excludes_operator_collectors() {
	// The other direction of the same split: per-operator rows in runtime::memory are what forced
	// raptor to reassemble one operator's footprint from two domains by hand.
	let registry = MetricsRegistry::new();
	registry.register_operator_collector(Arc::new(OperatorState));

	let samples = reader_with(registry).samples_for(Domain::Memory);

	assert!(
		!samples.iter().any(|sample| sample.scope.starts_with("flow_node::")),
		"operator collectors must not appear in the memory stream"
	);
}

#[test]
fn operator_heap_counts_toward_named_bytes() {
	// named_bytes is the numerator of the dark_bytes reconciliation, and collect_memory built it by
	// summing only the samples it had already pushed. The operator collectors live in a bucket that
	// push_subsystem_samples never reads, so every operator cache landed in dark_bytes and read as an
	// unattributed leak of exactly the operator heap. The roll-up keeps the per-operator rows out of
	// the memory stream (the split above) while still paying them into the numerator.
	let registry = MetricsRegistry::new();
	registry.register_operator_collector(Arc::new(OperatorHeap));

	let samples = reader_with(registry).samples_for(Domain::Memory);

	let rollup = samples
		.iter()
		.find(|sample| sample.scope == "flow_operators")
		.expect("the memory stream must carry the operator heap roll-up");
	assert_eq!(rollup.metric, "resident_bytes");
	assert_eq!(rollup.reading.as_f64(), 4096.0, "the roll-up sums the operator bucket, not the memory bucket");
	assert_eq!(rollup.reading.heap_bytes(), Some(4096), "a non-heap roll-up would leave the bytes dark");

	let named = samples
		.iter()
		.find(|sample| sample.metric == "named_bytes")
		.expect("the memory stream must carry the reconciliation numerator");
	let heap: u64 = samples.iter().filter_map(|sample| sample.reading.heap_bytes()).sum();
	assert_eq!(named.reading.as_f64() as u64, heap, "named_bytes must account for every heap sample it emits");
}
