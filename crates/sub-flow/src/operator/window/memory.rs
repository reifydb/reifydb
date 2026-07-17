// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc, Weak,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	util::memory::{MemoryReporter, MemorySample, StateMemory},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, count::Count};

pub struct WindowStateCell {
	entries: AtomicU64,
	bytes: AtomicU64,
}

impl WindowStateCell {
	pub fn new() -> Self {
		Self {
			entries: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
		}
	}

	pub fn record(&self, memory: StateMemory) {
		self.entries.store(memory.entries.as_u64(), Ordering::Relaxed);
		self.bytes.store(memory.bytes.as_bytes(), Ordering::Relaxed);
	}

	pub fn load(&self) -> StateMemory {
		StateMemory::new(
			Count::new(self.entries.load(Ordering::Relaxed)),
			ByteSize::from_bytes(self.bytes.load(Ordering::Relaxed)),
		)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowStateUsage {
	pub node: FlowNodeId,
	pub memory: StateMemory,
}

impl Default for WindowStateCell {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Clone)]
pub struct WindowStateRegistry {
	inner: Arc<Mutex<HashMap<FlowNodeId, Weak<WindowStateCell>>>>,
}

impl WindowStateRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn register(&self, node: FlowNodeId, cell: &Arc<WindowStateCell>) {
		self.inner.lock().insert(node, Arc::downgrade(cell));
	}

	pub fn collect(&self) -> Vec<WindowStateUsage> {
		let mut map = self.inner.lock();
		map.retain(|_, weak| weak.strong_count() > 0);
		let mut out: Vec<WindowStateUsage> = map
			.iter()
			.filter_map(|(node, weak)| {
				weak.upgrade().map(|cell| WindowStateUsage {
					node: *node,
					memory: cell.load(),
				})
			})
			.collect();
		out.sort_by_key(|usage| usage.node);
		out
	}
}

impl Default for WindowStateRegistry {
	fn default() -> Self {
		Self::new()
	}
}

pub struct WindowStateReporter {
	registry: WindowStateRegistry,
}

impl WindowStateReporter {
	pub fn new(registry: WindowStateRegistry) -> Self {
		Self {
			registry,
		}
	}
}

pub(crate) fn push_window_state_samples(out: &mut Vec<MemorySample>, usage: &WindowStateUsage) {
	let node = usage.node;
	out.push(MemorySample::new(
		format!("flow_node::{node}"),
		"window_state_entries",
		usage.memory.entries.as_u64() as f64,
		"count",
	));
	out.push(MemorySample::new(
		format!("flow_node::{node}"),
		"window_state_bytes",
		usage.memory.bytes.as_bytes() as f64,
		"bytes",
	));
}

impl MemoryReporter for WindowStateReporter {
	fn report(&self, out: &mut Vec<MemorySample>) {
		for usage in self.registry.collect() {
			push_window_state_samples(out, &usage);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		util::memory::{MemoryReporter, StateMemory},
	};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::{WindowStateCell, WindowStateRegistry, WindowStateReporter, WindowStateUsage};

	fn memory(entries: u64, bytes: u64) -> StateMemory {
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes))
	}

	fn usage(node: u64, entries: u64, bytes: u64) -> WindowStateUsage {
		WindowStateUsage {
			node: FlowNodeId(node),
			memory: memory(entries, bytes),
		}
	}

	#[test]
	fn collect_returns_recorded_values_sorted_by_node() {
		let registry = WindowStateRegistry::new();
		let cell_b = Arc::new(WindowStateCell::new());
		let cell_a = Arc::new(WindowStateCell::new());
		registry.register(FlowNodeId(2), &cell_b);
		registry.register(FlowNodeId(1), &cell_a);
		cell_a.record(memory(3, 300));
		cell_b.record(memory(7, 700));

		assert_eq!(registry.collect(), vec![usage(1, 3, 300), usage(2, 7, 700)]);
	}

	#[test]
	fn dropped_cells_are_pruned_so_dead_operators_stop_reporting() {
		let registry = WindowStateRegistry::new();
		let live = Arc::new(WindowStateCell::new());
		registry.register(FlowNodeId(1), &live);
		{
			let dead = Arc::new(WindowStateCell::new());
			dead.record(memory(9, 900));
			registry.register(FlowNodeId(2), &dead);
		}

		assert_eq!(
			registry.collect(),
			vec![usage(1, 0, 0)],
			"a dropped cell must vanish, not report stale values"
		);
	}

	#[test]
	fn re_registration_replaces_the_previous_cell() {
		let registry = WindowStateRegistry::new();
		let first = Arc::new(WindowStateCell::new());
		first.record(memory(1, 10));
		registry.register(FlowNodeId(5), &first);

		let second = Arc::new(WindowStateCell::new());
		second.record(memory(2, 20));
		registry.register(FlowNodeId(5), &second);

		assert_eq!(registry.collect(), vec![usage(5, 2, 20)], "an actor restart must supersede the old cell");
	}

	#[test]
	fn reporter_emits_entries_and_bytes_per_flow_node() {
		let registry = WindowStateRegistry::new();
		let cell = Arc::new(WindowStateCell::new());
		cell.record(memory(4, 4096));
		registry.register(FlowNodeId(7), &cell);

		let reporter = WindowStateReporter::new(registry);
		let mut out = Vec::new();
		reporter.report(&mut out);

		assert_eq!(out.len(), 2);
		assert_eq!(out[0].scope, "flow_node::7");
		assert_eq!(out[0].metric, "window_state_entries");
		assert_eq!(out[0].value, 4.0);
		assert_eq!(out[0].unit, "count");
		assert_eq!(out[1].scope, "flow_node::7");
		assert_eq!(out[1].metric, "window_state_bytes");
		assert_eq!(out[1].value, 4096.0);
		assert_eq!(out[1].unit, "bytes");
	}
}
