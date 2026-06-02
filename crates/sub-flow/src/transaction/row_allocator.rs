// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use reifydb_core::interface::catalog::flow::FlowNodeId;

#[derive(Debug, Default)]
pub struct RowAllocatorRegistry {
	nodes: DashMap<FlowNodeId, AtomicU64>,
}

impl RowAllocatorRegistry {
	pub fn new() -> Self {
		Self {
			nodes: DashMap::new(),
		}
	}

	pub fn is_seeded(&self, node: FlowNodeId) -> bool {
		self.nodes.contains_key(&node)
	}

	pub fn allocate(&self, node: FlowNodeId, count: u64, seed: u64) -> u64 {
		self.nodes.entry(node).or_insert_with(|| AtomicU64::new(seed)).fetch_add(count, Ordering::SeqCst)
	}

	pub fn high_water(&self, node: FlowNodeId) -> Option<u64> {
		self.nodes.get(&node).map(|c| c.load(Ordering::SeqCst))
	}

	pub fn evict(&self, node: FlowNodeId) {
		self.nodes.remove(&node);
	}
}
