// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct NodeRetentionInfo {
	pub node: FlowNodeId,
	pub stateful: bool,
	pub scale: Option<Duration>,
	pub frontier: Option<DateTime>,
}

#[derive(Clone)]
pub struct NodeRetentionStore {
	nodes: Arc<RwLock<HashMap<FlowNodeId, NodeRetentionInfo>>>,
}

impl Default for NodeRetentionStore {
	fn default() -> Self {
		Self::new()
	}
}

impl NodeRetentionStore {
	pub fn new() -> Self {
		Self {
			nodes: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn set(&self, info: NodeRetentionInfo) {
		self.nodes.write().insert(info.node, info);
	}

	pub fn set_frontier(&self, node: FlowNodeId, frontier: Option<DateTime>) {
		if let Some(info) = self.nodes.write().get_mut(&node) {
			info.frontier = frontier;
		}
	}

	pub fn remove(&self, node: FlowNodeId) {
		self.nodes.write().remove(&node);
	}

	pub fn get(&self, node: FlowNodeId) -> Option<NodeRetentionInfo> {
		self.nodes.read().get(&node).cloned()
	}

	pub fn list(&self) -> Vec<NodeRetentionInfo> {
		self.nodes.read().values().cloned().collect()
	}
}
