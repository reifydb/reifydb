// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::value::duration::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct NodeHorizonInfo {
	pub node: FlowNodeId,
	pub stateful: bool,
	pub span: Option<Duration>,
}

#[derive(Clone)]
pub struct NodeHorizonStore {
	horizons: Arc<RwLock<HashMap<FlowNodeId, NodeHorizonInfo>>>,
}

impl Default for NodeHorizonStore {
	fn default() -> Self {
		Self::new()
	}
}

impl NodeHorizonStore {
	pub fn new() -> Self {
		Self {
			horizons: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn set(&self, info: NodeHorizonInfo) {
		self.horizons.write().insert(info.node, info);
	}

	pub fn remove(&self, node: FlowNodeId) {
		self.horizons.write().remove(&node);
	}

	pub fn get(&self, node: FlowNodeId) -> Option<NodeHorizonInfo> {
		self.horizons.read().get(&node).cloned()
	}

	pub fn list(&self) -> Vec<NodeHorizonInfo> {
		self.horizons.read().values().cloned().collect()
	}
}
