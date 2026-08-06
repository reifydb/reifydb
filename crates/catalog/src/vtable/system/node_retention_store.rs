// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct NodeRetentionInfo {
	pub operator: OperatorId,
	pub stateful: bool,
	pub scale: Option<Duration>,
	pub frontier: Option<DateTime>,
}

#[derive(Clone)]
pub struct NodeRetentionStore {
	operators: Arc<RwLock<HashMap<OperatorId, NodeRetentionInfo>>>,
}

impl Default for NodeRetentionStore {
	fn default() -> Self {
		Self::new()
	}
}

impl NodeRetentionStore {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn set(&self, info: NodeRetentionInfo) {
		self.operators.write().insert(info.operator, info);
	}

	pub fn set_frontier(&self, operator: OperatorId, frontier: Option<DateTime>) {
		if let Some(info) = self.operators.write().get_mut(&operator) {
			info.frontier = frontier;
		}
	}

	pub fn remove(&self, operator: OperatorId) {
		self.operators.write().remove(&operator);
	}

	pub fn get(&self, operator: OperatorId) -> Option<NodeRetentionInfo> {
		self.operators.read().get(&operator).cloned()
	}
}
