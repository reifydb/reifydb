// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::RwLock;

use reifydb_core::interface::catalog::flow::FlowId;

pub(crate) struct ExecutionLevelCache {
	cache: RwLock<Option<Vec<Vec<FlowId>>>>,
}

impl ExecutionLevelCache {
	pub(crate) fn new() -> Self {
		Self {
			cache: RwLock::new(None),
		}
	}

	pub(crate) fn get(&self) -> Option<Vec<Vec<FlowId>>> {
		self.cache.read().unwrap().as_ref().cloned()
	}

	pub(crate) fn set(&self, levels: Vec<Vec<FlowId>>) {
		*self.cache.write().unwrap() = Some(levels);
	}

	pub(crate) fn invalidate(&self) {
		*self.cache.write().unwrap() = None;
	}
}
