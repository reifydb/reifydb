// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_runtime::sync::rwlock::RwLock;

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
		self.cache.read().as_ref().cloned()
	}

	pub(crate) fn set(&self, levels: Vec<Vec<FlowId>>) {
		*self.cache.write() = Some(levels);
	}

	pub(crate) fn invalidate(&self) {
		*self.cache.write() = None;
	}
}
