// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::schedule::FlowSchedule;
use reifydb_runtime::sync::rwlock::RwLock;

pub(crate) struct ScheduleCache {
	cache: RwLock<Option<FlowSchedule>>,
}

impl ScheduleCache {
	pub(crate) fn new() -> Self {
		Self {
			cache: RwLock::new(None),
		}
	}

	pub(crate) fn get(&self) -> Option<FlowSchedule> {
		self.cache.read().as_ref().cloned()
	}

	pub(crate) fn set(&self, schedule: FlowSchedule) {
		*self.cache.write() = Some(schedule);
	}

	pub(crate) fn invalidate(&self) {
		*self.cache.write() = None;
	}
}
