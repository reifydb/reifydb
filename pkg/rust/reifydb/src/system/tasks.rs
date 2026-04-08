// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::task::ScheduledTask;

#[cfg(all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos")))]
use super::memory::create_memory_watchdog_task;

#[cfg(not(reifydb_single_threaded))]
pub fn create_system_tasks() -> Vec<ScheduledTask> {
	vec![
		#[cfg(any(target_os = "linux", target_os = "macos"))]
		create_memory_watchdog_task(),
	]
}
