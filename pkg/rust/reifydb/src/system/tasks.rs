// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use reifydb_auth::service::AuthService;
#[cfg(not(reifydb_single_threaded))]
use reifydb_core::util::ioc::IocContainer;
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::task::ScheduledTask;

#[cfg(not(reifydb_single_threaded))]
use super::auth::create_auth_cleanup_task;
#[cfg(all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos")))]
use super::memory::create_memory_watchdog_task;
#[cfg(not(reifydb_single_threaded))]
use crate::Result;

#[cfg(not(reifydb_single_threaded))]
pub fn create_system_tasks(ioc: &IocContainer) -> Result<Vec<ScheduledTask>> {
	Ok(vec![
		#[cfg(any(target_os = "linux", target_os = "macos"))]
		create_memory_watchdog_task(),
		create_auth_cleanup_task(ioc.resolve::<AuthService>()?),
	])
}
