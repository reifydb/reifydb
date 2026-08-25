// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use reifydb_auth::service::AuthService;
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::{
	context::TaskContext,
	schedule::Schedule,
	task::{ScheduledTask, TaskExecutor},
};
#[cfg(not(reifydb_single_threaded))]
use reifydb_value::value::duration::Duration;

#[cfg(not(reifydb_single_threaded))]
pub fn create_auth_cleanup_task(auth: AuthService) -> ScheduledTask {
	ScheduledTask::builder("auth-cleanup")
		.schedule(Schedule::FixedInterval(Duration::from_seconds(300).unwrap()))
		.work_sync(move |_ctx: TaskContext| {
			auth.cleanup_expired();
			Ok(())
		})
		.executor(TaskExecutor::ComputePool)
		.build()
		.expect("Failed to create auth-cleanup task")
}
