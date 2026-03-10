use reifydb_sub_task::task::ScheduledTask;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::memory::create_memory_watchdog_task;

pub fn create_system_tasks() -> Vec<ScheduledTask> {
	vec![
		#[cfg(any(target_os = "linux", target_os = "macos"))]
		create_memory_watchdog_task(),
	]
}
