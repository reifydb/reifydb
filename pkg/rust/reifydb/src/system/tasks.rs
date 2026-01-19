use reifydb_sub_task::task::ScheduledTask;

#[cfg(target_os = "linux")]
use super::memory::create_memory_watchdog_task;

pub fn create_system_tasks() -> Vec<ScheduledTask> {
	vec![
		#[cfg(target_os = "linux")]
		create_memory_watchdog_task(),
	]
}
