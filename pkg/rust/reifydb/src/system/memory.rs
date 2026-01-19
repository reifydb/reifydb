use std::{fs, process::exit, time::Duration};

use reifydb_sub_task::{
	context::TaskContext,
	schedule::Schedule,
	task::{ScheduledTask, TaskExecutor},
};
use tracing::{debug, error};

const MEMORY_KILL_THRESHOLD_PERCENT: f32 = 90.0;
const MEMORY_CHECK_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct MemoryWatchdog {
	kill_threshold_percent: f32,
}

/// Current memory statistics.
#[derive(Debug, Clone)]
pub struct MemoryStats {
	pub current_bytes: u64,
	pub total_bytes: u64,
	pub percent_used: f32,
}

impl MemoryWatchdog {
	pub fn new(kill_threshold_percent: f32) -> Self {
		Self {
			kill_threshold_percent,
		}
	}

	#[cfg(target_os = "linux")]
	pub fn get_current_memory() -> Result<u64, String> {
		// Read /proc/self/stat for the current process
		let stat = fs::read_to_string("/proc/self/stat")
			.map_err(|e| format!("Failed to read /proc/self/stat: {}", e))?;

		// Find the last ')' to skip the comm field which can contain spaces
		let comm_end = stat.rfind(')').ok_or("Invalid /proc/self/stat format: no closing parenthesis")?;

		// Split the rest of the fields after the comm field
		let fields_after_comm: Vec<&str> = stat[comm_end + 1..].split_whitespace().collect();

		let rss_index_after_comm = 21;
		if fields_after_comm.len() <= rss_index_after_comm {
			return Err(format!(
				"Invalid /proc/self/stat format: expected at least {} fields, got {}",
				rss_index_after_comm + 1,
				fields_after_comm.len()
			));
		}

		let rss_pages: u64 = fields_after_comm[rss_index_after_comm]
			.parse()
			.map_err(|e| format!("Failed to parse RSS: {}", e))?;

		// Get page size (typically 4096 bytes)
		let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
		let rss_bytes = rss_pages * page_size;

		Ok(rss_bytes)
	}

	/// Get current process memory usage (RSS) in bytes.
	///
	/// This is not supported on non-Linux platforms.
	#[cfg(not(target_os = "linux"))]
	pub fn get_current_memory() -> Result<u64, String> {
		Err("Memory monitoring is only supported on Linux".to_string())
	}

	/// Get maximum available system memory in bytes.
	///
	/// This is Linux-only and reads from `/proc/meminfo`.
	#[cfg(target_os = "linux")]
	pub fn get_max_available_memory() -> Result<u64, String> {
		// Read /proc/meminfo for MemTotal
		let meminfo = fs::read_to_string("/proc/meminfo")
			.map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

		for line in meminfo.lines() {
			if line.starts_with("MemTotal:") {
				let parts: Vec<&str> = line.split_whitespace().collect();
				if parts.len() >= 2 {
					let kb: u64 = parts[1]
						.parse()
						.map_err(|e| format!("Failed to parse MemTotal: {}", e))?;
					return Ok(kb * 1024); // Convert KB to bytes
				}
			}
		}

		Err("MemTotal not found in /proc/meminfo".to_string())
	}

	/// Get maximum available system memory in bytes.
	///
	/// This is not supported on non-Linux platforms.
	#[cfg(not(target_os = "linux"))]
	pub fn get_max_available_memory() -> Result<u64, String> {
		Err("Memory monitoring is only supported on Linux".to_string())
	}

	fn check_and_kill_if_exceeded(&self, stats: &MemoryStats) {
		if stats.percent_used >= self.kill_threshold_percent {
			let msg = format!(
				"Memory usage exceeded kill threshold: {:.2}% >= {:.2}% ({} MB / {} MB). Exiting process.",
				stats.percent_used,
				self.kill_threshold_percent,
				stats.current_bytes / 1024 / 1024,
				stats.total_bytes / 1024 / 1024
			);

			error!("{}", msg);
			eprintln!("{}", msg);
			exit(1);
		}
	}
}

#[cfg(target_os = "linux")]
pub fn create_memory_watchdog_task() -> ScheduledTask {
	ScheduledTask::builder("memory-watchdog")
		.schedule(Schedule::FixedInterval(MEMORY_CHECK_INTERVAL))
		.work_sync(move |_ctx: TaskContext| {
			let monitor = MemoryWatchdog::new(MEMORY_KILL_THRESHOLD_PERCENT);

			let current = MemoryWatchdog::get_current_memory().unwrap(); // FIXME
			let total = MemoryWatchdog::get_max_available_memory().unwrap(); // FIXME
			let percent_used = ((current as f64 / total as f64) * 100.0) as f32;

			let stats = MemoryStats {
				current_bytes: current,
				total_bytes: total,
				percent_used,
			};

			debug!(
				"Memory usage: {:.2}% ({} MB / {} MB)",
				percent_used,
				current / 1024 / 1024,
				total / 1024 / 1024
			);

			// Check threshold and kill if exceeded
			monitor.check_and_kill_if_exceeded(&stats);

			Ok(())
		})
		.executor(TaskExecutor::ComputePool)
		.build()
		.expect("Failed to crete memory-watchdog task")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	#[cfg(target_os = "linux")]
	fn test_get_current_memory() {
		let current = MemoryWatchdog::get_current_memory();
		assert!(current.is_ok(), "Failed to get current memory: {:?}", current);
		assert!(current.unwrap() > 0, "Current memory should be > 0");
	}

	#[test]
	#[cfg(target_os = "linux")]
	fn test_get_max_available_memory() {
		let total = MemoryWatchdog::get_max_available_memory();
		assert!(total.is_ok(), "Failed to get max memory: {:?}", total);
		assert!(total.unwrap() > 0, "Total memory should be > 0");
	}

	#[test]
	fn test_memory_stats_calculation() {
		let stats = MemoryStats {
			current_bytes: 500 * 1024 * 1024, // 500 MB
			total_bytes: 1000 * 1024 * 1024,  // 1000 MB (1 GB)
			percent_used: 50.0,
		};

		assert_eq!(stats.percent_used, 50.0);
	}

	#[test]
	fn test_kill_threshold_not_exceeded() {
		let monitor = MemoryWatchdog::new(50.0);
		let stats = MemoryStats {
			current_bytes: 400 * 1024 * 1024, // 400 MB
			total_bytes: 1000 * 1024 * 1024,  // 1000 MB
			percent_used: 40.0,
		};

		// This should not kill
		monitor.check_and_kill_if_exceeded(&stats);
		// If we get here, the test passed
	}
}
