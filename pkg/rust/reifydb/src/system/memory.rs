#[cfg(target_os = "macos")]
use std::ptr;
#[cfg(target_os = "linux")]
use std::{fs, io};
use std::{mem, process::exit, time::Duration};

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

	/// Get current process memory usage (RSS) in bytes on Linux.
	///
	/// Reads RSS (field 1) from `/proc/self/statm` and converts pages to bytes.
	#[cfg(target_os = "linux")]
	pub fn get_current_memory() -> Result<u64, String> {
		let statm = fs::read_to_string("/proc/self/statm")
			.map_err(|e| format!("Failed to read /proc/self/statm: {}", e))?;
		let rss_pages: u64 = statm
			.split_whitespace()
			.nth(1)
			.ok_or("Invalid /proc/self/statm format")?
			.parse()
			.map_err(|e| format!("Failed to parse RSS: {}", e))?;
		let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
		Ok(rss_pages * page_size)
	}

	/// Get current process memory usage (RSS) in bytes on macOS.
	///
	/// Uses `task_info()` with `MACH_TASK_BASIC_INFO` to get `resident_size`.
	#[cfg(target_os = "macos")]
	pub fn get_current_memory() -> Result<u64, String> {
		unsafe {
			let mut info: libc::mach_task_basic_info = mem::zeroed();
			let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
			#[allow(deprecated)]
			let task = libc::mach_task_self();
			let kr = libc::task_info(
				task,
				libc::MACH_TASK_BASIC_INFO,
				&mut info as *mut _ as *mut i32,
				&mut count,
			);
			if kr != libc::KERN_SUCCESS {
				return Err(format!("task_info failed with kern_return {}", kr));
			}
			Ok(info.resident_size)
		}
	}

	/// Get current process memory usage (RSS) in bytes.
	///
	/// This is not supported on non-Linux/macOS platforms.
	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	pub fn get_current_memory() -> Result<u64, String> {
		panic!("Memory monitoring is only supported on Linux and macOS".to_string())
	}

	/// Get maximum available system memory in bytes on Linux.
	///
	/// Uses `libc::sysinfo()` to get total physical memory.
	#[cfg(target_os = "linux")]
	pub fn get_max_available_memory() -> Result<u64, String> {
		unsafe {
			let mut info: libc::sysinfo = mem::zeroed();
			let ret = libc::sysinfo(&mut info);
			if ret != 0 {
				return Err(format!("sysinfo() failed: {}", io::Error::last_os_error()));
			}
			Ok(info.totalram as u64 * info.mem_unit as u64)
		}
	}

	/// Get maximum available system memory in bytes on macOS.
	///
	/// Uses `sysctl()` with `CTL_HW` + `HW_MEMSIZE` to get total physical memory.
	#[cfg(target_os = "macos")]
	pub fn get_max_available_memory() -> Result<u64, String> {
		unsafe {
			let mut mib = [libc::CTL_HW, libc::HW_MEMSIZE];
			let mut memsize: u64 = 0;
			let mut len = mem::size_of::<u64>();
			let ret = libc::sysctl(
				mib.as_mut_ptr(),
				2,
				&mut memsize as *mut _ as *mut libc::c_void,
				&mut len,
				ptr::null_mut(),
				0,
			);
			if ret != 0 {
				return Err(format!("sysctl HW_MEMSIZE failed with errno {}", *libc::__error()));
			}
			Ok(memsize)
		}
	}

	/// Get maximum available system memory in bytes.
	///
	/// This is not supported on non-Linux/macOS platforms.
	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	pub fn get_max_available_memory() -> Result<u64, String> {
		panic!("Memory monitoring is only supported on Linux and macOS".to_string())
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

#[cfg(any(target_os = "linux", target_os = "macos"))]
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
	#[cfg(any(target_os = "linux", target_os = "macos"))]
	fn test_get_current_memory() {
		let current = MemoryWatchdog::get_current_memory();
		assert!(current.is_ok(), "Failed to get current memory: {:?}", current);
		assert!(current.unwrap() > 0, "Current memory should be > 0");
	}

	#[test]
	#[cfg(any(target_os = "linux", target_os = "macos"))]
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
