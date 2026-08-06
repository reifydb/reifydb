// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;
#[cfg(any(test, all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos"))))]
use std::process::exit;
#[cfg(target_os = "macos")]
use std::ptr;
#[cfg(target_os = "linux")]
use std::{fs, io};

#[cfg(target_os = "macos")]
#[allow(deprecated)]
use libc::{
	__error, CTL_HW, HW_MEMSIZE, KERN_SUCCESS, MACH_TASK_BASIC_INFO, MACH_TASK_BASIC_INFO_COUNT, c_void,
	mach_task_basic_info, mach_task_self, sysctl, task_info,
};
#[cfg(target_os = "linux")]
use libc::{_SC_PAGESIZE, sysconf, sysinfo};
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::{
	context::TaskContext,
	schedule::Schedule,
	task::{ScheduledTask, TaskExecutor},
};
#[cfg(all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos")))]
use reifydb_value::value::duration::Duration;
#[cfg(any(test, all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos"))))]
use tracing::error;
#[cfg(all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos")))]
use tracing::trace;

#[cfg(not(reifydb_single_threaded))]
const MEMORY_KILL_THRESHOLD_PERCENT: f32 = 90.0;

#[derive(Debug, Clone)]
pub struct MemoryWatchdog {
	#[cfg(any(test, all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos"))))]
	kill_threshold_percent: f32,
}

#[derive(Debug, Clone)]
pub struct MemoryStats {
	pub current_bytes: u64,
	pub total_bytes: u64,
	pub percent_used: f32,
}

impl MemoryWatchdog {
	#[cfg(any(test, all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos"))))]
	pub fn new(kill_threshold_percent: f32) -> Self {
		Self {
			kill_threshold_percent,
		}
	}

	/// Field 1 of `/proc/self/statm` is RSS, counted in pages.
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
		// SAFETY: `sysconf` takes an integer name and touches no caller memory.
		let page_size = unsafe { sysconf(_SC_PAGESIZE) } as u64;
		Ok(rss_pages * page_size)
	}

	#[cfg(target_os = "macos")]
	pub fn get_current_memory() -> Result<u64, String> {
		// SAFETY: `info` is a zeroed `mach_task_basic_info` and `count` is its matching element
		// count, so `task_info` writes only within the allocation it is handed.
		unsafe {
			let mut info: mach_task_basic_info = mem::zeroed();
			let mut count = MACH_TASK_BASIC_INFO_COUNT;
			#[allow(deprecated)]
			let task = mach_task_self();
			let kr = task_info(task, MACH_TASK_BASIC_INFO, &mut info as *mut _ as *mut i32, &mut count);
			if kr != KERN_SUCCESS {
				return Err(format!("task_info failed with kern_return {}", kr));
			}
			Ok(info.resident_size)
		}
	}

	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	pub fn get_current_memory() -> Result<u64, String> {
		panic!("Memory monitoring is only supported on Linux and macOS".to_string())
	}

	#[cfg(target_os = "linux")]
	pub fn get_max_available_memory() -> Result<u64, String> {
		// SAFETY: `info` is a zeroed, correctly sized `sysinfo` struct owned by this frame, and
		// is the only memory `sysinfo` writes to.
		unsafe {
			let mut info: sysinfo = mem::zeroed();
			let ret = sysinfo(&mut info);
			if ret != 0 {
				return Err(format!("sysinfo() failed: {}", io::Error::last_os_error()));
			}
			Ok(info.totalram as u64 * info.mem_unit as u64)
		}
	}

	#[cfg(target_os = "macos")]
	pub fn get_max_available_memory() -> Result<u64, String> {
		// SAFETY: `mib` holds exactly the 2 elements declared, and `len` is the true size of the
		// `u64` destination, so `sysctl` cannot write past either buffer.
		unsafe {
			let mut mib = [CTL_HW, HW_MEMSIZE];
			let mut memsize: u64 = 0;
			let mut len = mem::size_of::<u64>();
			let ret = sysctl(
				mib.as_mut_ptr(),
				2,
				&mut memsize as *mut _ as *mut c_void,
				&mut len,
				ptr::null_mut(),
				0,
			);
			if ret != 0 {
				return Err(format!("sysctl HW_MEMSIZE failed with errno {}", *__error()));
			}
			Ok(memsize)
		}
	}

	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	pub fn get_max_available_memory() -> Result<u64, String> {
		panic!("Memory monitoring is only supported on Linux and macOS".to_string())
	}

	#[cfg(any(test, all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos"))))]
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

#[cfg(all(not(reifydb_single_threaded), any(target_os = "linux", target_os = "macos")))]
pub fn create_memory_watchdog_task() -> ScheduledTask {
	ScheduledTask::builder("memory-watchdog")
		.schedule(Schedule::FixedInterval(Duration::from_seconds(30).unwrap()))
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

			trace!(
				"Memory usage: {:.2}% ({} MB / {} MB)",
				percent_used,
				current / 1024 / 1024,
				total / 1024 / 1024
			);

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
			current_bytes: 500 * 1024 * 1024,
			total_bytes: 1000 * 1024 * 1024,
			percent_used: 50.0,
		};

		assert_eq!(stats.percent_used, 50.0);
	}

	#[test]
	fn test_kill_threshold_not_exceeded() {
		// Usage below the threshold must not reach exit(1); returning is the assertion.
		let monitor = MemoryWatchdog::new(50.0);
		let stats = MemoryStats {
			current_bytes: 400 * 1024 * 1024,
			total_bytes: 1000 * 1024 * 1024,
			percent_used: 40.0,
		};

		monitor.check_and_kill_if_exceeded(&stats);
	}
}
