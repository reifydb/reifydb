// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod read;

#[cfg(target_os = "linux")]
use std::{
	fs::{read_dir, read_to_string},
	path::{Path, PathBuf},
};

use reifydb_core::metrics::sample::{MetricKind, Reading};
#[cfg(target_os = "linux")]
use reifydb_value::value::Value;
use reifydb_value::{byte_size::ByteSize, count::Count, value::duration::Duration};

use crate::framework::accumulator::{Measure, MetricsRow};
#[cfg(target_os = "linux")]
use crate::domains::proc::read::{
	parse_cgroup_cpu, parse_cgroup_io, parse_cgroup_memory, parse_cgroup_relative_path, parse_max_open_files,
	parse_pressure, parse_process_io, parse_process_stat, parse_process_status, parse_schedstat_run_queue_nanos,
	parse_smaps_rollup,
};
pub use crate::domains::proc::read::ProcessStatus;

#[cfg(target_os = "linux")]
pub fn process_status() -> Option<ProcessStatus> {
	read_to_string("/proc/self/status").ok().map(|content| parse_process_status(&content))
}

#[cfg(not(target_os = "linux"))]
pub fn process_status() -> Option<ProcessStatus> {
	None
}

#[cfg(target_os = "linux")]
pub fn process_io_rows() -> Vec<MetricsRow> {
	let Ok(content) = read_to_string("/proc/self/io") else {
		return Vec::new();
	};
	let io = parse_process_io(&content);
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			counter_bytes("rchar", io.rchar),
			counter_bytes("wchar", io.wchar),
			counter_bytes("read_bytes", io.read_bytes),
			counter_bytes("write_bytes", io.write_bytes),
			counter_bytes("cancelled_write_bytes", io.cancelled_write_bytes),
			counter_count("read_syscalls", io.read_syscalls),
			counter_count("write_syscalls", io.write_syscalls),
		],
	}]
}

#[cfg(target_os = "linux")]
pub fn process_memory_rows() -> Vec<MetricsRow> {
	let Some(status) = process_status() else {
		return Vec::new();
	};
	let rollup = parse_smaps_rollup(&read_to_string("/proc/self/smaps_rollup").unwrap_or_default());
	let mut measures = vec![
		level_bytes("rss_total", status.rss_total),
		level_bytes("rss_anon", status.rss_anon),
		level_bytes("rss_file", status.rss_file),
		level_bytes("rss_shmem", status.rss_shmem),
		level_bytes("vm_size", status.vm_size),
		level_bytes("vm_data", status.vm_data),
		level_bytes("vm_swap", status.vm_swap),
		level_bytes("vm_high_water_mark", status.vm_high_water_mark),
		level_count("threads", status.threads),
	];
	if let Some(pss) = rollup.pss {
		measures.push(level_bytes("pss", pss));
	}
	if let Some(dirty) = rollup.private_dirty {
		measures.push(level_bytes("private_dirty", dirty));
	}
	if let Some(clean) = rollup.private_clean {
		measures.push(level_bytes("private_clean", clean));
	}
	if let (Some(dirty), Some(clean)) = (rollup.private_dirty, rollup.private_clean) {
		measures.push(level_bytes("uss", dirty + clean));
	}
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures,
	}]
}

#[cfg(target_os = "linux")]
pub fn process_sched_rows() -> Vec<MetricsRow> {
	let Some(status) = process_status() else {
		return Vec::new();
	};
	let Some(stat) = read_to_string("/proc/self/stat").ok().and_then(|content| parse_process_stat(&content))
	else {
		return Vec::new();
	};
	let hertz = clock_ticks_per_second();
	let mut measures = vec![
		counter_count("minor_faults", stat.minor_faults),
		counter_count("major_faults", stat.major_faults),
		counter_micros("user_time", ticks_to_micros(stat.user_ticks, hertz)),
		counter_micros("system_time", ticks_to_micros(stat.system_ticks, hertz)),
		counter_count("voluntary_context_switches", status.voluntary_context_switches),
		counter_count("involuntary_context_switches", status.involuntary_context_switches),
	];
	if let Some(nanos) =
		read_to_string("/proc/self/schedstat").ok().and_then(|content| parse_schedstat_run_queue_nanos(&content))
	{
		measures.push(counter_nanos("run_queue_wait", nanos));
	}
	if let Ok(entries) = read_dir("/proc/self/fd") {
		measures.push(level_count("open_files", entries.count() as u64));
	}
	if let Some(limit) = read_to_string("/proc/self/limits").ok().and_then(|c| parse_max_open_files(&c)) {
		measures.push(level_count("max_open_files", limit));
	}
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures,
	}]
}

#[cfg(target_os = "linux")]
pub fn cgroup_io_rows() -> Vec<MetricsRow> {
	let Some(root) = cgroup_root() else {
		return Vec::new();
	};
	let Ok(content) = read_to_string(root.join("io.stat")) else {
		return Vec::new();
	};
	parse_cgroup_io(&content)
		.into_iter()
		.map(|device| MetricsRow {
			dimensions: vec![Value::Utf8(device.device)],
			measures: vec![
				counter_bytes("read_bytes", device.read_bytes),
				counter_bytes("write_bytes", device.write_bytes),
				counter_count("read_ios", device.read_ios),
				counter_count("write_ios", device.write_ios),
				counter_bytes("discard_bytes", device.discard_bytes),
				counter_count("discard_ios", device.discard_ios),
			],
		})
		.collect()
}

#[cfg(target_os = "linux")]
pub fn cgroup_memory_rows() -> Vec<MetricsRow> {
	let Some(root) = cgroup_root() else {
		return Vec::new();
	};
	let Ok(current) = read_to_string(root.join("memory.current")) else {
		return Vec::new();
	};
	let memory = parse_cgroup_memory(
		&read_to_string(root.join("memory.stat")).unwrap_or_default(),
		&current,
		&read_to_string(root.join("memory.max")).unwrap_or_default(),
		&read_to_string(root.join("memory.swap.current")).unwrap_or_default(),
		&read_to_string(root.join("memory.swap.max")).unwrap_or_default(),
	);
	let mut measures = vec![
		level_bytes("current", memory.current),
		level_bytes("anon", memory.anon),
		level_bytes("file", memory.file),
		level_bytes("file_dirty", memory.file_dirty),
		level_bytes("file_writeback", memory.file_writeback),
		level_bytes("slab", memory.slab),
		level_bytes("sock", memory.sock),
		level_bytes("swap_current", memory.swap_current),
		counter_count("page_faults", memory.page_faults),
		counter_count("major_page_faults", memory.major_page_faults),
	];
	if let Some(max) = memory.max {
		measures.push(level_bytes("max", max));
	}
	if let Some(swap_max) = memory.swap_max {
		measures.push(level_bytes("swap_max", swap_max));
	}
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures,
	}]
}

#[cfg(target_os = "linux")]
pub fn cgroup_cpu_rows() -> Vec<MetricsRow> {
	let Some(root) = cgroup_root() else {
		return Vec::new();
	};
	let Ok(content) = read_to_string(root.join("cpu.stat")) else {
		return Vec::new();
	};
	let cpu = parse_cgroup_cpu(&content);
	let mut measures = vec![
		counter_micros("usage", cpu.usage_micros),
		counter_micros("user", cpu.user_micros),
		counter_micros("system", cpu.system_micros),
	];
	if let Some(periods) = cpu.periods {
		measures.push(counter_count("periods", periods));
	}
	if let Some(throttled) = cpu.throttled_periods {
		measures.push(counter_count("throttled_periods", throttled));
	}
	if let Some(micros) = cpu.throttled_micros {
		measures.push(counter_micros("throttled", micros));
	}
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures,
	}]
}

#[cfg(target_os = "linux")]
pub fn cgroup_pressure_rows() -> Vec<MetricsRow> {
	let Some(root) = cgroup_root() else {
		return Vec::new();
	};
	[("cpu", "cpu.pressure"), ("io", "io.pressure"), ("memory", "memory.pressure")]
		.into_iter()
		.filter_map(|(resource, file)| {
			let pressure = read_to_string(root.join(file)).ok().and_then(|c| parse_pressure(&c))?;
			let mut measures = vec![
				counter_micros("some_stalled", pressure.some.stalled_micros),
				level_ratio("some_avg10", pressure.some.avg10),
				level_ratio("some_avg60", pressure.some.avg60),
				level_ratio("some_avg300", pressure.some.avg300),
			];
			if let Some(full) = pressure.full {
				measures.push(counter_micros("full_stalled", full.stalled_micros));
				measures.push(level_ratio("full_avg10", full.avg10));
				measures.push(level_ratio("full_avg60", full.avg60));
				measures.push(level_ratio("full_avg300", full.avg300));
			}
			Some(MetricsRow {
				dimensions: vec![Value::Utf8(resource.to_string())],
				measures,
			})
		})
		.collect()
}

#[cfg(target_os = "linux")]
fn cgroup_root() -> Option<PathBuf> {
	let mount = Path::new("/sys/fs/cgroup");
	let relative = read_to_string("/proc/self/cgroup")
		.ok()
		.and_then(|content| parse_cgroup_relative_path(&content).map(str::to_string));
	if let Some(relative) = relative {
		let scoped = mount.join(relative.trim_start_matches('/'));
		if scoped.join("cgroup.controllers").is_file() {
			return Some(scoped);
		}
	}
	mount.join("cgroup.controllers").is_file().then(|| mount.to_path_buf())
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_second() -> u64 {
	// SAFETY: sysconf reads a static configuration value from a name constant and writes through no pointer.
	let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
	if ticks > 0 {
		ticks as u64
	} else {
		100
	}
}

#[cfg(target_os = "linux")]
fn ticks_to_micros(ticks: u64, hertz: u64) -> u64 {
	ticks.saturating_mul(1_000_000) / hertz.max(1)
}

#[cfg(not(target_os = "linux"))]
pub fn process_io_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn process_memory_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn process_sched_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn cgroup_io_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn cgroup_memory_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn cgroup_cpu_rows() -> Vec<MetricsRow> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
pub fn cgroup_pressure_rows() -> Vec<MetricsRow> {
	Vec::new()
}

fn counter_bytes(metric: &'static str, bytes: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(ByteSize::from_bytes(bytes)),
		kind: MetricKind::Counter,
	}
}

fn counter_count(metric: &'static str, value: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Count(Count::new(value)),
		kind: MetricKind::Counter,
	}
}

fn counter_micros(metric: &'static str, micros: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Duration(Duration::from_micros_infallible(micros)),
		kind: MetricKind::Counter,
	}
}

#[cfg(target_os = "linux")]
fn counter_nanos(metric: &'static str, nanos: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Duration(Duration::from_nanos_infallible(nanos)),
		kind: MetricKind::Counter,
	}
}

fn level_bytes(metric: &'static str, bytes: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(ByteSize::from_bytes(bytes)),
		kind: MetricKind::Level,
	}
}

fn level_count(metric: &'static str, value: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Count(Count::new(value)),
		kind: MetricKind::Level,
	}
}

fn level_ratio(metric: &'static str, value: f64) -> Measure {
	Measure {
		metric,
		reading: Reading::Ratio(value),
		kind: MetricKind::Level,
	}
}
