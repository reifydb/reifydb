// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Default, Debug, PartialEq)]
pub(crate) struct ProcessIo {
	pub rchar: u64,
	pub wchar: u64,
	pub read_bytes: u64,
	pub write_bytes: u64,
	pub cancelled_write_bytes: u64,
	pub read_syscalls: u64,
	pub write_syscalls: u64,
}

#[derive(Default, Debug, PartialEq)]
pub struct ProcessStatus {
	pub rss_total: u64,
	pub rss_anon: u64,
	pub rss_file: u64,
	pub rss_shmem: u64,
	pub vm_size: u64,
	pub vm_data: u64,
	pub vm_swap: u64,
	pub vm_high_water_mark: u64,
	pub threads: u64,
	pub voluntary_context_switches: u64,
	pub involuntary_context_switches: u64,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct SmapsRollup {
	pub pss: Option<u64>,
	pub private_dirty: Option<u64>,
	pub private_clean: Option<u64>,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct ProcessStat {
	pub minor_faults: u64,
	pub major_faults: u64,
	pub user_ticks: u64,
	pub system_ticks: u64,
}

#[derive(Debug, PartialEq)]
pub(crate) struct CgroupIoDevice {
	pub device: String,
	pub read_bytes: u64,
	pub write_bytes: u64,
	pub read_ios: u64,
	pub write_ios: u64,
	pub discard_bytes: u64,
	pub discard_ios: u64,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct CgroupMemory {
	pub current: u64,
	pub max: Option<u64>,
	pub anon: u64,
	pub file: u64,
	pub file_dirty: u64,
	pub file_writeback: u64,
	pub slab: u64,
	pub sock: u64,
	pub swap_current: u64,
	pub swap_max: Option<u64>,
	pub page_faults: u64,
	pub major_page_faults: u64,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct CgroupCpu {
	pub usage_micros: u64,
	pub user_micros: u64,
	pub system_micros: u64,
	pub periods: Option<u64>,
	pub throttled_periods: Option<u64>,
	pub throttled_micros: Option<u64>,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct PressureLine {
	pub stalled_micros: u64,
	pub avg10: f64,
	pub avg60: f64,
	pub avg300: f64,
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct Pressure {
	pub some: PressureLine,
	pub full: Option<PressureLine>,
}

pub(crate) fn parse_process_io(content: &str) -> ProcessIo {
	ProcessIo {
		rchar: keyed(content, "rchar:").unwrap_or(0),
		wchar: keyed(content, "wchar:").unwrap_or(0),
		read_bytes: keyed(content, "read_bytes:").unwrap_or(0),
		write_bytes: keyed(content, "write_bytes:").unwrap_or(0),
		cancelled_write_bytes: keyed(content, "cancelled_write_bytes:").unwrap_or(0),
		read_syscalls: keyed(content, "syscr:").unwrap_or(0),
		write_syscalls: keyed(content, "syscw:").unwrap_or(0),
	}
}

pub(crate) fn parse_process_status(content: &str) -> ProcessStatus {
	ProcessStatus {
		rss_total: keyed_kb(content, "VmRSS:").unwrap_or(0),
		rss_anon: keyed_kb(content, "RssAnon:").unwrap_or(0),
		rss_file: keyed_kb(content, "RssFile:").unwrap_or(0),
		rss_shmem: keyed_kb(content, "RssShmem:").unwrap_or(0),
		vm_size: keyed_kb(content, "VmSize:").unwrap_or(0),
		vm_data: keyed_kb(content, "VmData:").unwrap_or(0),
		vm_swap: keyed_kb(content, "VmSwap:").unwrap_or(0),
		vm_high_water_mark: keyed_kb(content, "VmHWM:").unwrap_or(0),
		threads: keyed(content, "Threads:").unwrap_or(0),
		voluntary_context_switches: keyed(content, "voluntary_ctxt_switches:").unwrap_or(0),
		involuntary_context_switches: keyed(content, "nonvoluntary_ctxt_switches:").unwrap_or(0),
	}
}

pub(crate) fn parse_smaps_rollup(content: &str) -> SmapsRollup {
	SmapsRollup {
		pss: keyed_kb(content, "Pss:"),
		private_dirty: keyed_kb(content, "Private_Dirty:"),
		private_clean: keyed_kb(content, "Private_Clean:"),
	}
}

pub(crate) fn parse_process_stat(content: &str) -> Option<ProcessStat> {
	let tail = content.rsplit_once(')').map(|(_, tail)| tail)?;
	let fields: Vec<&str> = tail.split_whitespace().collect();
	Some(ProcessStat {
		minor_faults: field(&fields, 7)?,
		major_faults: field(&fields, 9)?,
		user_ticks: field(&fields, 11)?,
		system_ticks: field(&fields, 12)?,
	})
}

pub(crate) fn parse_schedstat_run_queue_nanos(content: &str) -> Option<u64> {
	content.split_whitespace().nth(1)?.parse().ok()
}

pub(crate) fn parse_max_open_files(content: &str) -> Option<u64> {
	let rest = content.lines().find_map(|line| line.strip_prefix("Max open files"))?;
	rest.split_whitespace().next()?.parse().ok()
}

pub(crate) fn parse_cgroup_relative_path(content: &str) -> Option<&str> {
	content.lines().find_map(|line| line.strip_prefix("0::")).map(str::trim)
}

pub(crate) fn parse_cgroup_io(content: &str) -> Vec<CgroupIoDevice> {
	content.lines()
		.filter_map(|line| {
			let mut parts = line.split_whitespace();
			let device = parts.next()?;
			if !device.contains(':') {
				return None;
			}
			let mut entry = CgroupIoDevice {
				device: device.to_string(),
				read_bytes: 0,
				write_bytes: 0,
				read_ios: 0,
				write_ios: 0,
				discard_bytes: 0,
				discard_ios: 0,
			};
			for part in parts {
				let Some((key, value)) = part.split_once('=') else {
					continue;
				};
				let Ok(value) = value.parse::<u64>() else {
					continue;
				};
				match key {
					"rbytes" => entry.read_bytes = value,
					"wbytes" => entry.write_bytes = value,
					"rios" => entry.read_ios = value,
					"wios" => entry.write_ios = value,
					"dbytes" => entry.discard_bytes = value,
					"dios" => entry.discard_ios = value,
					_ => {}
				}
			}
			Some(entry)
		})
		.collect()
}

pub(crate) fn parse_cgroup_memory(
	stat: &str,
	current: &str,
	max: &str,
	swap_current: &str,
	swap_max: &str,
) -> CgroupMemory {
	CgroupMemory {
		current: parse_scalar(current).unwrap_or(0),
		max: parse_limit(max),
		anon: spaced(stat, "anon").unwrap_or(0),
		file: spaced(stat, "file").unwrap_or(0),
		file_dirty: spaced(stat, "file_dirty").unwrap_or(0),
		file_writeback: spaced(stat, "file_writeback").unwrap_or(0),
		slab: spaced(stat, "slab").unwrap_or(0),
		sock: spaced(stat, "sock").unwrap_or(0),
		swap_current: parse_scalar(swap_current).unwrap_or(0),
		swap_max: parse_limit(swap_max),
		page_faults: spaced(stat, "pgfault").unwrap_or(0),
		major_page_faults: spaced(stat, "pgmajfault").unwrap_or(0),
	}
}

pub(crate) fn parse_cgroup_cpu(content: &str) -> CgroupCpu {
	CgroupCpu {
		usage_micros: spaced(content, "usage_usec").unwrap_or(0),
		user_micros: spaced(content, "user_usec").unwrap_or(0),
		system_micros: spaced(content, "system_usec").unwrap_or(0),
		periods: spaced(content, "nr_periods"),
		throttled_periods: spaced(content, "nr_throttled"),
		throttled_micros: spaced(content, "throttled_usec"),
	}
}

pub(crate) fn parse_pressure(content: &str) -> Option<Pressure> {
	let some = content.lines().find_map(|line| pressure_line(line, "some"))?;
	Some(Pressure {
		some,
		full: content.lines().find_map(|line| pressure_line(line, "full")),
	})
}

fn pressure_line(line: &str, prefix: &str) -> Option<PressureLine> {
	let rest = line.strip_prefix(prefix)?;
	if !rest.starts_with(' ') {
		return None;
	}
	let mut out = PressureLine::default();
	for part in rest.split_whitespace() {
		let Some((key, value)) = part.split_once('=') else {
			continue;
		};
		match key {
			"total" => out.stalled_micros = value.parse().unwrap_or(0),
			"avg10" => out.avg10 = value.parse().unwrap_or(0.0),
			"avg60" => out.avg60 = value.parse().unwrap_or(0.0),
			"avg300" => out.avg300 = value.parse().unwrap_or(0.0),
			_ => {}
		}
	}
	Some(out)
}

fn parse_scalar(content: &str) -> Option<u64> {
	content.trim().parse().ok()
}

fn parse_limit(content: &str) -> Option<u64> {
	let trimmed = content.trim();
	if trimmed == "max" {
		return None;
	}
	trimmed.parse().ok()
}

fn field(fields: &[&str], index: usize) -> Option<u64> {
	fields.get(index)?.parse().ok()
}

fn keyed(content: &str, key: &str) -> Option<u64> {
	content.lines().find_map(|line| {
		let rest = line.strip_prefix(key)?;
		rest.split_whitespace().next()?.parse().ok()
	})
}

fn keyed_kb(content: &str, key: &str) -> Option<u64> {
	keyed(content, key).map(|kb| kb * 1024)
}

fn spaced(content: &str, key: &str) -> Option<u64> {
	content.lines().find_map(|line| {
		let mut parts = line.split_whitespace();
		if parts.next()? != key {
			return None;
		}
		parts.next()?.parse().ok()
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn process_io_reads_every_field_including_cancelled_writes() {
		// Without cancelled_write_bytes nothing reports dirtied pages that never reached the device.
		let content = "rchar: 16878\nwchar: 42\nsyscr: 22\nsyscw: 3\nread_bytes: 4096\nwrite_bytes: 8192\ncancelled_write_bytes: 512\n";
		assert_eq!(
			parse_process_io(content),
			ProcessIo {
				rchar: 16878,
				wchar: 42,
				read_bytes: 4096,
				write_bytes: 8192,
				cancelled_write_bytes: 512,
				read_syscalls: 22,
				write_syscalls: 3,
			}
		);
	}

	#[test]
	fn a_missing_io_field_reads_zero_rather_than_dropping_the_row() {
		// Older kernels omit cancelled_write_bytes, and dropping the sample would blind every other counter.
		let io = parse_process_io("rchar: 5\nwchar: 7\n");
		assert_eq!(io.rchar, 5);
		assert_eq!(io.cancelled_write_bytes, 0);
	}

	#[test]
	fn status_fields_convert_from_kilobytes_to_bytes() {
		// /proc reports kB and every other byte metric is bytes, so a missed conversion understates by 1024x.
		let content = "VmSize:\t    9464 kB\nVmHWM:\t    2604 kB\nVmRSS:\t    2604 kB\nRssAnon:\t     168 kB\nRssFile:\t    2436 kB\nRssShmem:\t       0 kB\nVmData:\t     100 kB\nVmSwap:\t       8 kB\nThreads:\t7\nvoluntary_ctxt_switches:\t11\nnonvoluntary_ctxt_switches:\t2\n";
		let status = parse_process_status(content);
		assert_eq!(status.rss_total, 2604 * 1024);
		assert_eq!(status.vm_swap, 8 * 1024);
		assert_eq!(status.vm_high_water_mark, 2604 * 1024);
		assert_eq!(status.threads, 7);
		assert_eq!(status.voluntary_context_switches, 11);
		assert_eq!(status.involuntary_context_switches, 2);
	}

	#[test]
	fn smaps_rollup_absence_is_none_not_zero() {
		// A kernel without smaps_rollup must publish none, never a zero that reads as a measurement.
		assert_eq!(parse_smaps_rollup(""), SmapsRollup {
			pss: None,
			private_dirty: None,
			private_clean: None
		});
		let rollup = parse_smaps_rollup("Pss:  100 kB\nPrivate_Dirty:  40 kB\nPrivate_Clean:  8 kB\n");
		assert_eq!(rollup.pss, Some(100 * 1024));
	}

	#[test]
	fn stat_fields_are_indexed_after_a_comm_containing_spaces_and_parens() {
		// comm may itself hold a paren, and splitting on the first one shifts every field silently.
		let content = "152573 (weird ) name) R 152550 152573 152550 0 -1 4194304 470 0 13 0 21 34 0 0 20 0 1 0 1674218";
		assert_eq!(
			parse_process_stat(content),
			Some(ProcessStat {
				minor_faults: 470,
				major_faults: 13,
				user_ticks: 21,
				system_ticks: 34,
			})
		);
	}

	#[test]
	fn schedstat_takes_the_run_queue_wait_not_the_cpu_time() {
		// Field one is time on cpu and field two is time waiting, so swapping them inverts the signal.
		assert_eq!(parse_schedstat_run_queue_nanos("128831 4242 1"), Some(4242));
	}

	#[test]
	fn max_open_files_takes_the_soft_limit() {
		// The soft limit is what a request hits, so reporting the hard limit hides live exhaustion.
		let content = "Limit                     Soft Limit           Hard Limit           Units\nMax open files            1024                 524288               files\n";
		assert_eq!(parse_max_open_files(content), Some(1024));
	}

	#[test]
	fn cgroup_path_comes_from_the_v2_line() {
		// A hybrid host lists v1 controllers first, and taking the first line finds none of the v2 files.
		let content = "3:cpu:/legacy\n0::/user.slice/app.scope\n";
		assert_eq!(parse_cgroup_relative_path(content), Some("/user.slice/app.scope"));
	}

	#[test]
	fn cgroup_io_keeps_one_row_per_device() {
		// Summing devices merges the database disk with the log disk and makes amplification unattributable.
		let content = "259:0 rbytes=100 wbytes=200 rios=3 wios=4 dbytes=5 dios=6\n259:1 rbytes=7 wbytes=8 rios=9 wios=10 dbytes=11 dios=12\n";
		let devices = parse_cgroup_io(content);
		assert_eq!(devices.len(), 2);
		assert_eq!(devices[0].device, "259:0");
		assert_eq!(devices[0].write_bytes, 200);
		assert_eq!(devices[1].discard_ios, 12);
	}

	#[test]
	fn an_empty_cgroup_io_file_yields_no_devices() {
		// The io controller is often off on a leaf cgroup, and a zero row would claim nothing was written.
		assert!(parse_cgroup_io("").is_empty());
	}

	#[test]
	fn an_unlimited_memory_max_is_none_not_a_huge_number() {
		// memory.max reads "max" when unset, and parsing that as a number makes headroom meaningless.
		let stat = "anon 489242624\nfile 24347705344\nfile_dirty 1593344\nfile_writeback 0\nslab 12\nsock 0\npgfault 100\npgmajfault 5\n";
		let memory = parse_cgroup_memory(stat, "25983082496", "max", "0", "max");
		assert_eq!(memory.max, None);
		assert_eq!(memory.current, 25983082496);
		assert_eq!(memory.anon, 489242624);
		assert_eq!(memory.major_page_faults, 5);
	}

	#[test]
	fn cpu_throttling_fields_are_none_when_no_quota_is_set() {
		// Without cpu.max the kernel omits nr_throttled, and a zero would assert "not throttled".
		let cpu = parse_cgroup_cpu("usage_usec 1494667605\nuser_usec 1371935992\nsystem_usec 122731613\n");
		assert_eq!(cpu.usage_micros, 1494667605);
		assert_eq!(cpu.throttled_periods, None);

		let throttled = parse_cgroup_cpu("usage_usec 1\nuser_usec 1\nsystem_usec 0\nnr_periods 10\nnr_throttled 3\nthrottled_usec 900\n");
		assert_eq!(throttled.throttled_periods, Some(3));
		assert_eq!(throttled.throttled_micros, Some(900));
	}

	#[test]
	fn pressure_without_a_full_line_still_parses_some() {
		// cpu.pressure carries no full line on older kernels, and requiring both would drop the stall signal.
		let pressure = parse_pressure("some avg10=0.50 avg60=0.25 avg300=0.00 total=1393549\n").unwrap();
		assert_eq!(pressure.some.stalled_micros, 1393549);
		assert_eq!(pressure.some.avg10, 0.50);
		assert!(pressure.full.is_none());

		let both = parse_pressure(
			"some avg10=0.00 avg60=0.00 avg300=0.00 total=10\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=4\n",
		)
		.unwrap();
		assert_eq!(both.full.unwrap().stalled_micros, 4);
	}

	#[test]
	fn a_pressure_file_that_is_missing_its_some_line_is_no_reading_at_all() {
		// PSI can be compiled out, and a default row would report a permanently unstalled system.
		assert_eq!(parse_pressure(""), None);
	}
}
