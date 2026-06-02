// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use std::fs::read_to_string;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use libc::mallinfo2;
use reifydb_engine::engine::StandardEngine;
use reifydb_store_multi::MultiStore;

#[derive(Clone)]
pub struct Collectors {
	pub engine: StandardEngine,
	pub multi_store: MultiStore,
}

pub struct Sample {
	pub scope: &'static str,
	pub metric: &'static str,
	pub value: f64,
	pub unit: &'static str,
}

impl Sample {
	fn new(scope: &'static str, metric: &'static str, value: f64, unit: &'static str) -> Self {
		Self {
			scope,
			metric,
			value,
			unit,
		}
	}
}

pub fn collect_memory(c: &Collectors) -> Vec<Sample> {
	let mut out = Vec::with_capacity(24);

	let proc_mem = collect_process();
	let alloc = collect_allocator();

	push_process_allocator_samples(&mut out, &proc_mem, &alloc);
	push_buffer_derived_samples(c, &mut out, &proc_mem, &alloc);

	out
}

#[inline]
fn push_process_allocator_samples(out: &mut Vec<Sample>, proc_mem: &Option<ProcMem>, alloc: &Option<AllocMem>) {
	if let Some(p) = proc_mem {
		out.push(Sample::new("process", "rss_total_bytes", p.rss_total as f64, "bytes"));
		out.push(Sample::new("process", "rss_anon_bytes", p.rss_anon as f64, "bytes"));
		out.push(Sample::new("process", "rss_file_bytes", p.rss_file as f64, "bytes"));
		out.push(Sample::new("process", "rss_shmem_bytes", p.rss_shmem as f64, "bytes"));
		out.push(Sample::new("process", "vm_size_bytes", p.vm_size as f64, "bytes"));
		out.push(Sample::new("process", "vm_data_bytes", p.vm_data as f64, "bytes"));
		out.push(Sample::new("process", "private_dirty_bytes", p.private_dirty as f64, "bytes"));
		out.push(Sample::new("process", "private_clean_bytes", p.private_clean as f64, "bytes"));
		out.push(Sample::new("process", "pss_bytes", p.pss as f64, "bytes"));
		out.push(Sample::new("process", "uss_bytes", (p.private_dirty + p.private_clean) as f64, "bytes"));
		out.push(Sample::new("process", "thread_count", p.threads as f64, "threads"));
	}

	if let Some(a) = alloc {
		out.push(Sample::new("allocator", "heap_live_bytes", a.heap_live as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_free_retained_bytes", a.heap_free_retained as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_arena_bytes", a.heap_arena as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_mmap_bytes", a.heap_mmap as f64, "bytes"));
	}
}

#[inline]
fn push_buffer_derived_samples(
	c: &Collectors,
	out: &mut Vec<Sample>,
	proc_mem: &Option<ProcMem>,
	alloc: &Option<AllocMem>,
) {
	collect_buffer(c, out);

	if let Some(p) = proc_mem {
		if p.rss_total > 0 {
			out.push(Sample::new("derived", "mmap_share", p.rss_file as f64 / p.rss_total as f64, "ratio"));
		}
		if let Some(a) = alloc
			&& p.rss_anon > 0
		{
			let frag = (p.rss_anon as f64 - a.heap_live as f64) / p.rss_anon as f64;
			out.push(Sample::new("derived", "heap_retention_ratio", frag, "ratio"));
		}
	}
}

pub fn collect_watermarks(c: &Collectors) -> Vec<Sample> {
	let mut out = Vec::with_capacity(9);
	collect_mvcc(c, &mut out);
	collect_cdc(c, &mut out);
	out
}

fn collect_buffer(c: &Collectors, out: &mut Vec<Sample>) {
	let Some(buffer) = c.multi_store.commit() else {
		out.push(Sample::new("buffer", "buffer_table_count", 0.0, "count"));
		return;
	};
	let Ok(kinds) = buffer.list_all_entry_kinds() else {
		return;
	};
	out.push(Sample::new("buffer", "buffer_table_count", kinds.len() as f64, "count"));
	let mut current_total: u64 = 0;
	for kind in kinds {
		if let Ok(n) = buffer.count_current(kind) {
			current_total += n;
		}
	}
	out.push(Sample::new("buffer", "buffer_current_keys_total", current_total as f64, "count"));
}

fn collect_mvcc(c: &Collectors, out: &mut Vec<Sample>) {
	let commit = c.engine.done_until().0;
	let query = c.engine.query_done_until().0;
	let last = c.engine.current_version().map(|v| v.0).unwrap_or(commit);

	out.push(Sample::new("mvcc", "commit_watermark", commit as f64, "versions"));
	out.push(Sample::new("mvcc", "query_watermark", query as f64, "versions"));
	out.push(Sample::new("mvcc", "last_allocated_version", last as f64, "versions"));
	out.push(Sample::new("mvcc", "watermark_lag", last.saturating_sub(query) as f64, "versions"));
	out.push(Sample::new("mvcc", "query_command_skew", commit.saturating_sub(query) as f64, "versions"));
	out.push(Sample::new("mvcc", "oracle_window_count", c.engine.oracle_window_count() as f64, "count"));
}

fn collect_cdc(c: &Collectors, out: &mut Vec<Sample>) {
	let producer = c.engine.cdc_producer_watermark().0;
	let consumer = c.engine.cdc_consumer_watermark().0;
	out.push(Sample::new("cdc", "cdc_producer_watermark", producer as f64, "versions"));
	out.push(Sample::new("cdc", "cdc_consumer_watermark", consumer as f64, "versions"));
	out.push(Sample::new("cdc", "cdc_lag", producer.saturating_sub(consumer) as f64, "versions"));
}

struct ProcMem {
	rss_total: u64,
	rss_anon: u64,
	rss_file: u64,
	rss_shmem: u64,
	vm_size: u64,
	vm_data: u64,
	private_dirty: u64,
	private_clean: u64,
	pss: u64,
	threads: u64,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn collect_process() -> Option<ProcMem> {
	let status = read_to_string("/proc/self/status").ok()?;
	let rollup = read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
	Some(ProcMem {
		rss_total: field_kb(&status, "VmRSS:").unwrap_or(0),
		rss_anon: field_kb(&status, "RssAnon:").unwrap_or(0),
		rss_file: field_kb(&status, "RssFile:").unwrap_or(0),
		rss_shmem: field_kb(&status, "RssShmem:").unwrap_or(0),
		vm_size: field_kb(&status, "VmSize:").unwrap_or(0),
		vm_data: field_kb(&status, "VmData:").unwrap_or(0),
		threads: field_raw(&status, "Threads:").unwrap_or(0),
		private_dirty: field_kb(&rollup, "Private_Dirty:").unwrap_or(0),
		private_clean: field_kb(&rollup, "Private_Clean:").unwrap_or(0),
		pss: field_kb(&rollup, "Pss:").unwrap_or(0),
	})
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn field_raw(content: &str, key: &str) -> Option<u64> {
	content.lines().find_map(|line| {
		let rest = line.strip_prefix(key)?;
		rest.split_whitespace().next()?.parse::<u64>().ok()
	})
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn field_kb(content: &str, key: &str) -> Option<u64> {
	field_raw(content, key).map(|kb| kb * 1024)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn collect_process() -> Option<ProcMem> {
	None
}

struct AllocMem {
	heap_live: u64,
	heap_free_retained: u64,
	heap_arena: u64,
	heap_mmap: u64,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn collect_allocator() -> Option<AllocMem> {
	let mi = unsafe { mallinfo2() };
	Some(AllocMem {
		heap_live: mi.uordblks as u64,
		heap_free_retained: mi.fordblks as u64,
		heap_arena: mi.arena as u64,
		heap_mmap: mi.hblkhd as u64,
	})
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn collect_allocator() -> Option<AllocMem> {
	None
}
