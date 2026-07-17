// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use std::fs::read_to_string;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use libc::mallinfo2;
use reifydb_allocator::{JemallocStats, jemalloc_stats};
use reifydb_core::util::memory::MemoryRegistry;
use reifydb_engine::engine::StandardEngine;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::memory::global_memory_used;

#[derive(Clone)]
pub struct Collectors {
	pub engine: StandardEngine,
	pub registry: MemoryRegistry,
}

pub struct Sample {
	pub scope: Cow<'static, str>,
	pub metric: &'static str,
	pub value: f64,
	pub unit: &'static str,
}

impl Sample {
	fn new(scope: impl Into<Cow<'static, str>>, metric: &'static str, value: f64, unit: &'static str) -> Self {
		Self {
			scope: scope.into(),
			metric,
			value,
			unit,
		}
	}
}

pub fn collect_memory(c: &Collectors) -> Vec<Sample> {
	let mut out = Vec::with_capacity(32);

	let proc_mem = collect_process();
	let jemalloc = jemalloc_stats();
	let alloc = collect_allocator();

	push_process_samples(&mut out, &proc_mem);
	push_allocator_samples(&mut out, &jemalloc, &alloc);
	push_subsystem_samples(c, &mut out);
	push_sqlite_samples(&mut out);
	push_derived_samples(&mut out, &proc_mem, &jemalloc, &alloc);

	out
}

#[cfg(not(target_arch = "wasm32"))]
fn push_sqlite_samples(out: &mut Vec<Sample>) {
	out.push(Sample::new("sqlite", "memory_used_bytes", global_memory_used().as_bytes() as f64, "bytes"));
}

#[cfg(target_arch = "wasm32")]
fn push_sqlite_samples(_out: &mut Vec<Sample>) {}

#[inline]
fn push_process_samples(out: &mut Vec<Sample>, proc_mem: &Option<ProcMem>) {
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
}

#[inline]
fn push_allocator_samples(out: &mut Vec<Sample>, jemalloc: &Option<JemallocStats>, alloc: &Option<AllocMem>) {
	if let Some(j) = jemalloc {
		out.push(Sample::new("allocator", "jemalloc_allocated_bytes", j.allocated as f64, "bytes"));
		out.push(Sample::new("allocator", "jemalloc_active_bytes", j.active as f64, "bytes"));
		out.push(Sample::new("allocator", "jemalloc_resident_bytes", j.resident as f64, "bytes"));
		out.push(Sample::new("allocator", "jemalloc_mapped_bytes", j.mapped as f64, "bytes"));
		out.push(Sample::new("allocator", "jemalloc_retained_bytes", j.retained as f64, "bytes"));
		out.push(Sample::new("allocator", "jemalloc_metadata_bytes", j.metadata as f64, "bytes"));
	} else if let Some(a) = alloc {
		out.push(Sample::new("allocator", "heap_live_bytes", a.heap_live as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_free_retained_bytes", a.heap_free_retained as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_arena_bytes", a.heap_arena as f64, "bytes"));
		out.push(Sample::new("allocator", "heap_mmap_bytes", a.heap_mmap as f64, "bytes"));
	}
}

#[inline]
fn push_subsystem_samples(c: &Collectors, out: &mut Vec<Sample>) {
	for sample in c.registry.collect() {
		out.push(Sample::new(sample.scope, sample.metric, sample.value, sample.unit));
	}
	collect_dictionary(c, out);
}

#[inline]
fn push_derived_samples(
	out: &mut Vec<Sample>,
	proc_mem: &Option<ProcMem>,
	jemalloc: &Option<JemallocStats>,
	alloc: &Option<AllocMem>,
) {
	let Some(p) = proc_mem else {
		return;
	};

	if p.rss_total > 0 {
		out.push(Sample::new("derived", "mmap_share", p.rss_file as f64 / p.rss_total as f64, "ratio"));
	}

	if let Some(j) = jemalloc {
		out.push(Sample::new(
			"derived",
			"allocator_fragmentation_bytes",
			j.resident.saturating_sub(j.allocated) as f64,
			"bytes",
		));
		if p.rss_anon > 0 {
			let unaccounted = (p.rss_anon as f64 - j.resident as f64).max(0.0);
			out.push(Sample::new("derived", "unaccounted_anon_bytes", unaccounted, "bytes"));
			out.push(Sample::new(
				"derived",
				"heap_retention_ratio",
				(p.rss_anon as f64 - j.allocated as f64) / p.rss_anon as f64,
				"ratio",
			));
		}
	} else if let Some(a) = alloc
		&& p.rss_anon > 0
	{
		out.push(Sample::new(
			"derived",
			"heap_retention_ratio",
			(p.rss_anon as f64 - a.heap_live as f64) / p.rss_anon as f64,
			"ratio",
		));
	}
}

fn collect_dictionary(c: &Collectors, out: &mut Vec<Sample>) {
	let (count, bytes) = c.engine.dictionary_allocators().cached_entries();
	out.push(Sample::new("dictionary", "cached_entry_count", count as f64, "count"));
	out.push(Sample::new("dictionary", "cached_entry_bytes", bytes as f64, "bytes"));
}

pub fn collect_watermarks(c: &Collectors) -> Vec<Sample> {
	let mut out = Vec::with_capacity(9);
	collect_mvcc(c, &mut out);
	collect_cdc(c, &mut out);
	out
}

pub fn collect_operators(c: &Collectors) -> Vec<Sample> {
	let read_buffer = c.engine.operator_read_buffer_usage();
	let disk = c.engine.operator_disk_payload_bytes();
	let mut out = Vec::with_capacity(read_buffer.len() * 2 + disk.len());
	for usage in read_buffer {
		let scope = format!("flow_node::{}", usage.node);
		out.push(Sample::new(
			scope.clone(),
			"read_buffer_resident_bytes",
			usage.resident.as_bytes() as f64,
			"bytes",
		));
		out.push(Sample::new(scope, "read_buffer_payload_bytes", usage.payload.as_bytes() as f64, "bytes"));
	}
	for (node, bytes) in disk {
		out.push(Sample::new(
			format!("flow_node::{node}"),
			"disk_payload_bytes",
			bytes.as_bytes() as f64,
			"bytes",
		));
	}
	out
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
	// SAFETY: mallinfo2 is a libc global-allocator statistics call with no

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
