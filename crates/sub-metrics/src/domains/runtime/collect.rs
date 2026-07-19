// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use std::fs::read_to_string;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use libc::mallinfo2;
use reifydb_allocator::{JemallocStats, jemalloc_stats};
use reifydb_core::metrics::{registry::MetricsRegistry, sample::MetricsSample};
use reifydb_engine::engine::StandardEngine;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::memory::global_memory_used;
use reifydb_value::byte_size::ByteSize;

#[derive(Clone)]
pub struct Collectors {
	pub engine: StandardEngine,
	pub registry: MetricsRegistry,
}

pub fn collect_memory(c: &Collectors) -> Vec<MetricsSample> {
	let mut out = Vec::with_capacity(32);

	let proc_mem = collect_process();
	let jemalloc = jemalloc_stats();
	let alloc = collect_allocator();

	push_process_samples(&mut out, &proc_mem);
	push_allocator_samples(&mut out, &jemalloc, &alloc);
	push_subsystem_samples(c, &mut out);
	push_sqlite_samples(&mut out);
	let named_heap = out.iter().filter_map(|sample| sample.reading.heap_bytes()).sum::<u64>();
	push_derived_samples(&mut out, named_heap, &proc_mem, &jemalloc, &alloc);

	out
}

#[cfg(not(target_arch = "wasm32"))]
fn push_sqlite_samples(out: &mut Vec<MetricsSample>) {
	out.push(MetricsSample::heap("sqlite", "memory_used_bytes", global_memory_used()));
}

#[cfg(target_arch = "wasm32")]
fn push_sqlite_samples(_out: &mut Vec<MetricsSample>) {}

#[inline]
fn push_process_samples(out: &mut Vec<MetricsSample>, proc_mem: &Option<ProcMem>) {
	if let Some(p) = proc_mem {
		out.push(MetricsSample::bytes("process", "rss_total_bytes", ByteSize::from_bytes(p.rss_total)));
		out.push(MetricsSample::bytes("process", "rss_anon_bytes", ByteSize::from_bytes(p.rss_anon)));
		out.push(MetricsSample::bytes("process", "rss_file_bytes", ByteSize::from_bytes(p.rss_file)));
		out.push(MetricsSample::bytes("process", "rss_shmem_bytes", ByteSize::from_bytes(p.rss_shmem)));
		out.push(MetricsSample::bytes("process", "vm_size_bytes", ByteSize::from_bytes(p.vm_size)));
		out.push(MetricsSample::bytes("process", "vm_data_bytes", ByteSize::from_bytes(p.vm_data)));
		out.push(MetricsSample::bytes("process", "private_dirty_bytes", ByteSize::from_bytes(p.private_dirty)));
		out.push(MetricsSample::bytes("process", "private_clean_bytes", ByteSize::from_bytes(p.private_clean)));
		out.push(MetricsSample::bytes("process", "pss_bytes", ByteSize::from_bytes(p.pss)));
		out.push(MetricsSample::bytes(
			"process",
			"uss_bytes",
			ByteSize::from_bytes(p.private_dirty + p.private_clean),
		));
		out.push(MetricsSample::count("process", "thread_count", p.threads));
	}
}

#[inline]
fn push_allocator_samples(out: &mut Vec<MetricsSample>, jemalloc: &Option<JemallocStats>, alloc: &Option<AllocMem>) {
	if let Some(j) = jemalloc {
		out.push(MetricsSample::bytes(
			"allocator",
			"jemalloc_allocated_bytes",
			ByteSize::from_bytes(j.allocated),
		));
		out.push(MetricsSample::bytes("allocator", "jemalloc_active_bytes", ByteSize::from_bytes(j.active)));
		out.push(MetricsSample::bytes(
			"allocator",
			"jemalloc_resident_bytes",
			ByteSize::from_bytes(j.resident),
		));
		out.push(MetricsSample::bytes("allocator", "jemalloc_mapped_bytes", ByteSize::from_bytes(j.mapped)));
		out.push(MetricsSample::bytes(
			"allocator",
			"jemalloc_retained_bytes",
			ByteSize::from_bytes(j.retained),
		));
		out.push(MetricsSample::bytes(
			"allocator",
			"jemalloc_metadata_bytes",
			ByteSize::from_bytes(j.metadata),
		));
	} else if let Some(a) = alloc {
		out.push(MetricsSample::bytes("allocator", "heap_live_bytes", ByteSize::from_bytes(a.heap_live)));
		out.push(MetricsSample::bytes(
			"allocator",
			"heap_free_retained_bytes",
			ByteSize::from_bytes(a.heap_free_retained),
		));
		out.push(MetricsSample::bytes("allocator", "heap_arena_bytes", ByteSize::from_bytes(a.heap_arena)));
		out.push(MetricsSample::bytes("allocator", "heap_mmap_bytes", ByteSize::from_bytes(a.heap_mmap)));
	}
}

#[inline]
fn push_subsystem_samples(c: &Collectors, out: &mut Vec<MetricsSample>) {
	out.extend(c.registry.collect());
	collect_dictionary(c, out);
}

#[inline]
fn push_derived_samples(
	out: &mut Vec<MetricsSample>,
	named_heap: u64,
	proc_mem: &Option<ProcMem>,
	jemalloc: &Option<JemallocStats>,
	alloc: &Option<AllocMem>,
) {
	let Some(p) = proc_mem else {
		return;
	};

	if p.rss_total > 0 {
		out.push(MetricsSample::ratio("derived", "mmap_share", p.rss_file as f64 / p.rss_total as f64));
	}

	out.push(MetricsSample::bytes("derived", "named_bytes", ByteSize::from_bytes(named_heap)));

	if let Some(j) = jemalloc {
		let dark = j.allocated.saturating_sub(named_heap);
		out.push(MetricsSample::bytes("derived", "dark_bytes", ByteSize::from_bytes(dark)));
		if j.allocated > 0 {
			out.push(MetricsSample::ratio("derived", "dark_share", dark as f64 / j.allocated as f64));
		}
		out.push(MetricsSample::bytes(
			"derived",
			"allocator_fragmentation_bytes",
			ByteSize::from_bytes(j.resident.saturating_sub(j.allocated)),
		));
		if p.rss_anon > 0 {
			let non_jemalloc = p.rss_anon.saturating_sub(j.resident);
			out.push(MetricsSample::bytes(
				"derived",
				"non_jemalloc_anon_bytes",
				ByteSize::from_bytes(non_jemalloc),
			));
			out.push(MetricsSample::ratio(
				"derived",
				"heap_retention_ratio",
				(p.rss_anon as f64 - j.allocated as f64) / p.rss_anon as f64,
			));
		}
	} else if let Some(a) = alloc
		&& p.rss_anon > 0
	{
		out.push(MetricsSample::ratio(
			"derived",
			"heap_retention_ratio",
			(p.rss_anon as f64 - a.heap_live as f64) / p.rss_anon as f64,
		));
	}
}

fn collect_dictionary(c: &Collectors, out: &mut Vec<MetricsSample>) {
	let (count, bytes) = c.engine.dictionary_allocators().cached_entries();
	out.push(MetricsSample::count("dictionary", "cached_entry_count", count as u64));
	out.push(MetricsSample::heap("dictionary", "cached_entry_bytes", ByteSize::from_bytes(bytes)));
}

pub fn collect_watermarks(c: &Collectors) -> Vec<MetricsSample> {
	let mut out = Vec::with_capacity(9);
	collect_mvcc(c, &mut out);
	collect_cdc(c, &mut out);
	out
}

pub fn collect_operators(c: &Collectors) -> Vec<MetricsSample> {
	let read_buffer = c.engine.read_buffer_operator_metrics();
	let disk = c.engine.operator_disk_payload_bytes();
	let mut out = Vec::with_capacity(read_buffer.len() * 2 + disk.len());
	for metrics in read_buffer {
		let scope = format!("flow_node::{}", metrics.node);
		out.push(MetricsSample::bytes(scope.clone(), "read_buffer_resident_bytes", metrics.resident));
		out.push(MetricsSample::bytes(scope, "read_buffer_payload_bytes", metrics.payload));
	}
	for (node, bytes) in disk {
		out.push(MetricsSample::bytes(format!("flow_node::{node}"), "disk_payload_bytes", bytes));
	}
	out
}

fn collect_mvcc(c: &Collectors, out: &mut Vec<MetricsSample>) {
	let commit = c.engine.done_until().0;
	let query = c.engine.query_done_until().0;
	let last = c.engine.current_version().map(|v| v.0).unwrap_or(commit);

	out.push(MetricsSample::version("mvcc", "commit_watermark", commit));
	out.push(MetricsSample::version("mvcc", "query_watermark", query));
	out.push(MetricsSample::version("mvcc", "last_allocated_version", last));
	out.push(MetricsSample::version("mvcc", "watermark_lag", last.saturating_sub(query)));
	out.push(MetricsSample::version("mvcc", "query_command_skew", commit.saturating_sub(query)));
	out.push(MetricsSample::count("mvcc", "oracle_window_count", c.engine.oracle_window_count() as u64));
}

fn collect_cdc(c: &Collectors, out: &mut Vec<MetricsSample>) {
	let producer = c.engine.cdc_producer_watermark().0;
	let consumer = c.engine.cdc_consumer_watermark().0;
	out.push(MetricsSample::version("cdc", "cdc_producer_watermark", producer));
	out.push(MetricsSample::version("cdc", "cdc_consumer_watermark", consumer));
	out.push(MetricsSample::version("cdc", "cdc_lag", producer.saturating_sub(consumer)));
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
