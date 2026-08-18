// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(target_os = "linux", target_env = "gnu"))]
use libc::mallinfo2;
use reifydb_allocator::{JemallocStats, jemalloc_stats};
use reifydb_cdc::storage::CdcStore;
use reifydb_core::metrics::{registry::MetricsRegistry, sample::MetricsSample};
use reifydb_engine::engine::StandardEngine;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::memory::global_memory_used;
use reifydb_value::byte_size::ByteSize;

use crate::domains::proc::{ProcessStatus, process_status};

#[derive(Clone)]
pub struct Collectors {
	pub engine: StandardEngine,
	pub registry: MetricsRegistry,
}

pub fn collect_memory(c: &Collectors) -> Vec<MetricsSample> {
	let mut out = Vec::with_capacity(32);

	let proc_mem = process_status();
	let jemalloc = jemalloc_stats();
	let alloc = collect_allocator();

	push_allocator_samples(&mut out, &jemalloc, &alloc);
	push_subsystem_samples(c, &mut out);
	push_operator_rollup(c, &mut out);
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
fn push_operator_rollup(c: &Collectors, out: &mut Vec<MetricsSample>) {
	let heap: u64 = c.registry.collect_operators().iter().filter_map(|sample| sample.reading.heap_bytes()).sum();
	out.push(MetricsSample::heap("flow_operators", "resident_bytes", ByteSize::from_bytes(heap)));
}

#[inline]
fn push_derived_samples(
	out: &mut Vec<MetricsSample>,
	named_heap: u64,
	proc_mem: &Option<ProcessStatus>,
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
	c.registry.collect_operators()
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
	if let Some(store) = c.engine.ioc().try_resolve::<CdcStore>()
		&& let Ok(truncated) = store.truncated_before()
	{
		out.push(MetricsSample::version("cdc", "cdc_truncated_before", truncated.0));
	}
}

struct AllocMem {
	heap_live: u64,
	heap_free_retained: u64,
	heap_arena: u64,
	heap_mmap: u64,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn collect_allocator() -> Option<AllocMem> {
	// SAFETY: mallinfo2 takes no arguments, reads only libc's own allocator statistics and returns the struct by
	// value, so the call carries no pointer, lifetime or aliasing obligation for the caller to uphold.
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
