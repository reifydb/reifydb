// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::atomic::{AtomicU64, Ordering},
	time::Instant,
};

pub static QUERIES: AtomicU64 = AtomicU64::new(0);
pub static SETUP_NS: AtomicU64 = AtomicU64::new(0);
pub static COMPILE_NS: AtomicU64 = AtomicU64::new(0);
pub static EXECUTE_NS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_CALLS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_ROWS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_STORAGE_NS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_DECODE_NS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_STAMP_NS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_BATCHES: AtomicU64 = AtomicU64::new(0);
pub static FILTER_EVAL_NS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_MASK_NS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_COMPACT_NS: AtomicU64 = AtomicU64::new(0);
pub static FILTER_INIT_NS: AtomicU64 = AtomicU64::new(0);

pub fn add(counter: &AtomicU64, start: Instant) {
	counter.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

pub fn bump(counter: &AtomicU64, by: u64) {
	counter.fetch_add(by, Ordering::Relaxed);
}

pub fn finish_query() {
	let done = QUERIES.fetch_add(1, Ordering::Relaxed) + 1;
	if done % 100 != 0 {
		return;
	}
	let take = |counter: &AtomicU64| counter.swap(0, Ordering::Relaxed) / 100;
	let us = |ns: u64| ns / 1000;
	eprintln!(
		"PROBE queries={} per_query: setup_us={} compile_us={} execute_us={} | scan calls={} rows={} storage_us={} decode_us={} stamp_us={} | filter init_us={} batches={} eval_us={} mask_us={} compact_us={}",
		done,
		us(take(&SETUP_NS)),
		us(take(&COMPILE_NS)),
		us(take(&EXECUTE_NS)),
		take(&SCAN_CALLS),
		take(&SCAN_ROWS),
		us(take(&SCAN_STORAGE_NS)),
		us(take(&SCAN_DECODE_NS)),
		us(take(&SCAN_STAMP_NS)),
		us(take(&FILTER_INIT_NS)),
		take(&FILTER_BATCHES),
		us(take(&FILTER_EVAL_NS)),
		us(take(&FILTER_MASK_NS)),
		us(take(&FILTER_COMPACT_NS)),
	);
}
