// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, thread};

use reifydb_metric::{counter::Counter, gauge::Gauge, histogram::Histogram, statement::StatementStats};

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 10_000;

#[test]
fn counter_concurrent_inc() {
	let c = Arc::new(Counter::new("t", "h"));
	let handles: Vec<_> = (0..THREADS)
		.map(|_| {
			let c = Arc::clone(&c);
			thread::spawn(move || {
				for _ in 0..OPS_PER_THREAD {
					c.inc();
				}
			})
		})
		.collect();
	for h in handles {
		h.join().unwrap();
	}
	assert_eq!(c.get(), (THREADS * OPS_PER_THREAD) as f64);
}

#[test]
fn counter_concurrent_add() {
	let c = Arc::new(Counter::new("t", "h"));
	let handles: Vec<_> = (0..THREADS)
		.map(|_| {
			let c = Arc::clone(&c);
			thread::spawn(move || {
				for _ in 0..OPS_PER_THREAD {
					c.add(0.5);
				}
			})
		})
		.collect();
	for h in handles {
		h.join().unwrap();
	}
	assert_eq!(c.get(), (THREADS * OPS_PER_THREAD) as f64 * 0.5);
}

#[test]
fn gauge_concurrent_inc_dec() {
	let g = Arc::new(Gauge::new("t", "h"));
	let handles: Vec<_> = (0..THREADS)
		.map(|i| {
			let g = Arc::clone(&g);
			thread::spawn(move || {
				for _ in 0..OPS_PER_THREAD {
					if i % 2 == 0 {
						g.inc();
					} else {
						g.dec();
					}
				}
			})
		})
		.collect();
	for h in handles {
		h.join().unwrap();
	}
	// Equal threads inc and dec → net zero
	assert_eq!(g.get(), 0.0);
}

static BOUNDS: &[f64] = &[10.0, 20.0, 50.0, 100.0];

#[test]
fn histogram_concurrent_observe() {
	let h = Arc::new(Histogram::new("t", "h", BOUNDS));
	let handles: Vec<_> = (0..THREADS)
		.map(|_| {
			let h = Arc::clone(&h);
			thread::spawn(move || {
				for _ in 0..OPS_PER_THREAD {
					h.observe(15.0);
				}
			})
		})
		.collect();
	for handle in handles {
		handle.join().unwrap();
	}
	let snap = h.snapshot();
	let expected_count = (THREADS * OPS_PER_THREAD) as u64;
	assert_eq!(snap.count, expected_count);
	assert_eq!(snap.sum, expected_count as f64 * 15.0);
}

#[test]
fn statement_stats_concurrent_record() {
	let s = Arc::new(StatementStats::new("q".into()));
	let handles: Vec<_> = (0..THREADS)
		.map(|i| {
			let s = Arc::clone(&s);
			thread::spawn(move || {
				for j in 0..OPS_PER_THREAD {
					let duration = (i * OPS_PER_THREAD + j) as u64;
					s.record(duration, duration, 1, true);
				}
			})
		})
		.collect();
	for h in handles {
		h.join().unwrap();
	}
	let total = (THREADS * OPS_PER_THREAD) as u64;
	assert_eq!(s.calls(), total);
	assert_eq!(s.total_rows(), total);
	assert_eq!(s.errors(), 0);
	assert_eq!(s.min_duration_us(), 0);
	assert_eq!(s.max_duration_us(), total - 1);
}
