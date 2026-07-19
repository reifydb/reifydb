// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Instruments are updated from producer threads at event rate; correctness under contention
//! is part of their contract.

use std::{sync::Arc, thread};

use reifydb_core::metrics::{
	instruments::{counter::Counter, gauge::Gauge, histogram::Histogram},
	sample::ReadingKind,
};

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
	let g = Arc::new(Gauge::new("t", "h", ReadingKind::Count));
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
	let h = Arc::new(Histogram::new("t", "h", ReadingKind::Ratio, BOUNDS));
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
	let expected_count = (THREADS * OPS_PER_THREAD) as u64;
	assert_eq!(h.count(), expected_count);
	assert_eq!(h.sum(), expected_count as f64 * 15.0);
}
