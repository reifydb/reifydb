// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, thread};

use reifydb_metrics::statement::StatementMetricsAggregate;

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 10_000;

#[test]
fn statement_stats_concurrent_record() {
	let s = Arc::new(StatementMetricsAggregate::new("q".into()));
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
