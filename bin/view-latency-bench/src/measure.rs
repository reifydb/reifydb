// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	hint::spin_loop,
	thread::yield_now,
	time::Duration,
};

use hdrhistogram::Histogram;
use reifydb::{Clock, Database, Params};

use crate::workload::{self, Workload};

const HIST_MAX_US: u64 = 60_000_000;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Copy, Clone)]
pub struct Stats {
	pub p50: u64,
	pub p99: u64,
	pub p999: u64,
	pub max: u64,
}

impl Stats {
	fn from(h: &Histogram<u64>) -> Self {
		Stats {
			p50: h.value_at_quantile(0.50),
			p99: h.value_at_quantile(0.99),
			p999: h.value_at_quantile(0.999),
			max: h.max(),
		}
	}
}

pub struct Outcome {
	pub commit: Stats,
	pub fresh: Stats,
	pub timeouts: u64,
}

pub fn pass_a(db: &Database, workload: Workload, iterations: u64, warmup: u64) -> Outcome {
	let mut commit = Histogram::<u64>::new_with_bounds(1, HIST_MAX_US, 3).unwrap();
	let mut fresh = Histogram::<u64>::new_with_bounds(1, HIST_MAX_US, 3).unwrap();
	let mut timeouts = 0u64;
	let clock = Clock::Real;

	let total = warmup + iterations;
	for i in 0..total {
		let id = i as i64;
		let measured = i >= warmup;

		let insert = workload::insert_row(workload, id);
		let t0 = clock.instant();
		db.command_as_root(&insert, Params::None).expect("insert failed");
		let t_commit = clock.instant();

		let probe = workload::probe_query(id);
		let mut spins = 0u32;
		let mut timed_out = false;
		loop {
			let frames = db.query_as_root(&probe, Params::None).expect("probe query failed");
			if frames.first().map(|f| f.row_count()).unwrap_or(0) > 0 {
				break;
			}
			if t_commit.elapsed() >= PROBE_TIMEOUT {
				timed_out = true;
				break;
			}
			spins += 1;
			if spins < 64 {
				spin_loop();
			} else {
				yield_now();
			}
		}
		let t_visible = clock.instant();

		if measured {
			let c = ((&t_commit - &t0).as_micros() as u64).clamp(1, HIST_MAX_US);
			commit.record(c).ok();
			if timed_out {
				timeouts += 1;
				fresh.record(HIST_MAX_US).ok();
			} else {
				let v = ((&t_visible - &t0).as_micros() as u64).clamp(1, HIST_MAX_US);
				fresh.record(v).ok();
			}
		}
	}

	Outcome {
		commit: Stats::from(&commit),
		fresh: Stats::from(&fresh),
		timeouts,
	}
}
