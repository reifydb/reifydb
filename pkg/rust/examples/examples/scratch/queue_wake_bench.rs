// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	process::exit,
	sync::{
		Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb::{Database, Params, Session, embedded};
use reifydb_value::value::duration::Duration;

const WORKERS: usize = 8;
const RUN_SECS: u64 = 3;
const INSERT_EVERY_MS: u64 = 5;
const PARTITIONS: u16 = 1;

struct Report {
	workers: usize,
	claimed: u64,
	empty_rescans: u64,
	p50_ms: f64,
	p95_ms: f64,
	max_ms: f64,
	spread: u64,
}

#[derive(Default)]
struct Collector {
	wake_us: Vec<u64>,
	per_worker: Vec<u64>,
	empty_rescans: u64,
}

fn main() {
	let report = run();

	println!("policy,workers,claimed,empty_rescans,p50_ms,p95_ms,max_ms,spread");
	println!(
		"fifo_wake_n,{},{},{},{:.3},{:.3},{:.3},{}",
		report.workers,
		report.claimed,
		report.empty_rescans,
		report.p50_ms,
		report.p95_ms,
		report.max_ms,
		report.spread
	);

	exit(i32::from(report.claimed == 0));
}

fn database() -> Database {
	let db = embedded::memory().build().unwrap();
	db.admin_as_root("CREATE NAMESPACE bench", Params::None).unwrap();
	db.admin_as_root(
		&format!("CREATE QUEUE bench::jobs {{ seq: int8 }} WITH {{ fifo: {{ partitions: {PARTITIONS} }} }}"),
		Params::None,
	)
	.unwrap();
	db
}

fn percentile(sorted_us: &[u64], fraction: f64) -> f64 {
	if sorted_us.is_empty() {
		return 0.0;
	}
	let index = ((sorted_us.len() - 1) as f64 * fraction).round() as usize;
	sorted_us[index] as f64 / 1_000.0
}

fn run() -> Report {
	let db = database();
	let sessions: Vec<Session> = (0..WORKERS).map(|_| db.root_session()).collect();
	let inserter = db.root_session();

	let collector = Mutex::new(Collector {
		per_worker: vec![0; WORKERS],
		..Collector::default()
	});
	let inserted = AtomicU64::new(0);
	let inserting = AtomicBool::new(true);
	let deadline = Instant::now() + StdDuration::from_secs(RUN_SECS);

	thread::scope(|scope| {
		let collector = &collector;
		let inserted = &inserted;
		let inserting = &inserting;

		scope.spawn(|| {
			let mut next = 0u64;
			while Instant::now() < deadline {
				let _ = inserter
					.command(&format!("INSERT bench::jobs [{{ seq: {next} }}]"), Params::None);
				inserted.fetch_add(1, Ordering::Relaxed);
				next += 1;
				thread::sleep(StdDuration::from_millis(INSERT_EVERY_MS));
			}
			inserting.store(false, Ordering::Release);
		});

		for (worker, session) in sessions.iter().enumerate() {
			scope.spawn(move || {
				while inserting.load(Ordering::Acquire) {
					let call = Instant::now();
					let result = session.claim_wait(
						"bench::jobs",
						&format!("w{worker}"),
						1,
						Duration::from_seconds(30).unwrap(),
						Duration::from_milliseconds(500).unwrap(),
					);
					let elapsed = call.elapsed();

					let rows: usize = result.frames.iter().map(|frame| frame.row_count()).sum();
					let mut collector = collector.lock().unwrap();
					if rows == 0 {
						collector.empty_rescans += 1;
					} else {
						collector.wake_us.push(elapsed.as_micros() as u64);
						collector.per_worker[worker] += rows as u64;
					}
				}
			});
		}
	});

	let collector = collector.into_inner().unwrap();
	let mut wake_us = collector.wake_us;
	wake_us.sort_unstable();

	let claimed: u64 = collector.per_worker.iter().sum();
	let spread = collector.per_worker.iter().max().copied().unwrap_or(0)
		- collector.per_worker.iter().min().copied().unwrap_or(0);

	Report {
		workers: WORKERS,
		claimed,
		empty_rescans: collector.empty_rescans,
		p50_ms: percentile(&wake_us, 0.50),
		p95_ms: percentile(&wake_us, 0.95),
		max_ms: wake_us.last().copied().unwrap_or(0) as f64 / 1_000.0,
		spread,
	}
}
