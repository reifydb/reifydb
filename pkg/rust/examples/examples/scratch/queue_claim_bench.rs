// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	process::exit,
	sync::{
		Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use reifydb::{Database, Params, Session, embedded};
use reifydb_value::value::Value;

const ROWS: u64 = 10_000;
const RUN_SECS: u64 = 3;
const BATCH: usize = 10;
const LEASE_SECS: u64 = 30;
const PARTITIONS: u16 = 16;

struct Report {
	config: &'static str,
	workers: usize,
	ops_ok: u64,
	ops_err: u64,
	rows_claimed: u64,
	p50_ms: f64,
	p95_ms: f64,
	max_ms: f64,
	drained: bool,
	first_err: Option<String>,
	duplicates: Vec<u64>,
	elapsed: Duration,
}

#[derive(Default)]
struct Collector {
	latencies_us: Vec<u64>,
	first_err: Option<String>,
	claimed: HashSet<u64>,
	duplicates: Vec<u64>,
}

fn main() {
	let mut reports = Vec::new();

	for workers in [1usize, 2, 4, 8] {
		reports.push(run_claim_take10(workers));
	}
	for workers in [1usize, 8] {
		reports.push(run_claim_ack_cycle(workers));
	}
	reports.push(run_claim_plus_inserter(8));

	println!("config,workers,ops_ok,ops_err,rows_claimed,p50_ms,p95_ms,max_ms,drained,first_err");
	for report in &reports {
		println!(
			"{},{},{},{},{},{:.3},{:.3},{:.3},{},{}",
			report.config,
			report.workers,
			report.ops_ok,
			report.ops_err,
			report.rows_claimed,
			report.p50_ms,
			report.p95_ms,
			report.max_ms,
			report.drained,
			report.first_err.as_deref().unwrap_or("")
		);
	}

	println!();
	for report in &reports {
		let per_sec = report.rows_claimed as f64 / report.elapsed.as_secs_f64();
		println!("{} workers={} rows/sec={:.0}", report.config, report.workers, per_sec);
	}

	exit(violations(&reports));
}

fn violations(reports: &[Report]) -> i32 {
	let mut failed = false;

	for report in reports {
		if report.ops_err != 0 {
			eprintln!(
				"FAIL {} workers={}: {} failed operations, first: {}",
				report.config,
				report.workers,
				report.ops_err,
				report.first_err.as_deref().unwrap_or("<none>")
			);
			failed = true;
		}
		if !report.duplicates.is_empty() {
			eprintln!(
				"FAIL {} workers={}: {} items handed out more than once under a live lease: {:?}",
				report.config,
				report.workers,
				report.duplicates.len(),
				&report.duplicates[..report.duplicates.len().min(8)]
			);
			failed = true;
		}
	}

	for report in reports.iter().filter(|r| r.config == "claim_take10") {
		if !report.drained {
			eprintln!(
				"FAIL claim_take10 workers={}: drained {} of {} items in {}s",
				report.workers, report.rows_claimed, ROWS, RUN_SECS
			);
			failed = true;
		}
	}

	let take10: Vec<&Report> = reports.iter().filter(|r| r.config == "claim_take10").collect();
	let rate = |r: &Report| r.rows_claimed as f64 / r.elapsed.as_secs_f64();
	for pair in take10.windows(2) {
		if rate(pair[1]) < rate(pair[0]) * 0.9 {
			eprintln!(
				"WARN claim_take10 throughput fell from {} to {} workers: {:.0} -> {:.0} rows/sec",
				pair[0].workers,
				pair[1].workers,
				rate(pair[0]),
				rate(pair[1])
			);
		}
	}
	if let (Some(one), Some(eight)) = (take10.first(), take10.last()) {
		let scale = rate(eight) / rate(one);
		println!("claim_take10 scaling 1 -> 8 workers: {scale:.2}x");
		if scale < 3.0 {
			eprintln!(
				"FAIL claim_take10 scaled only {scale:.2}x from 1 to 8 workers, expected at least 3x"
			);
			failed = true;
		}
	}

	i32::from(failed)
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

fn seed(db: &Database, from: u64, count: u64) {
	for chunk in (from..from + count).collect::<Vec<_>>().chunks(100) {
		let rows: Vec<String> = chunk.iter().map(|seq| format!("{{ seq: {seq} }}")).collect();
		db.command_as_root(&format!("INSERT bench::jobs [{}]", rows.join(", ")), Params::None).unwrap();
	}
}

fn claim(session: &Session, worker: &str) -> Result<Vec<(u64, String)>, String> {
	let result = session.command(
		&format!(r#"CALL queue::claim("{worker}", "bench::jobs", {BATCH}, duration::seconds({LEASE_SECS}))"#),
		Params::None,
	);
	if let Some(err) = result.error {
		return Err(err.to_string());
	}

	let mut claimed = Vec::new();
	for frame in &result.frames {
		for row in frame.to_rows() {
			let mut item = None;
			let mut token = None;
			for (name, value) in row {
				match (name.as_str(), value) {
					("item", Value::Uint8(n)) => item = Some(n),
					("token", Value::Utf8(t)) => token = Some(t),
					_ => {}
				}
			}
			claimed.push((
				item.expect("claim must return an item column"),
				token.expect("claim must return a token column"),
			));
		}
	}
	Ok(claimed)
}

fn ack(session: &Session, token: &str) -> Result<(), String> {
	match session.command(&format!(r#"CALL queue::ack("{token}", "ok", none)"#), Params::None).error {
		Some(err) => Err(err.to_string()),
		None => Ok(()),
	}
}

fn insert(session: &Session, from: u64, count: u64) -> Result<(), String> {
	let rows: Vec<String> = (from..from + count).map(|seq| format!("{{ seq: {seq} }}")).collect();
	match session.command(&format!("INSERT bench::jobs [{}]", rows.join(", ")), Params::None).error {
		Some(err) => Err(err.to_string()),
		None => Ok(()),
	}
}

fn percentile(sorted_us: &[u64], fraction: f64) -> f64 {
	if sorted_us.is_empty() {
		return 0.0;
	}
	let index = ((sorted_us.len() - 1) as f64 * fraction).round() as usize;
	sorted_us[index] as f64 / 1_000.0
}

fn finish(
	config: &'static str,
	workers: usize,
	collector: Collector,
	ops_ok: u64,
	ops_err: u64,
	seeded: u64,
	elapsed: Duration,
) -> Report {
	let mut latencies = collector.latencies_us;
	latencies.sort_unstable();
	let rows_claimed = collector.claimed.len() as u64;

	Report {
		config,
		workers,
		ops_ok,
		ops_err,
		rows_claimed,
		p50_ms: percentile(&latencies, 0.50),
		p95_ms: percentile(&latencies, 0.95),
		max_ms: latencies.last().copied().unwrap_or(0) as f64 / 1_000.0,
		drained: rows_claimed == seeded,
		first_err: collector.first_err,
		duplicates: collector.duplicates,
		elapsed,
	}
}

fn record(collector: &mut Collector, elapsed: Duration, items: &[(u64, String)]) {
	collector.latencies_us.push(elapsed.as_micros() as u64);
	for (item, _) in items {
		if !collector.claimed.insert(*item) {
			collector.duplicates.push(*item);
		}
	}
}

fn note_error(collector: &mut Collector, err: impl ToString) {
	if collector.first_err.is_none() {
		collector.first_err = Some(err.to_string().replace(['\n', ','], " "));
	}
}

fn run_claim_take10(workers: usize) -> Report {
	let db = database();
	seed(&db, 0, ROWS);

	let collector = Mutex::new(Collector::default());
	let ops_ok = AtomicU64::new(0);
	let ops_err = AtomicU64::new(0);
	let deadline = Instant::now() + Duration::from_secs(RUN_SECS);

	let sessions: Vec<Session> = (0..workers).map(|_| db.root_session()).collect();

	let started = Instant::now();
	thread::scope(|scope| {
		let collector = &collector;
		let ops_ok = &ops_ok;
		let ops_err = &ops_err;
		for (worker, session) in sessions.iter().enumerate() {
			scope.spawn(move || {
				let name = format!("w{worker}");
				while Instant::now() < deadline {
					let call = Instant::now();
					match claim(session, &name) {
						Ok(items) if items.is_empty() => break,
						Ok(items) => {
							ops_ok.fetch_add(1, Ordering::Relaxed);
							record(&mut collector.lock().unwrap(), call.elapsed(), &items);
						}
						Err(err) => {
							ops_err.fetch_add(1, Ordering::Relaxed);
							note_error(&mut collector.lock().unwrap(), err);
						}
					}
				}
			});
		}
	});
	let elapsed = started.elapsed();

	finish(
		"claim_take10",
		workers,
		collector.into_inner().unwrap(),
		ops_ok.into_inner(),
		ops_err.into_inner(),
		ROWS,
		elapsed,
	)
}

fn run_claim_ack_cycle(workers: usize) -> Report {
	let db = database();
	seed(&db, 0, ROWS);

	let collector = Mutex::new(Collector::default());
	let ops_ok = AtomicU64::new(0);
	let ops_err = AtomicU64::new(0);
	let deadline = Instant::now() + Duration::from_secs(RUN_SECS);

	let sessions: Vec<Session> = (0..workers).map(|_| db.root_session()).collect();

	let started = Instant::now();
	thread::scope(|scope| {
		let collector = &collector;
		let ops_ok = &ops_ok;
		let ops_err = &ops_err;
		for (worker, session) in sessions.iter().enumerate() {
			scope.spawn(move || {
				let name = format!("w{worker}");
				while Instant::now() < deadline {
					let call = Instant::now();
					let items = match claim(session, &name) {
						Ok(items) if items.is_empty() => break,
						Ok(items) => items,
						Err(err) => {
							ops_err.fetch_add(1, Ordering::Relaxed);
							note_error(&mut collector.lock().unwrap(), err);
							continue;
						}
					};

					let mut failed = false;
					for (_, token) in &items {
						if let Err(err) = ack(session, token) {
							failed = true;
							ops_err.fetch_add(1, Ordering::Relaxed);
							note_error(&mut collector.lock().unwrap(), err);
						}
					}
					if !failed {
						ops_ok.fetch_add(1, Ordering::Relaxed);
					}
					record(&mut collector.lock().unwrap(), call.elapsed(), &items);
				}
			});
		}
	});
	let elapsed = started.elapsed();

	finish(
		"claim_ack_cycle",
		workers,
		collector.into_inner().unwrap(),
		ops_ok.into_inner(),
		ops_err.into_inner(),
		ROWS,
		elapsed,
	)
}

fn run_claim_plus_inserter(workers: usize) -> Report {
	let db = database();
	seed(&db, 0, ROWS);

	let collector = Mutex::new(Collector::default());
	let ops_ok = AtomicU64::new(0);
	let ops_err = AtomicU64::new(0);
	let inserted = AtomicU64::new(0);
	let inserting = AtomicBool::new(true);
	let deadline = Instant::now() + Duration::from_secs(RUN_SECS);

	let sessions: Vec<Session> = (0..workers).map(|_| db.root_session()).collect();
	let inserter = db.root_session();

	let started = Instant::now();
	thread::scope(|scope| {
		let collector = &collector;
		let ops_ok = &ops_ok;
		let ops_err = &ops_err;
		let inserted = &inserted;
		let inserting = &inserting;
		scope.spawn(|| {
			let mut next = ROWS;
			while Instant::now() < deadline {
				match insert(&inserter, next, BATCH as u64) {
					Ok(()) => {
						inserted.fetch_add(BATCH as u64, Ordering::Relaxed);
					}
					Err(err) => {
						ops_err.fetch_add(1, Ordering::Relaxed);
						note_error(&mut collector.lock().unwrap(), err);
					}
				}
				next += BATCH as u64;
			}
			inserting.store(false, Ordering::Release);
		});

		for (worker, session) in sessions.iter().enumerate() {
			scope.spawn(move || {
				let name = format!("c{worker}");
				loop {
					let call = Instant::now();
					match claim(session, &name) {
						Ok(items) if items.is_empty() => {
							if !inserting.load(Ordering::Acquire) {
								break;
							}
							thread::yield_now();
						}
						Ok(items) => {
							ops_ok.fetch_add(1, Ordering::Relaxed);
							record(&mut collector.lock().unwrap(), call.elapsed(), &items);
						}
						Err(err) => {
							ops_err.fetch_add(1, Ordering::Relaxed);
							note_error(&mut collector.lock().unwrap(), err);
						}
					}
				}
			});
		}
	});
	let elapsed = started.elapsed();

	finish(
		"claim_plus_inserter",
		workers,
		collector.into_inner().unwrap(),
		ops_ok.into_inner(),
		ops_err.into_inner(),
		ROWS + inserted.into_inner(),
		elapsed,
	)
}
