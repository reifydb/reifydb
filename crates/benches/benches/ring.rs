// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb::{Database, embedded};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{BenchReport, env_list_u64, env_select, env_u64, latency_histogram};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::context::clock::Clock;
use reifydb_value::value::{Value, duration::Duration as ValueDuration, identity::IdentityId};

set_global_allocator!();

const DEFAULT_CAPS: [u64; 3] = [1_000, 100_000, 1_000_000];
const DEFAULT_COMMITS: u64 = 200;
const DEFAULT_TTL_DRAIN_SECONDS: u64 = 1_800;
const FILL_BATCH: u64 = 1_000;
const DELETE_BATCH: u64 = 10_000;
const TTL_SHORT_MAX_CAP: u64 = 100_000;
const TTL_LONG_SECONDS: u64 = 300;
const EVICT_INTERVAL_MILLIS: i64 = 200;
const EVICT_BATCH_SIZE: u64 = 1_024;
const EVICT_MAX_BATCHES_PER_TICK: u64 = 8;

#[derive(Clone, Copy, PartialEq)]
enum Workload {
	Append,
	Delete,
	Ttl,
}

impl Workload {
	fn label(self) -> &'static str {
		match self {
			Workload::Append => "append",
			Workload::Delete => "delete",
			Workload::Ttl => "ttl",
		}
	}
}

fn build_database() -> Database {
	let db = embedded::memory().build().expect("embedded database builds");
	db.admin_as_root("CREATE NAMESPACE bench", ()).expect("bench namespace is creatable");
	db
}

fn speed_up_evictor(db: &Database) {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut admin = engine.begin_admin(IdentityId::system()).expect("admin txn begins");
	let interval = ValueDuration::from_milliseconds(EVICT_INTERVAL_MILLIS).expect("interval is representable");
	catalog.set_config(&mut admin, ConfigKey::RetentionEvictInterval, Value::Duration(interval))
		.expect("evict interval is settable");
	catalog.set_config(&mut admin, ConfigKey::RetentionEvictBatchSize, Value::Uint8(EVICT_BATCH_SIZE))
		.expect("evict batch size is settable");
	catalog.set_config(
		&mut admin,
		ConfigKey::RetentionEvictMaxBatchesPerTick,
		Value::Uint8(EVICT_MAX_BATCHES_PER_TICK),
	)
	.expect("evict batches per tick is settable");
	admin.commit().expect("evictor config commits");
}

fn create_ring(db: &Database, ring: &str, workload: Workload, cap: u64) {
	let options = match workload {
		Workload::Ttl => {
			let ttl = if cap > TTL_SHORT_MAX_CAP {
				TTL_LONG_SECONDS
			} else {
				1
			};
			format!("time: processing, capacity: {cap}, row: {{ ttl: {ttl}s }}")
		}
		Workload::Append | Workload::Delete => format!("capacity: {cap}"),
	};
	db.admin_as_root(&format!("create ringbuffer bench::{ring} {{ id: int8 }} with {{ {options} }}"), ())
		.expect("bench ring is creatable");
}

fn fill(db: &Database, ring: &str, cap: u64) {
	let mut next = 1;
	while next <= cap {
		let last = (next + FILL_BATCH - 1).min(cap);
		let rows: Vec<String> = (next..=last).map(|id| format!("{{ id: {id} }}")).collect();
		db.command_as_root(&format!("INSERT bench::{ring} [{}]", rows.join(",")), ())
			.expect("bench ring fill must commit");
		next = last + 1;
	}
}

fn delete_upper_half(db: &Database, ring: &str, cap: u64) {
	let mut lower = cap / 2;
	while lower < cap {
		let upper = (lower + DELETE_BATCH).min(cap);
		db.command_as_root(&format!("DELETE bench::{ring} FILTER {{ id > {lower} and id <= {upper} }}"), ())
			.expect("bench ring delete must commit");
		lower = upper;
	}
}

fn ring_is_empty(db: &Database, ring: &str) -> bool {
	let frames = db.query_as_root(&format!("from bench::{ring} take 1"), ()).expect("bench ring is readable");
	frames.iter().all(|frame| frame.to_rows().is_empty())
}

fn drain(db: &Database, ring: &str, cap: u64, drain_seconds: u64) {
	eprintln!("RINGBENCH start ring workload=ttl-drain cap={cap}");
	let started = Clock::Real.instant();
	while !ring_is_empty(db, ring) {
		assert!(
			started.elapsed().as_secs() < drain_seconds,
			"ttl drain of cap {cap} exceeded {drain_seconds}s, so the evictor is not keeping up"
		);
		thread::yield_now();
	}
	eprintln!("RINGBENCH end ring workload=ttl-drain cap={cap}");
	println!("ring workload=ttl-drain cap={cap} drain_ms={}", started.elapsed().as_millis());
}

fn measure(report: &mut BenchReport, db: &Database, workload: Workload, cap: u64, commits: u64, drain_seconds: u64) {
	let ring = format!("r{}_{cap}", workload.label());
	create_ring(db, &ring, workload, cap);
	fill(db, &ring, cap);
	match workload {
		Workload::Append => {}
		Workload::Delete => delete_upper_half(db, &ring, cap),
		Workload::Ttl => drain(db, &ring, cap, drain_seconds),
	}

	let label = format!("ring workload={} cap={cap}", workload.label());
	eprintln!("RINGBENCH start {label}");
	let mut histogram = latency_histogram();
	let started = Clock::Real.instant();
	for id in 0..commits {
		let commit = Clock::Real.instant();
		db.command_as_root(&format!("INSERT bench::{ring} [{{ id: {} }}]", cap + id + 1), ())
			.expect("bench ring insert must commit");
		histogram.record(commit.elapsed().as_nanos() as u64).expect("latency is within bounds");
	}
	let elapsed = started.elapsed();
	eprintln!("RINGBENCH end {label}");
	report.record(&label, commits, elapsed, &histogram);
}

fn main() {
	let caps = env_list_u64("CAPS", &DEFAULT_CAPS);
	assert!(!caps.is_empty(), "CAPS matched no capacity");
	let workloads = env_select(
		"WORKLOADS",
		&[("append", Workload::Append), ("delete", Workload::Delete), ("ttl", Workload::Ttl)],
		&[Workload::Append, Workload::Delete, Workload::Ttl],
	);
	assert!(!workloads.is_empty(), "WORKLOADS matched no known workload");
	let commits = env_u64("COMMITS", DEFAULT_COMMITS);
	assert!(commits > 0, "COMMITS must be at least one");
	let drain_seconds = env_u64("TTL_DRAIN_SECONDS", DEFAULT_TTL_DRAIN_SECONDS);

	let db = build_database();
	speed_up_evictor(&db);
	let mut report = BenchReport::new("ring");
	for &workload in &workloads {
		for &cap in &caps {
			measure(&mut report, &db, workload, cap, commits, drain_seconds);
		}
	}
	report.save();
}
