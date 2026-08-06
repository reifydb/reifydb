// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::disallowed_types)]
use std::time::Duration;
use std::{hint::black_box, time::Instant};

use reifydb::{Database, WithSubsystem, embedded};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{env_opt, env_u64};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_testing_scenario::{query::OperationKind, registry::by_name, scenario::Scenario};
#[cfg(feature = "cow-stats")]
use reifydb_value::util::cowvec::stats;
use reifydb_value::{util::cowvec::CowVec, value::Value};

set_global_allocator!();

const DEFAULT_BATCH_SIZE: u64 = 128;
const DEFAULT_SCALE: u64 = 20_000;
const DEFAULT_ITERATIONS: u64 = 10;
const DEFAULT_WARMUP: u64 = 2;

const MICRO_ROWS: usize = 100_000;
const MICRO_REPEATS: u32 = 50;
const BATCH: usize = 128;

struct Shape {
	name: &'static str,
	rql: String,
}

fn scan_shapes() -> Vec<Shape> {
	vec![
		Shape {
			name: "scan",
			rql: "from bench::users".to_string(),
		},
		Shape {
			name: "filter",
			rql: "from bench::users filter id < 50".to_string(),
		},
		Shape {
			name: "map_project",
			rql: "from bench::users map { id, name }".to_string(),
		},
		Shape {
			name: "map_expr",
			rql: "from bench::users map { id * 2, name }".to_string(),
		},
		Shape {
			name: "filter_map",
			rql: "from bench::users filter id < 5000 map { id, name }".to_string(),
		},
		Shape {
			name: "distinct",
			rql: "from bench::users map { name } distinct { name }".to_string(),
		},
		Shape {
			name: "sort",
			rql: "from bench::users sort { id } take 100".to_string(),
		},
		Shape {
			name: "aggregate",
			rql: "from bench::users aggregate { math::count(id) } by { name }".to_string(),
		},
		Shape {
			name: "aggregate_sum",
			rql: "from bench::users aggregate { math::sum(id) } by { name }".to_string(),
		},
		Shape {
			name: "filter_map_distinct",
			rql: "from bench::users filter id < 5000 map { name } distinct { name }".to_string(),
		},
		Shape {
			name: "take_100",
			rql: "from bench::users take 100".to_string(),
		},
	]
}

fn join_shapes() -> Vec<Shape> {
	vec![
		Shape {
			name: "join",
			rql: "from bench::orders left join { from bench::customers } as customers using (customer_id, customers.id)".to_string(),
		},
		Shape {
			name: "join_filter",
			rql: "from bench::orders filter amount > 5000 left join { from bench::customers } as customers using (customer_id, customers.id)".to_string(),
		},
		Shape {
			name: "join_map",
			rql: "from bench::orders left join { from bench::customers } as customers using (customer_id, customers.id) map { amount }".to_string(),
		},
	]
}

fn seed(db: &Database, scenario: &Scenario, scale: u64) {
	for statement in scenario.setup_statements(scale) {
		let outcome = match statement.kind {
			OperationKind::Admin => db.admin_as_root(&statement.rql, ()),
			OperationKind::Command => db.command_as_root(&statement.rql, ()),
			OperationKind::Query => db.query_as_root(&statement.rql, ()),
		};
		outcome.unwrap_or_else(|e| panic!("scenario setup rejected `{}`: {}", statement.rql, e));
	}
}

fn build(scenario_name: &str, scale: u64, batch_size: u16) -> Database {
	let scenario =
		by_name(scenario_name).unwrap_or_else(|| panic!("scenario '{}' is not registered", scenario_name));
	let db = embedded::memory()
		.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch_size))
		.build()
		.expect("embedded database builds");
	seed(&db, &scenario, scale);
	db
}

#[allow(clippy::disallowed_types)]
fn run(db: &Database, rql: &str, iterations: u64) -> Option<Duration> {
	let started = Instant::now();
	for _ in 0..iterations {
		if let Err(e) = db.query_as_root(rql, ()) {
			if env_opt("SHOW_ERRORS").is_some() {
				println!("  error: {}", e);
			}
			return None;
		}
	}
	Some(started.elapsed())
}

#[cfg(feature = "cow-stats")]
fn measure(db: &Database, shape: &Shape, iterations: u64, warmup: u64) {
	if run(db, &shape.rql, warmup).is_none() {
		println!("{:<20} REJECTED: {}", shape.name, shape.rql);
		return;
	}

	stats::reset();
	let elapsed = run(db, &shape.rql, iterations).expect("shape executes after warmup");
	let s = stats::snapshot();

	let per = |v: u64| v as f64 / iterations as f64;
	let us = (elapsed / iterations as u32).as_micros();
	let mutations = per(s.mutations);
	let clones = per(s.clones);
	let mb = per(s.bytes_cloned) / 1_048_576.0;
	let mean_clone = if s.clones > 0 {
		s.bytes_cloned as f64 / s.clones as f64
	} else {
		0.0
	};

	println!(
		"{:<20} {:>9} {:>13.0} {:>12.0} {:>10.2} {:>11.1} {:>9.0} {:>10.0}",
		shape.name,
		us,
		mutations,
		clones,
		mb,
		mean_clone,
		per(s.copies),
		per(s.elements_copied)
	);
}

#[cfg(not(feature = "cow-stats"))]
fn measure(db: &Database, shape: &Shape, iterations: u64, warmup: u64) {
	if run(db, &shape.rql, warmup).is_none() {
		println!("{:<20} REJECTED: {}", shape.name, shape.rql);
		return;
	}
	let elapsed = run(db, &shape.rql, iterations).expect("shape executes after warmup");
	println!("{:<20} {:>9}", shape.name, (elapsed / iterations as u32).as_micros());
}

fn header() {
	#[cfg(feature = "cow-stats")]
	println!(
		"\n{:<20} {:>9} {:>13} {:>12} {:>10} {:>11} {:>9} {:>10}",
		"shape", "us/query", "make_mut/q", "clones/q", "cloned_MB", "mean_clone_B", "deepcopy", "elems_cp"
	);
	#[cfg(not(feature = "cow-stats"))]
	println!("\n{:<20} {:>9}", "shape", "us/query");
}

fn write_db(with_view: bool, batch_size: u16) -> Database {
	let db = embedded::memory()
		.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch_size))
		.with_flow(|f| f)
		.build()
		.expect("embedded database builds");
	db.admin_as_root("create namespace if not exists flowbench", ()).expect("namespace");
	db.admin_as_root("create table flowbench::src { id: int4, sym: utf8, amount: int4 }", ()).expect("table");
	if with_view {
		db.admin_as_root(
			"create deferred view flowbench::agg { sym: utf8, total: int4 } as { from flowbench::src filter amount > 10 map { sym: sym, total: amount } }",
			(),
		)
		.expect("deferred view");
	}
	db
}

fn insert_rows(db: &Database, rows: u64, per_statement: u64) {
	let mut written = 0;
	while written < rows {
		let n = per_statement.min(rows - written);
		let values: Vec<String> = (0..n)
			.map(|i| {
				let id = written + i;
				format!("{{ id: {}, sym: \"sym_{}\", amount: {} }}", id, id % 64, id % 1000)
			})
			.collect();
		let rql = format!("insert flowbench::src [{}]", values.join(", "));
		db.command_as_root(&rql, ()).expect("insert succeeds");
		written += n;
	}
}

#[cfg(feature = "cow-stats")]
fn write_phase(label: &str, with_view: bool, rows: u64, batch_size: u16) {
	let db = write_db(with_view, batch_size);
	stats::reset();
	let started = Instant::now();
	insert_rows(&db, rows, env_u64("ROWS_PER_STMT", 100));
	let view_rows = if with_view {
		let mut seen = 0;
		for _ in 0..200 {
			seen = db
				.query_as_root("from flowbench::agg", ())
				.map(|f| f.iter().map(|c| c.row_count()).sum::<usize>())
				.unwrap_or(0);
			if seen > 0 {
				break;
			}
			thread::yield_now();
		}
		seen
	} else {
		0
	};
	drop(db);
	let elapsed = started.elapsed();
	let s = stats::snapshot();
	if with_view {
		println!("  (view materialized rows={})", view_rows);
	}

	let per_row = |v: u64| v as f64 / rows as f64;
	let mean_clone = if s.clones > 0 {
		s.bytes_cloned as f64 / s.clones as f64
	} else {
		0.0
	};
	println!(
		"{:<20} {:>9} {:>13.1} {:>12.1} {:>10.2} {:>11.1} {:>9} {:>10}",
		label,
		elapsed.as_millis(),
		per_row(s.mutations),
		per_row(s.clones),
		s.bytes_cloned as f64 / 1_048_576.0,
		mean_clone,
		s.copies,
		s.elements_copied
	);
}

#[cfg(not(feature = "cow-stats"))]
fn write_phase(label: &str, with_view: bool, rows: u64, batch_size: u16) {
	let db = write_db(with_view, batch_size);
	let started = Instant::now();
	insert_rows(&db, rows, env_u64("ROWS_PER_STMT", 100));
	drop(db);
	println!("{:<20} {:>9}", label, started.elapsed().as_millis());
}

fn write_header() {
	#[cfg(feature = "cow-stats")]
	println!(
		"\n{:<20} {:>9} {:>13} {:>12} {:>10} {:>11} {:>9} {:>10}",
		"path", "total_ms", "make_mut/row", "clones/row", "cloned_MB", "mean_clone_B", "deepcopy", "elems_cp"
	);
	#[cfg(not(feature = "cow-stats"))]
	println!("\n{:<20} {:>9}", "path", "total_ms");
}

#[allow(clippy::disallowed_types)]
fn time<F: FnMut()>(mut f: F) -> Duration {
	let started = Instant::now();
	for _ in 0..MICRO_REPEATS {
		f();
	}
	started.elapsed() / MICRO_REPEATS
}

fn row(label: &str, cow: Duration, plain: Duration) {
	let cow_ns = cow.as_secs_f64() * 1e9;
	let plain_ns = plain.as_secs_f64() * 1e9;
	let delta = if plain_ns > 0.0 {
		(cow_ns - plain_ns) / plain_ns * 100.0
	} else {
		0.0
	};
	println!("{:<28} cow={:>12.0}ns plain={:>12.0}ns cow_overhead={:>+8.1}%", label, cow_ns, plain_ns, delta);
}

fn microbenchmarks() {
	println!("\n=== vec-layer microbenchmark (rows={} repeats={}) ===", MICRO_ROWS, MICRO_REPEATS);

	let batches: Vec<Vec<u64>> =
		(0..MICRO_ROWS / BATCH).map(|b| (0..BATCH).map(|i| (b * BATCH + i) as u64).collect()).collect();

	let cow = time(|| {
		let mut v: CowVec<u64> = CowVec::with_capacity(MICRO_ROWS);
		for i in 0..MICRO_ROWS {
			v.push(i as u64);
		}
		black_box(v.len());
	});
	let plain = time(|| {
		let mut v: Vec<u64> = Vec::with_capacity(MICRO_ROWS);
		for i in 0..MICRO_ROWS {
			v.push(i as u64);
		}
		black_box(v.len());
	});
	row("push (pre-sized)", cow, plain);

	let cow = time(|| {
		let mut acc: CowVec<u64> = CowVec::with_capacity(0);
		for batch in &batches {
			acc.extend_from_slice(batch);
		}
		black_box(acc.len());
	});
	let plain = time(|| {
		let mut acc: Vec<u64> = Vec::with_capacity(0);
		for batch in &batches {
			acc.extend_from_slice(batch);
		}
		black_box(acc.len());
	});
	row("extend_from_slice (accum)", cow, plain);

	for elems in [2usize, 16, 128] {
		let src_cow: CowVec<u64> = CowVec::new((0..elems as u64).collect());
		let src_plain: Vec<u64> = (0..elems as u64).collect();
		let reps = 137_478usize;
		let cow = time(|| {
			let mut total = 0;
			for _ in 0..reps {
				total += src_cow.clone().len();
			}
			black_box(total);
		});
		let plain = time(|| {
			let mut total = 0;
			for _ in 0..reps {
				total += src_plain.clone().len();
			}
			black_box(total);
		});
		row(&format!("clone x137k @{}B", elems * 8), cow, plain);
	}
}

fn main() {
	let scale = env_u64("SCALES", DEFAULT_SCALE);
	let iterations = env_u64("ITERATIONS", DEFAULT_ITERATIONS);
	let warmup = env_u64("WARMUP", DEFAULT_WARMUP);
	let batch_size = env_u64("BATCH_SIZE", DEFAULT_BATCH_SIZE) as u16;
	let only = env_opt("SHAPE");

	println!("scale={} iterations={} batch_size={}", scale, iterations, batch_size);

	let scan_db = build("scan", scale, batch_size);
	header();
	for shape in scan_shapes() {
		if only.as_ref().is_some_and(|o| o != shape.name) {
			continue;
		}
		measure(&scan_db, &shape, iterations, warmup);
	}
	drop(scan_db);

	let join_db = build("join", scale, batch_size);
	for shape in join_shapes() {
		if only.as_ref().is_some_and(|o| o != shape.name) {
			continue;
		}
		measure(&join_db, &shape, iterations, warmup);
	}
	drop(join_db);

	let write_rows = env_u64("WRITE_ROWS", 20_000);
	write_header();
	write_phase("write_cdc", false, write_rows, batch_size);
	write_phase("write_cdc_flow", true, write_rows, batch_size);

	microbenchmarks();
}
