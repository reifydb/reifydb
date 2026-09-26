// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{GlobalAlloc, Layout},
	hint::black_box,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering::Relaxed},
	},
};

use arrow_buffer::BooleanBuffer;
use reifydb::{Database, embedded};
use reifydb_allocator::backend::ALLOCATOR as BACKEND;
use reifydb_benches::{BenchReport, env_flag, env_usize};
use reifydb_column::{
	reader::SnapshotReader,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	builder::ColumnBuilder,
	columns::Columns,
	data::{Column, canonical::Canonical},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_value::value::{
	Value, datetime::DateTime, decimal::Decimal, duration::Duration, system_columns::SystemColumn, uuid::Uuid7,
	value_type::ValueType,
};
use uuid::Uuid;

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

fn count(size: usize) {
	ALLOCATIONS.fetch_add(1, Relaxed);
	ALLOCATED_BYTES.fetch_add(size as u64, Relaxed);
}

// SAFETY: every method forwards its arguments unchanged to jemalloc, which upholds the GlobalAlloc contract.
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		count(layout.size());
		// SAFETY: the caller's layout guarantees are passed to jemalloc unchanged.
		unsafe { BACKEND.alloc(layout) }
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		count(layout.size());
		// SAFETY: the caller's layout guarantees are passed to jemalloc unchanged.
		unsafe { BACKEND.alloc_zeroed(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		// SAFETY: ptr was returned by this allocator for exactly this layout, as the caller guarantees.
		unsafe { BACKEND.dealloc(ptr, layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		count(new_size);
		// SAFETY: ptr, layout and new_size satisfy the realloc contract, as the caller guarantees.
		unsafe { BACKEND.realloc(ptr, layout, new_size) }
	}
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const DEFAULT_ROWS: usize = 1_000_000;
const DEFAULT_REPEATS: usize = 5;
const CHUNK_ROWS: usize = 65_536;
const SCAN_BATCH: usize = 1_024;
const CONCAT_BATCHES: usize = 16;
const INSERT_BATCH: usize = 1_000;
const GROUPS: u64 = 1_000;

struct Sample {
	elapsed: Duration,
	allocations: u64,
	bytes: u64,
}

fn measure<S, T>(repeats: usize, mut setup: impl FnMut() -> S, mut run: impl FnMut(S) -> T) -> Sample {
	let mut samples: Vec<Sample> = (0..repeats)
		.map(|_| {
			let input = setup();
			let allocations = ALLOCATIONS.load(Relaxed);
			let bytes = ALLOCATED_BYTES.load(Relaxed);
			let started = Clock::Real.instant();
			let output = run(input);
			let elapsed = started.elapsed().into();
			let sample = Sample {
				elapsed,
				allocations: ALLOCATIONS.load(Relaxed) - allocations,
				bytes: ALLOCATED_BYTES.load(Relaxed) - bytes,
			};
			drop(black_box(output));
			sample
		})
		.collect();
	samples.sort_by_key(|sample| sample.elapsed);
	samples.swap_remove(samples.len() / 2)
}

fn record(report: &mut BenchReport, label: &str, rows: usize, sample: Sample) {
	let label = format!("{label} allocs={} alloc_bytes={}", sample.allocations, sample.bytes);
	report.record_throughput(&label, rows as u64, sample.elapsed.to_std());
}

fn mix(i: usize) -> u64 {
	let mut z = (i as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
	z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
	z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
	z ^ (z >> 31)
}

fn unit(i: usize) -> f64 {
	(mix(i) >> 11) as f64 / (1u64 << 53) as f64
}

fn group(i: usize) -> i32 {
	(mix(i) % GROUPS) as i32
}

fn name(i: usize) -> String {
	format!("user_{i}")
}

fn maybe(i: usize) -> Option<i32> {
	(!i.is_multiple_of(7)).then_some(i as i32)
}

fn uuid7(i: usize) -> Uuid7 {
	Uuid7(Uuid::from_u128(((i as u128 + 1) << 80) | (0x7 << 76) | (0x2 << 62)))
}

fn value_sets(rows: usize) -> Vec<(&'static str, ValueType, Vec<Value>)> {
	let values = |f: fn(usize) -> Value| (0..rows).map(f).collect::<Vec<_>>();
	vec![
		("bool", ValueType::Boolean, values(|i| Value::Boolean(mix(i) & 1 == 0))),
		("int4", ValueType::Int4, values(|i| Value::Int4(mix(i) as i32))),
		("int8", ValueType::Int8, values(|i| Value::Int8(mix(i) as i64))),
		("float8", ValueType::Float8, values(|i| Value::float8(unit(i)))),
		(
			"datetime",
			ValueType::DateTime,
			values(|i| {
				Value::DateTime(DateTime::from_nanos(
					i64::try_from(mix(i) >> 2).expect("bench datetime fits in i64 nanos"),
				))
			}),
		),
		("uuid7", ValueType::Uuid7, values(|i| Value::Uuid7(uuid7(i)))),
		("utf8", ValueType::Utf8, values(|i| Value::Utf8(name(i)))),
		("decimal", ValueType::DECIMAL, values(|i| Value::Decimal(Decimal::from((mix(i) % 1_000_000) as i64)))),
		(
			"option_int4",
			ValueType::Option(Box::new(ValueType::Int4)),
			values(|i| maybe(i).map(Value::Int4).unwrap_or_else(|| Value::none_of(ValueType::Int4))),
		),
	]
}

fn columns_fixture(start: usize, end: usize) -> Columns {
	let rows = start..end;
	Columns::new(vec![
		ColumnWithName::new("id", ColumnBuffer::int8(rows.clone().map(|i| i as i64))),
		ColumnWithName::new("g", ColumnBuffer::int4(rows.clone().map(group))),
		ColumnWithName::new("v", ColumnBuffer::float8(rows.clone().map(unit))),
		ColumnWithName::new("name", ColumnBuffer::utf8(rows.clone().map(name))),
		ColumnWithName::new("maybe", ColumnBuffer::int4_optional(rows.clone().map(maybe))),
		ColumnWithName::new("uid", ColumnBuffer::uuid7(rows.map(uuid7))),
	])
}

fn snapshot_block(rows: usize) -> Arc<ColumnBlock> {
	let schema = vec![
		(SystemColumn::RowNumbers.name().to_string(), ValueType::Uint8, false),
		("id".to_string(), ValueType::Int8, false),
		("g".to_string(), ValueType::Int4, false),
		("v".to_string(), ValueType::Float8, false),
		("name".to_string(), ValueType::Utf8, false),
		("maybe".to_string(), ValueType::Int4, true),
	];
	let mut per_column: Vec<Vec<Column>> = vec![Vec::new(); schema.len()];
	for start in (0..rows).step_by(CHUNK_ROWS) {
		let chunk = start..(start + CHUNK_ROWS).min(rows);
		let buffers = [
			ColumnBuffer::uint8(chunk.clone().map(|i| i as u64 + 1)),
			ColumnBuffer::int8(chunk.clone().map(|i| i as i64)),
			ColumnBuffer::int4(chunk.clone().map(group)),
			ColumnBuffer::float8(chunk.clone().map(unit)),
			ColumnBuffer::utf8(chunk.clone().map(name)),
			ColumnBuffer::int4_optional(chunk.map(maybe)),
		];
		for (column, buffer) in per_column.iter_mut().zip(buffers) {
			column.push(Column::from_canonical(Canonical::from_buffer(buffer)));
		}
	}
	let columns = schema
		.iter()
		.zip(per_column)
		.map(|((_, ty, nullable), parts)| ColumnChunks::new(ty.clone(), *nullable, parts))
		.collect();
	Arc::new(ColumnBlock::new(Arc::new(schema), columns))
}

fn bench_push(report: &mut BenchReport, rows: usize, repeats: usize) {
	for (label, ty, values) in value_sets(rows) {
		let sample = measure(
			repeats,
			|| values.clone(),
			|values| {
				let mut builder = ColumnBuilder::with_capacity(ty.clone(), rows);
				for value in values {
					builder.push_value(value);
				}
				builder.finish()
			},
		);
		record(report, &format!("push_value/{label}"), rows, sample);
	}

	let sample = measure(
		repeats,
		|| (),
		|()| {
			let mut builder = ColumnBuilder::with_capacity(ValueType::Int4, rows);
			for i in 0..rows {
				builder.push(mix(i) as i32);
			}
			builder.finish()
		},
	);
	record(report, "push/int4", rows, sample);

	let names: Vec<String> = (0..rows).map(name).collect();
	let sample = measure(
		repeats,
		|| (),
		|()| {
			let mut builder = ColumnBuilder::with_capacity(ValueType::Utf8, rows);
			for name in &names {
				builder.push(name.as_str());
			}
			builder.finish()
		},
	);
	record(report, "push/utf8", rows, sample);
}

fn bench_columns(report: &mut BenchReport, rows: usize, repeats: usize) {
	let columns = columns_fixture(0, rows);

	let mask = BooleanBuffer::collect_bool(rows, |i| mix(i) & 1 == 0);
	let sample = measure(
		repeats,
		|| columns.clone(),
		|mut columns| {
			columns.filter(&mask).expect("filter keeps column lengths aligned");
			columns
		},
	);
	record(report, "filter/half", rows, sample);

	let indices: Vec<usize> = (0..rows / 10).map(|i| (mix(i) % rows as u64) as usize).collect();
	let sample = measure(repeats, || (), |()| columns.extract_by_indices(&indices));
	record(report, "take/random_tenth", indices.len(), sample);

	let per_batch = rows.div_ceil(CONCAT_BATCHES);
	let batches: Vec<Columns> = (0..rows)
		.step_by(per_batch)
		.map(|start| columns_fixture(start, (start + per_batch).min(rows)))
		.collect();
	let sample = measure(
		repeats,
		|| batches.clone(),
		|batches| Columns::concat(batches).expect("batches share one shape"),
	);
	record(report, "concat/16", rows, sample);

	let block = snapshot_block(rows);
	let sample = measure(
		repeats,
		|| (),
		|()| {
			let mut seen = 0usize;
			for batch in SnapshotReader::new(Arc::clone(&block), SCAN_BATCH) {
				seen += batch.expect("snapshot batch materializes").row_count();
			}
			assert_eq!(seen, rows, "snapshot scan must return every row");
			seen
		},
	);
	record(report, "snapshot_scan/full", rows, sample);
}

fn insert_statement(start: usize, end: usize) -> String {
	let rows: Vec<String> = (start..end)
		.map(|i| {
			let maybe = maybe(i).map(|m| m.to_string()).unwrap_or_else(|| "none".to_string());
			format!(
				"{{ id: {i}, g: {}, v: {:.6}, name: '{}', maybe: {maybe} }}",
				group(i),
				unit(i),
				name(i)
			)
		})
		.collect();
	format!("INSERT bench::facts [{}]", rows.join(", "))
}

fn seeded_database(report: &mut BenchReport, rows: usize) -> Database {
	let db = embedded::memory().build().expect("embedded database builds");
	db.admin_as_root("create namespace bench", ()).expect("namespace is created");
	db.admin_as_root(
		"create table bench::facts { id: int8, g: int4, v: float8, name: utf8, maybe: Option(int4) }",
		(),
	)
	.expect("table is created");
	let statements: Vec<String> = (0..rows)
		.step_by(INSERT_BATCH)
		.map(|start| insert_statement(start, (start + INSERT_BATCH).min(rows)))
		.collect();
	let sample = measure(
		1,
		|| (),
		|()| {
			for statement in &statements {
				db.command_as_root(statement, ()).expect("insert batch commits");
			}
		},
	);
	record(report, "engine/insert", rows, sample);
	db
}

fn bench_engine(report: &mut BenchReport, rows: usize, repeats: usize) {
	let db = seeded_database(report, rows);
	let queries = [
		("engine/scan", "from bench::facts"),
		("engine/filter", "from bench::facts filter v > 0.5"),
		("engine/sort", "from bench::facts sort { v }"),
		("engine/group_by", "from bench::facts aggregate { total: math::sum(v), n: math::count(v) } by { g }"),
		("engine/distinct", "from bench::facts distinct { g }"),
	];
	for (label, rql) in queries {
		let sample = measure(repeats, || (), |()| db.query_as_root(rql, ()).expect("bench query executes"));
		record(report, label, rows, sample);
	}
}

fn main() {
	let rows = env_usize("ROWS", DEFAULT_ROWS);
	let engine_rows = env_usize("ENGINE_ROWS", rows);
	let repeats = env_usize("REPEATS", DEFAULT_REPEATS);
	println!("rows={rows} engine_rows={engine_rows} repeats={repeats}");

	let mut report = BenchReport::new("columns");
	bench_push(&mut report, rows, repeats);
	bench_columns(&mut report, rows, repeats);
	if !env_flag("SKIP_ENGINE") {
		bench_engine(&mut report, engine_rows, repeats);
	}
	report.save();
}
