// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	ops::Range,
	sync::Arc,
	time::Duration as StdDuration,
};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb::{
	Frame, WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_flow_async::{
	context::FlowContext,
	operator::window::operator::{WindowConfig, WindowOperator},
};
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_test_harness::assert::rows;
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::value::{
	Value, datetime::DateTime, digest::Digest, duration::Duration, row_number::RowNumber, value_type::ValueType,
};

const PPM: u32 = 10_000;
const BASE_MS: i64 = 1_000_000;
const SIZE_MS: i64 = 10_000;

const TIMEOUT: StdDuration = StdDuration::from_secs(60);
const SEED: u64 = 0xD16E_5744_0E2E;
const CHUNK: usize = 200;
const PER_STEP: usize = 4;
const STEP_SECS: usize = 5;
const FRAME_STEPS: i64 = 120;
const CAPACITY: usize = 40;

const CALLS: &str = "n: math::count(latency), p50: stats::approx_percentile(latency, 0.5, 0.01), p99: stats::approx_percentile(latency, 0.99, 0.01)";

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}

fn rolling_window(immutable: Option<Duration>, aggregations: &'static [&'static str]) -> Harness<WindowOperator> {
	Harness::new(move |runtime| {
		WindowOperator::new(WindowConfig {
			parent_schema: Some(Columns::empty()),
			operator: OperatorId(1),
			kind: WindowKind::Rolling {
				size: WindowSize::Duration(Duration::from_milliseconds(SIZE_MS).unwrap()),
				lag: None,
				pane: None,
			},
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: aggregations
				.iter()
				.flat_map(|call| parse_expression(call).expect("aggregation parses"))
				.collect(),
			runtime_context: runtime,
			routines: routines(),
			lateness: None,
			immutable,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the window operator must build")
	})
}

fn row(number: u64, slot_ms: i64, value: i64) -> Row {
	let at = DateTime::from_epoch_millis(BASE_MS + slot_ms).expect("a row stamp is representable");
	generator::row(RowNumber(number), 1, value, at)
}

fn frame_percentile(frame: &BTreeMap<u64, (i64, i64)>, p: f64) -> Value {
	let mut digest = Digest::new(ValueType::Int8, PPM).unwrap();
	for (_, value) in frame.values() {
		digest.add_value(&Value::Int8(*value)).unwrap();
	}
	digest.percentile_value(p).unwrap()
}

fn median_of_slot_medians(frame: &BTreeMap<u64, (i64, i64)>) -> Value {
	let mut slots: BTreeMap<i64, Digest> = BTreeMap::new();
	for (slot, value) in frame.values() {
		slots.entry(*slot)
			.or_insert_with(|| Digest::new(ValueType::Int8, PPM).unwrap())
			.add_value(&Value::Int8(*value))
			.unwrap();
	}
	let mut outer = Digest::new(ValueType::Float8, PPM).unwrap();
	for slot in slots.values() {
		outer.add_value(&slot.percentile_value(0.5).unwrap()).unwrap();
	}
	outer.percentile_value(0.5).unwrap()
}

fn published(changes: &[Change], name: &str) -> Value {
	changes.iter()
		.flat_map(|change| change.diffs.iter())
		.filter_map(|diff| diff.post())
		.next_back()
		.unwrap_or_else(|| panic!("the window published no row carrying {name}"))
		.column(name)
		.unwrap_or_else(|| panic!("the published row has no column {name}"))
		.data()
		.get_value(0)
}

fn assert_reads_the_frame(changes: &[Change], frame: &BTreeMap<u64, (i64, i64)>, with_min: bool, step: &str) {
	let answer = frame_percentile(frame, 0.5);
	assert_ne!(
		median_of_slot_medians(frame),
		answer,
		"{step}: the fixture must make a percentile of per-slot percentiles differ from the frame's"
	);
	assert_eq!(published(changes, "p50"), answer, "{step}: p50 must be read from one digest over the whole frame");
	assert_eq!(published(changes, "p99"), frame_percentile(frame, 0.99), "{step}: p99 over the whole frame");
	if with_min {
		let min = frame.values().map(|(_, value)| *value).min().expect("the frame holds rows");
		assert_eq!(published(changes, "lo"), Value::Int8(min), "{step}: min over the whole frame");
	}
}

fn insert_rows(
	harness: &mut Harness<WindowOperator>,
	frame: &mut BTreeMap<u64, (i64, i64)>,
	rows: &[(u64, i64, i64)],
) -> Change {
	frame.extend(rows.iter().map(|(number, slot, value)| (*number, (*slot, *value))));
	harness.apply(generator::insert(rows.iter().map(|(number, slot, value)| row(*number, *slot, *value)).collect()))
		.expect("the window applies an insert")
}

#[test]
fn a_rolling_percentile_is_read_from_one_frame_digest_never_from_per_slot_percentiles() {
	// A percentile of per-slot percentiles differs from the frame's on these rows, so a slot fold must fail.
	let percentiles: &[&str] =
		&["p50: stats::approx_percentile(v, 0.5, 0.01)", "p99: stats::approx_percentile(v, 0.99, 0.01)"];
	let next_to_a_min: &[&str] = &[
		"p50: stats::approx_percentile(v, 0.5, 0.01)",
		"lo: math::min(v)",
		"p99: stats::approx_percentile(v, 0.99, 0.01)",
	];
	let cases: [(Option<Duration>, &'static [&'static str], bool); 2] =
		[(None, percentiles, false), (Some(Duration::from_milliseconds(500).unwrap()), next_to_a_min, true)];
	for (immutable, aggregations, with_min) in cases {
		let path = if with_min {
			"recombine"
		} else {
			"running"
		};
		let mut harness = rolling_window(immutable, aggregations);
		let mut frame: BTreeMap<u64, (i64, i64)> = BTreeMap::new();

		let first = [
			(1, 0, 1),
			(2, 0, 2),
			(3, 0, 3),
			(4, 1_000, 40),
			(5, 1_000, 50),
			(6, 1_000, 60),
			(7, 2_000, 7),
			(8, 2_000, 80),
			(9, 2_000, 90),
		];
		let out = insert_rows(&mut harness, &mut frame, &first);
		assert_reads_the_frame(&[out], &frame, with_min, &format!("{path}: three slots of three rows"));

		let out = harness.apply(generator::remove(vec![row(2, 0, 2)])).expect("the window applies a remove");
		frame.remove(&2);
		assert_reads_the_frame(&[out], &frame, with_min, &format!("{path}: a retraction inside the frame"));

		let out = harness
			.apply(generator::update(vec![(row(6, 1_000, 60), row(6, 1_000, 65))]))
			.expect("the window applies an update");
		frame.insert(6, (1_000, 65));
		assert_reads_the_frame(&[out], &frame, with_min, &format!("{path}: an update inside the frame"));

		let out = insert_rows(&mut harness, &mut frame, &[(10, 3_000, 5), (11, 3_000, 6), (12, 3_000, 8)]);
		assert_reads_the_frame(&[out], &frame, with_min, &format!("{path}: a fourth slot"));

		let expired = harness.settle_timers(BASE_MS + SIZE_MS).expect("the seal timers fire");
		frame.retain(|_, (slot, _)| *slot > 0);
		assert_reads_the_frame(&expired, &frame, with_min, &format!("{path}: the oldest slot evicted"));

		let out =
			harness.apply(generator::remove(vec![row(11, 3_000, 6)])).expect("the window applies a remove");
		frame.remove(&11);
		assert_reads_the_frame(&[out], &frame, with_min, &format!("{path}: a retraction after an eviction"));
	}
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum FrameKind {
	Duration,
	Count,
}

struct View {
	name: &'static str,
	kind: FrameKind,
	with_min: bool,
	with: &'static str,
}

const VIEWS: [View; 3] = [
	View {
		name: "running",
		kind: FrameKind::Duration,
		with_min: false,
		with: "duration: 10m, lateness: 1h",
	},
	View {
		name: "sealed",
		kind: FrameKind::Duration,
		with_min: true,
		with: "duration: 10m, lateness: 1h, immutable: 5m",
	},
	View {
		name: "counted",
		kind: FrameKind::Count,
		with_min: false,
		with: "count: 40",
	},
];

impl View {
	fn calls(&self) -> String {
		if self.with_min {
			format!("{CALLS}, lo: math::min(latency)")
		} else {
			CALLS.to_string()
		}
	}

	fn names(&self) -> Vec<&'static str> {
		if self.with_min {
			vec!["n", "p50", "p99", "lo"]
		} else {
			vec!["n", "p50", "p99"]
		}
	}
}

#[derive(Default)]
struct Model {
	table: BTreeMap<usize, (i32, Option<f64>)>,
	counted: BTreeMap<i32, BTreeSet<usize>>,
	newest_step: usize,
	reentries: usize,
}

impl Model {
	fn commit(&mut self, events: BTreeMap<(i32, usize), Vec<bool>>) {
		for ((group, id), adds) in events {
			let frame = self.counted.entry(group).or_default();
			let mut held = usize::from(frame.remove(&id));
			let mut touched = false;
			let mut reentered = false;
			for add in adds {
				if add {
					reentered |= held == 0 && frame.first().is_some_and(|oldest| *oldest > id);
					held += 1;
					touched = true;
				} else if held > 0 {
					held -= 1;
					touched = true;
				}
			}
			if held > 0 {
				frame.insert(id);
			}
			if touched {
				while frame.len() > CAPACITY {
					frame.pop_first();
				}
			}
			if reentered && frame.contains(&id) {
				self.reentries += 1;
			}
		}
	}

	fn count_frame_rows(&self) -> Vec<(usize, i32, Option<f64>)> {
		self.counted
			.iter()
			.flat_map(|(group, ids)| ids.iter().map(move |id| (*id, *group)))
			.map(|(id, group)| (id, group, self.table[&id].1))
			.collect()
	}
}

fn runtime() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

fn literal(latency: Option<f64>) -> String {
	match latency {
		Some(value) => format!("{value:?}"),
		None => "none".to_string(),
	}
}

fn latency(rng: &mut StdRng) -> Option<f64> {
	match rng.random_range(0..10) {
		0 => None,
		1 => Some(0.0),
		_ => Some(rng.random_range(-4_000..400_000i64) as f64 * 0.25),
	}
}

fn insert(db: &TestDb, model: &mut Model, ids: Range<usize>, rng: &mut StdRng) {
	let rows: Vec<(usize, i32, Option<f64>)> = ids.map(|id| (id, rng.random_range(1..5), latency(rng))).collect();
	for chunk in rows.chunks(CHUNK) {
		let literals: Vec<String> = chunk
			.iter()
			.map(|(id, group, latency)| {
				let step = id / PER_STEP;
				let secs = step * STEP_SECS;
				format!(
					r#"{{ id: {id}, g: {group}, step: {step}, ts: "2026-01-01T{:02}:{:02}:{:02}Z", latency: {} }}"#,
					secs / 3_600,
					secs / 60 % 60,
					secs % 60,
					literal(*latency)
				)
			})
			.collect();
		db.command(&format!("INSERT app::t [{}]", literals.join(", ")));
		let mut events: BTreeMap<(i32, usize), Vec<bool>> = BTreeMap::new();
		for (id, group, latency) in chunk {
			model.table.insert(*id, (*group, *latency));
			model.newest_step = model.newest_step.max(id / PER_STEP);
			events.entry((*group, *id)).or_default().push(true);
		}
		model.commit(events);
	}
}

fn delete(db: &TestDb, model: &mut Model, ids: Range<usize>) {
	db.command(&format!("DELETE app::t FILTER {{ id >= {} and id < {} }}", ids.start, ids.end));
	let mut events: BTreeMap<(i32, usize), Vec<bool>> = BTreeMap::new();
	for id in ids {
		if let Some((group, _)) = model.table.remove(&id) {
			events.entry((group, id)).or_default().push(false);
		}
	}
	model.commit(events);
}

fn update_latency(db: &TestDb, model: &mut Model, ids: Range<usize>, latency: Option<f64>) {
	db.command(&format!(
		"UPDATE app::t {{ latency: {} }} FILTER {{ id >= {} and id < {} }}",
		literal(latency),
		ids.start,
		ids.end
	));
	let mut events: BTreeMap<(i32, usize), Vec<bool>> = BTreeMap::new();
	for id in ids {
		if let Some((group, held)) = model.table.get_mut(&id) {
			*held = latency;
			events.entry((*group, id)).or_default().extend([false, true]);
		}
	}
	model.commit(events);
}

fn move_group(db: &TestDb, model: &mut Model, ids: Range<usize>, from: i32, to: i32) {
	db.command(&format!(
		"UPDATE app::t {{ g: {to} }} FILTER {{ g == {from} and id >= {} and id < {} }}",
		ids.start, ids.end
	));
	let mut events: BTreeMap<(i32, usize), Vec<bool>> = BTreeMap::new();
	for id in ids {
		if let Some((group, _)) = model.table.get_mut(&id)
			&& *group == from
		{
			*group = to;
			events.entry((from, id)).or_default().push(false);
			events.entry((to, id)).or_default().push(true);
		}
	}
	model.commit(events);
}

fn keyed(frames: &[Frame], values: &[&str]) -> Vec<(String, Vec<Value>)> {
	let mut out: Vec<(String, Vec<Value>)> = rows(frames)
		.into_iter()
		.map(|row| {
			let cell = |name: &str| {
				row.iter()
					.find(|(column, _)| column == name)
					.map(|(_, value)| value.clone())
					.unwrap_or_else(|| panic!("row has no column {name}: {row:?}"))
			};
			(cell("g").to_string(), values.iter().map(|name| cell(name)).collect())
		})
		.collect();
	out.sort_by(|a, b| a.0.cmp(&b.0));
	out
}

fn declare(db: &TestDb, kind: FrameKind) {
	db.admin("CREATE NAMESPACE app");
	db.admin(
		"CREATE TABLE app::t { id: int4, g: int4, step: int4, ts: datetime, latency: Option(float8) } with { time: event(ts) }",
	);
	db.admin("CREATE TABLE app::frame { id: int4, g: int4, latency: Option(float8) }");
	for view in VIEWS.iter().filter(|view| view.kind == kind) {
		let columns = view
			.names()
			.iter()
			.map(|name| match *name {
				"n" => "n: int8".to_string(),
				name => format!("{name}: Option(float8)"),
			})
			.collect::<Vec<_>>()
			.join(", ");
		db.admin(&format!(
			"CREATE DEFERRED VIEW app::{} {{ g: int4, {columns} }} AS {{ FROM app::t | window rolling {{ {} }} with {{ {} }} by {{ g }} }}",
			view.name,
			view.calls(),
			view.with
		));
	}
}

fn frame_source(db: &TestDb, model: &Model, kind: FrameKind) -> String {
	match kind {
		FrameKind::Duration => {
			format!("FROM app::t | filter {{ step > {} }}", model.newest_step as i64 - FRAME_STEPS)
		}
		FrameKind::Count => {
			db.command("DELETE app::frame FILTER { id >= 0 }");
			let rows: Vec<String> = model
				.count_frame_rows()
				.iter()
				.map(|(id, group, latency)| {
					format!("{{ id: {id}, g: {group}, latency: {} }}", literal(*latency))
				})
				.collect();
			db.command(&format!("INSERT app::frame [{}]", rows.join(", ")));
			"FROM app::frame".to_string()
		}
	}
}

fn assert_views_match_the_frame(db: &TestDb, model: &Model, kind: FrameKind, step: &str) {
	assert!(db.await_all_flows(TIMEOUT), "{step}: the flows must process every change");
	let source = frame_source(db, model, kind);
	for view in VIEWS.iter().filter(|view| view.kind == kind) {
		let names = view.names();
		let batch = format!("{source} | aggregate {{ {} }} by {{ g }}", view.calls());
		let expected = keyed(&db.query(&batch), &names);
		assert!(!expected.is_empty(), "{step}: the batch query {batch} must not be empty");
		let whole =
			keyed(&db.query(&format!("FROM app::t | aggregate {{ {} }} by {{ g }}", view.calls())), &names);
		assert_ne!(expected, whole, "{step}: the frame must have dropped rows, or eviction goes untested");
		let got = await_value(expected.clone(), TIMEOUT, || {
			keyed(&db.query(&format!("FROM app::{}", view.name)), &names)
		});
		assert_eq!(got, expected, "{step}: app::{} must equal {batch}", view.name);
	}
}

fn rolling_lifecycle(kind: FrameKind) {
	let db = memory();
	declare(&db, kind);
	let mut model = Model::default();
	let mut rng = StdRng::seed_from_u64(SEED);

	insert(&db, &mut model, 0..800, &mut rng);
	assert_views_match_the_frame(&db, &model, kind, "insert the first half");

	insert(&db, &mut model, 800..1600, &mut rng);
	assert_views_match_the_frame(&db, &model, kind, "insert the second half");

	delete(&db, &mut model, 1500..1540);
	delete(&db, &mut model, 300..340);
	assert_views_match_the_frame(&db, &model, kind, "delete inside and outside the frame");

	update_latency(&db, &mut model, 1560..1580, Some(123456.5));
	update_latency(&db, &mut model, 1400..1420, Some(-7.25));
	update_latency(&db, &mut model, 1000..1020, Some(3.5));
	assert_views_match_the_frame(&db, &model, kind, "update inside, at the edge of and outside the frame");

	update_latency(&db, &mut model, 1450..1470, None);
	move_group(&db, &mut model, 1580..1600, 1, 2);
	assert_views_match_the_frame(&db, &model, kind, "update to none and move rows into another group");

	insert(&db, &mut model, 1600..2000, &mut rng);
	assert_views_match_the_frame(&db, &model, kind, "insert past the frame");

	if kind == FrameKind::Count {
		assert!(model.reentries > 0, "an evicted row updated back into a short count frame must be exercised");
	}
}

#[test]
fn a_duration_rolling_percentile_view_equals_batch_over_the_frame_through_inserts_deletes_and_updates() {
	// A running digest that misses an eviction or a retraction answers from rows the frame no longer holds.
	rolling_lifecycle(FrameKind::Duration);
}

#[test]
fn a_count_rolling_percentile_view_equals_batch_over_the_rows_it_holds_through_inserts_deletes_and_updates() {
	// A digest that misses a capacity eviction or a re-entry answers from rows the count frame does not hold.
	rolling_lifecycle(FrameKind::Count);
}
