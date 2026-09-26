// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb::{
	Frame, SqliteConfig, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, await_value},
};
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::catalog::flow::OperatorId,
	value::column::columns::Columns,
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{
	aggregation::core::{Aggregation, SlotInput},
	window::operator::{WindowConfig, WindowOperator},
};
use reifydb_rql::{
	expression::{Expression, parse_expression},
	flow::aggregate::{AggregateContext, DIGEST_FUNCTION, PERCENTILE_FUNCTION, SlotKind},
};
use reifydb_runtime::{RuntimeConfig, context::clock::Clock, fatal::FatalConfig};
use reifydb_sub_api::subsystem::HealthStatus;
use reifydb_test_harness::{assert::rows, engine::TestEngine};
use reifydb_value::value::{Value, datetime::DateTime, digest::Digest, duration::Duration, value_type::ValueType};

const TIMEOUT: Duration = Duration::from_seconds_const(60);
const ROWS: usize = 1200;
const CHUNK: usize = 400;
const SEED: u64 = 0x5443_0000_D16E_0059;

const FROZEN_CALLS: [(&str, &str); 10] = [
	("p50", "stats::approx_percentile(latency, 0.5, 0.01)"),
	("s", "math::sum(latency)"),
	("p90w", "stats::approx_percentile(latency, 0.9, 0.05)"),
	("q90", "stats::approx_percentile(queue, 0.9, 0.01)"),
	("l50", "stats::approx_percentile(lat, 0.5)"),
	("e99", "stats::approx_percentile(latency - queue, 0.99, 0.01)"),
	("l99", "stats::approx_percentile(lat, 0.99)"),
	("d", "stats::digest(latency, 0.01)"),
	(
		"sum99",
		"stats::approx_percentile(latency, 0.99, 0.01) + stats::approx_percentile(latency - queue, 0.99, 0.01)",
	),
	("p75w", "stats::approx_percentile(latency, 0.75, 0.05)"),
];

const PERCENTILE_CALLS: [(&str, &str); 4] = [
	("p50", "stats::approx_percentile(latency, 0.5, 0.01)"),
	("p99", "stats::approx_percentile(latency, 0.99, 0.01)"),
	("p90w", "stats::approx_percentile(latency, 0.9, 0.05)"),
	("e99", "stats::approx_percentile(latency - queue, 0.99, 0.01)"),
];

type Cases<'a> = Vec<(Vec<(&'a str, &'a str)>, Value, &'a str, &'a str)>;

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

fn sqlite(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_runtime_config(runtime())
			.with_flow(|f| f)
			.build()
			.expect("build sqlite db with flow"),
	)
}

fn map_of(calls: &[(&str, &str)]) -> String {
	calls.iter().map(|(name, call)| format!("{name}: {call}")).collect::<Vec<_>>().join(", ")
}

fn names_of(calls: &[(&'static str, &str)]) -> Vec<&'static str> {
	calls.iter().map(|(name, _)| *name).collect()
}

fn aggregations(calls: &[(&str, &str)]) -> Vec<Expression> {
	calls.iter()
		.flat_map(|(name, call)| parse_expression(&format!("{name}: {call}")).expect("aggregation parses"))
		.collect()
}

fn grouped_core(engine: &TestEngine, calls: &[(&str, &str)]) -> Aggregation {
	Aggregation::new(
		OperatorId(1),
		None,
		Vec::new(),
		aggregations(calls),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		AggregateContext::Grouped,
		Arc::new(FlowContext::default()),
	)
	.expect("the aggregation core must build")
}

fn windowed_core(engine: &TestEngine, calls: &[(&str, &str)]) -> Aggregation {
	WindowOperator::new(WindowConfig {
		parent_schema: None,
		operator: OperatorId(2),
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
		},
		group_by: Vec::new(),
		aggregations: aggregations(calls),
		runtime_context: engine.executor().runtime_context.clone(),
		routines: engine.executor().routines.clone(),
		lateness: None,
		immutable: None,
		ctx: Arc::new(FlowContext::default()),
	})
	.expect("the window operator must build")
	.core
}

fn cores(calls: &[(&str, &str)]) -> Vec<(&'static str, Aggregation)> {
	let engine = TestEngine::new();
	vec![("aggregate", grouped_core(&engine, calls)), ("window", windowed_core(&engine, calls))]
}

fn layout(core: &Aggregation) -> Vec<(SlotKind, String)> {
	let kinds = core.slot_kinds.clone().expect("every call must be representable as a slot");
	kinds.into_iter()
		.zip(core.slot_inputs.iter())
		.map(|(kind, input)| {
			let input = match input {
				SlotInput::Star => "*".to_string(),
				SlotInput::Column(name) => name.clone(),
				SlotInput::Expr(idx) => format!("expression {idx}"),
				SlotInput::EventTime => "event time".to_string(),
			};
			(kind, input)
		})
		.collect()
}

fn digest_slot(ppm: Option<u32>, input: &str) -> (SlotKind, String) {
	(
		SlotKind::Digest {
			accuracy: ppm,
		},
		input.to_string(),
	)
}

fn single(ppm: u32, value: f64) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, ppm).unwrap();
	digest.add_value(&Value::float8(value)).unwrap();
	digest
}

fn read(digest: &Digest, p: f64) -> Value {
	digest.percentile_value(p).unwrap()
}

fn float(value: &Value) -> f64 {
	match value {
		Value::Float8(v) => v.value(),
		other => panic!("expected a float8 percentile, got {other:?}"),
	}
}

#[test]
fn p50_and_p99_on_one_column_share_one_digest_slot_in_every_flow_operator() {
	// A slot per percentile doubles the stored state of every group and changes no answer.
	for (operator, core) in cores(&PERCENTILE_CALLS[..2]) {
		assert_eq!(layout(&core), vec![digest_slot(Some(10_000), "latency")], "{operator}");
		let slot = single(10_000, 42.0);
		let outputs = core.compute_outputs(&[Value::Digest(Box::new(slot.clone()))]).unwrap();
		assert_eq!(outputs, vec![read(&slot, 0.5), read(&slot, 0.99)], "{operator}: both outputs read slot 0");
	}
}

#[test]
fn an_expression_input_and_another_accuracy_get_their_own_digest_slots_in_every_flow_operator() {
	// Sharing across accuracies or expressions would answer from a digest the call never asked for.
	let calls = [
		PERCENTILE_CALLS[0],
		PERCENTILE_CALLS[2],
		PERCENTILE_CALLS[3],
		("e50", "stats::approx_percentile(latency - queue, 0.5, 0.01)"),
		PERCENTILE_CALLS[1],
	];
	for (operator, core) in cores(&calls) {
		assert_eq!(
			layout(&core),
			vec![
				digest_slot(Some(10_000), "latency"),
				digest_slot(Some(50_000), "latency"),
				digest_slot(Some(10_000), "expression 0"),
				digest_slot(Some(10_000), "expression 1"),
			],
			"{operator}"
		);
	}
}

#[test]
fn the_frozen_sharing_rule_maps_a_fixed_call_set_to_a_pinned_slot_layout_in_every_flow_operator() {
	// Flow slot values are stored by position, so any change to this layout reads old state against new slots.
	let slots = [
		single(10_000, 10.0),
		single(10_000, 0.0),
		single(50_000, 200.0),
		single(10_000, 3_000.0),
		single(10_000, 40_000.0),
		single(10_000, 500_000.0),
		single(10_000, 6_000_000.0),
		single(10_000, 70_000_000.0),
	];
	for (operator, core) in cores(&FROZEN_CALLS) {
		assert_eq!(
			layout(&core),
			vec![
				digest_slot(Some(10_000), "latency"),
				(SlotKind::Sum, "latency".to_string()),
				digest_slot(Some(50_000), "latency"),
				digest_slot(Some(10_000), "queue"),
				digest_slot(None, "lat"),
				digest_slot(Some(10_000), "expression 0"),
				digest_slot(Some(10_000), "latency"),
				digest_slot(Some(10_000), "expression 1"),
			],
			"{operator}"
		);
		let mut values: Vec<Value> = slots.iter().map(|slot| Value::Digest(Box::new(slot.clone()))).collect();
		values[1] = Value::float8(7.0);
		let outputs = core.compute_outputs(&values).unwrap();
		assert_eq!(
			outputs,
			vec![
				read(&slots[0], 0.5),
				Value::float8(7.0),
				read(&slots[2], 0.9),
				read(&slots[3], 0.9),
				read(&slots[4], 0.5),
				read(&slots[5], 0.99),
				read(&slots[4], 0.99),
				Value::Digest(Box::new(slots[6].clone())),
				Value::float8(float(&read(&slots[0], 0.99)) + float(&read(&slots[7], 0.99))),
				read(&slots[2], 0.75),
			],
			"{operator}: each output must read its pinned slot"
		);
	}
}

fn create_error(db: &TestDb, call: &str) -> String {
	db.try_admin(&format!(
		"CREATE DEFERRED VIEW app::bad {{ g: int4, p: Option(float8) }} AS {{ FROM app::t | aggregate {{ p: {call} }} by {{ g }} }}"
	))
	.expect_err("a bad percentile call must be refused when the view is created")
	.to_string()
}

#[test]
fn every_bad_percentile_call_is_refused_at_view_create_with_its_code_and_the_function_written() {
	// A bad literal accepted at create only fails on the first row, long after the view went live.
	let db = memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime, weight: float8, latency: Option(float8) }");
	let cases = [
		("stats::approx_percentile(latency)", "FLOW_058", "with 1 arguments"),
		("stats::approx_percentile(latency, 0.5, 0.01, 0.1)", "FLOW_058", "with 4 arguments"),
		("stats::approx_percentile(latency, weight, 0.01)", "FLOW_059", "a p that is not"),
		("stats::approx_percentile(latency, '0.5', 0.01)", "FLOW_059", "a p that is not"),
		("stats::approx_percentile(latency, 1.5, 0.01)", "FLOW_060", "a p outside 0 to 1"),
		("stats::approx_percentile(latency, -0.5, 0.01)", "FLOW_060", "a p outside 0 to 1"),
		("stats::approx_percentile(latency, 1.5, 0.5)", "FLOW_060", "a p outside 0 to 1"),
		("stats::approx_percentile(latency, 0.5, weight)", "FLOW_052", "an accuracy that is not"),
		("stats::approx_percentile(latency, 0.5, '0.01')", "FLOW_052", "an accuracy that is not"),
		("stats::approx_percentile(latency, 0.5, 0.5)", "FLOW_053", "an accuracy outside"),
		("stats::approx_percentile(latency, 0.5, 0.0000005)", "FLOW_054", "not a whole number"),
		("math::sum(latency) + stats::approx_percentile(latency, 2, 0.01)", "FLOW_060", "a p outside 0 to 1"),
	];
	for (call, code, text) in cases {
		let message = create_error(&db, call);
		assert!(message.contains(code), "{call} must be {code}, got: {message}");
		assert!(
			message.contains("aggregate output 'p' passes stats::approx_percentile")
				|| message.contains("aggregate output 'p' calls stats::approx_percentile"),
			"{call} must name the output and the function written, got: {message}"
		);
		assert!(message.contains(text), "{call} must say {text}, got: {message}");
		assert!(!message.contains("stats::digest an"), "{call} must not name the rewritten call: {message}");
	}
	let digest = create_error(&db, "stats::digest(latency, 0.5)");
	assert!(digest.contains("FLOW_053") && digest.contains("passes stats::digest an accuracy"), "{digest}");
	assert!(db.try_query("FROM app::bad").is_err(), "a refused view must not exist");

	let window = db
		.try_admin(
			"CREATE DEFERRED VIEW app::bad { g: int4, p: Option(float8) } AS { FROM app::t | window tumbling { p: stats::approx_percentile(latency, 0.5, 0.2) } by { g } with { duration: 60s } }",
		)
		.expect_err("a bad accuracy in a window must be refused at create")
		.to_string();
	assert!(window.contains("FLOW_053") && window.contains("passes stats::approx_percentile"), "{window}");
}

#[test]
fn a_digest_input_error_at_runtime_names_the_percentile_function_the_user_wrote() {
	// An error naming stats::digest sends a user who wrote stats::approx_percentile after a call that is not there.
	for (operator, core) in cores(&[("p", "stats::approx_percentile(latency, 0.5)")]) {
		let columns = Columns::single_row([("latency", Value::float8(1.5))]);
		let error = core
			.evaluate_slot_inputs(&columns)
			.expect_err("a raw input without an accuracy must be refused");
		assert_eq!(error.0.code, "FLOW_055", "{operator}");
		assert!(
			error.0.message.contains("stats::approx_percentile"),
			"{operator}: the error must name the function written, got: {}",
			error.0.message
		);
	}
}

#[test]
fn every_runtime_digest_input_error_names_the_function_written_for_the_slot_that_failed() {
	// One node mixes both functions, so naming by node or by a fixed slot blames the call that did not fail.
	let float = || Value::float8(1.5);
	let digest = || {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		digest.add_value(&Value::float8(1.5)).unwrap();
		Value::Digest(Box::new(digest))
	};
	let instant = || Value::DateTime(DateTime::from_millis(1_000_000));
	let cases: Cases<'_> = vec![
		(
			vec![("d", "stats::digest(x)"), ("p", "stats::approx_percentile(y, 0.5)")],
			float(),
			"FLOW_055",
			DIGEST_FUNCTION,
		),
		(
			vec![("p", "stats::approx_percentile(x, 0.5)"), ("d", "stats::digest(y)")],
			float(),
			"FLOW_055",
			PERCENTILE_FUNCTION,
		),
		(
			vec![("d", "stats::digest(x, 0.01)"), ("p", "stats::approx_percentile(y, 0.5)")],
			digest(),
			"FLOW_056",
			DIGEST_FUNCTION,
		),
		(
			vec![("p", "stats::approx_percentile(x, 0.5, 0.01)"), ("d", "stats::digest(y)")],
			digest(),
			"FLOW_056",
			PERCENTILE_FUNCTION,
		),
		(
			vec![("d", "stats::digest(x, 0.01)"), ("p", "stats::approx_percentile(y, 0.5)")],
			instant(),
			"FLOW_057",
			DIGEST_FUNCTION,
		),
		(
			vec![("p", "stats::approx_percentile(x, 0.5, 0.01)"), ("d", "stats::digest(y)")],
			instant(),
			"FLOW_057",
			PERCENTILE_FUNCTION,
		),
	];
	for (calls, x, code, function) in cases {
		for (operator, core) in cores(&calls) {
			let columns = Columns::single_row([("x", x.clone()), ("y", digest())]);
			let error = core.evaluate_slot_inputs(&columns).expect_err("the x slot must refuse its input");
			let other = if function == DIGEST_FUNCTION {
				PERCENTILE_FUNCTION
			} else {
				DIGEST_FUNCTION
			};
			assert_eq!(error.0.code, code, "{operator} {calls:?}: {}", error.0.message);
			assert!(
				error.0.message.starts_with(&format!("{function} "))
					&& !error.0.message.contains(other),
				"{operator} {calls:?}: the error must name only {function}, got: {}",
				error.0.message
			);
		}
	}
}

#[test]
fn digest_accuracy_help_shows_the_syntax_of_the_function_the_user_wrote() {
	// The help must show the call the user wrote, otherwise a percentile user is told to write stats::digest.
	let runtime = [
		(
			"stats::approx_percentile(latency, 0.5)",
			"Only a digest input takes no accuracy, because its accuracy comes from its type. Write \
			 stats::approx_percentile(x, 0.99, 0.01) for any other input.",
		),
		(
			"stats::digest(latency)",
			"Only a digest input takes no accuracy, because its accuracy comes from its type. Write \
			 stats::digest(x, 0.01) for any other input.",
		),
	];
	for (call, help) in runtime {
		for (operator, core) in cores(&[("p", call)]) {
			let columns = Columns::single_row([("latency", Value::float8(1.5))]);
			let error = core
				.evaluate_slot_inputs(&columns)
				.expect_err("a raw input without an accuracy must fail");
			assert_eq!(error.0.code, "FLOW_055", "{operator} {call}: {}", error.0.message);
			assert_eq!(error.0.help.as_deref(), Some(help), "{operator} {call}");
		}
	}

	let db = memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, latency: Option(float8) }");
	let create = [
		(
			"stats::approx_percentile(latency, 0.5, '0.01')",
			"The accuracy is part of the digest type, so it is fixed when the view is created: write it as a \
			 number literal, for example stats::approx_percentile(latency, 0.99, 0.01).",
		),
		(
			"stats::digest(latency, '0.01')",
			"The accuracy is part of the digest type, so it is fixed when the view is created: write it as a \
			 number literal, for example stats::digest(latency, 0.01).",
		),
	];
	for (call, help) in create {
		let error = db
			.try_admin(&format!(
				"CREATE DEFERRED VIEW app::bad {{ g: int4, p: Option(float8) }} AS {{ FROM app::t | aggregate {{ p: {call} }} by {{ g }} }}"
			))
			.expect_err("an accuracy that is not a number literal must be refused at create");
		assert_eq!(error.0.code, "FLOW_052", "{call}: {}", error.0.message);
		assert_eq!(error.0.help.as_deref(), Some(help), "{call}");
	}
}

#[derive(Clone, Copy)]
enum Latency {
	Float8,
	Int4,
	Duration,
}

impl Latency {
	fn ddl(self) -> &'static str {
		match self {
			Latency::Float8 => "float8",
			Latency::Int4 => "int4",
			Latency::Duration => "duration",
		}
	}

	fn output(self) -> &'static str {
		match self {
			Latency::Duration => "duration",
			_ => "float8",
		}
	}

	fn random(self, rng: &mut StdRng) -> String {
		if rng.random_range(0..10) == 0 {
			return "none".to_string();
		}
		match self {
			Latency::Float8 => {
				let magnitude =
					rng.random_range(1..1_000_000i64) as f64 / 10f64.powi(rng.random_range(0..5));
				let sign = if rng.random_range(0..4) == 0 {
					-1.0
				} else {
					1.0
				};
				format!("{:?}", sign * magnitude)
			}
			Latency::Int4 => rng.random_range(-50_000..1_000_000i32).to_string(),
			Latency::Duration => format!("{}ms", rng.random_range(0..10_000_000i64)),
		}
	}

	fn moved(self) -> &'static str {
		match self {
			Latency::Float8 => "123456.5",
			Latency::Int4 => "-777",
			Latency::Duration => "42ms",
		}
	}
}

fn declare_percentile_views(db: &TestDb, latency: Latency) {
	let (ty, out) = (latency.ddl(), latency.output());
	let columns =
		names_of(&PERCENTILE_CALLS).iter().map(|name| format!("{name}: Option({out})")).collect::<Vec<_>>();
	let columns = columns.join(", ");
	let map = map_of(&PERCENTILE_CALLS);
	db.admin("CREATE NAMESPACE app");
	db.admin(&format!(
		"CREATE TABLE app::t {{ id: int4, g: int4, ws: datetime, ts: datetime, latency: Option({ty}), queue: Option({ty}) }} with {{ time: event(ts) }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::grouped {{ g: int4, {columns} }} AS {{ FROM app::t | aggregate {{ {map} }} by {{ g }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::whole {{ {columns} }} AS {{ FROM app::t | aggregate {{ {map} }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::windowed {{ g: int4, s: datetime, {columns} }} AS {{ FROM app::t | window tumbling {{ s: window::start(), {map} }} by {{ g }} with {{ duration: 60s, lateness: 3600s }} }}"
	));
}

fn corpus(latency: Latency) -> Vec<String> {
	let mut rng = StdRng::seed_from_u64(SEED);
	(0..ROWS)
		.map(|id| {
			let g = rng.random_range(1..5);
			let w = rng.random_range(0..3);
			let second = rng.random_range(0..60);
			let (value, queue) = (latency.random(&mut rng), latency.random(&mut rng));
			format!(
				r#"{{ id: {id}, g: {g}, ws: "2026-01-01T00:{w:02}:00Z", ts: "2026-01-01T00:{w:02}:{second:02}Z", latency: {value}, queue: {queue} }}"#
			)
		})
		.collect()
}

fn insert(db: &TestDb, rows: &[String]) {
	for chunk in rows.chunks(CHUNK) {
		db.command(&format!("INSERT app::t [{}]", chunk.join(", ")));
	}
}

fn keyed(frames: &[Frame], keys: &[&str], values: &[&str]) -> Vec<(Vec<String>, Vec<Value>)> {
	let mut out: Vec<(Vec<String>, Vec<Value>)> = rows(frames)
		.into_iter()
		.map(|row| {
			let cell = |name: &str| {
				row.iter()
					.find(|(column, _)| column == name)
					.map(|(_, value)| value.clone())
					.unwrap_or_else(|| panic!("row has no column {name}: {row:?}"))
			};
			(
				keys.iter().map(|key| cell(key).to_string()).collect(),
				values.iter().map(|v| cell(v)).collect(),
			)
		})
		.collect();
	out.sort_by(|a, b| a.0.cmp(&b.0));
	out
}

fn assert_percentile_views_match_batch(db: &TestDb, step: &str) {
	assert!(db.await_all_flows(TIMEOUT), "{step}: the flows must process every change");
	let names = names_of(&PERCENTILE_CALLS);
	let map = map_of(&PERCENTILE_CALLS);
	let pairs: [(&str, &[&str], String, &[&str]); 3] = [
		("FROM app::grouped", &["g"], format!("FROM app::t | aggregate {{ {map} }} by {{ g }}"), &["g"]),
		("FROM app::whole", &[], format!("FROM app::t | aggregate {{ {map} }}"), &[]),
		(
			"FROM app::windowed",
			&["s", "g"],
			format!("FROM app::t | aggregate {{ {map} }} by {{ ws, g }}"),
			&["ws", "g"],
		),
	];
	for (view, view_keys, batch, batch_keys) in &pairs {
		let expected = keyed(&db.query(batch), batch_keys, &names);
		assert!(!expected.is_empty(), "{step}: the batch query {batch} must not be empty");
		let got = await_value(expected.clone(), TIMEOUT, || keyed(&db.query(view), view_keys, &names));
		assert_eq!(got, expected, "{step}: {view} must read the same percentiles as {batch}");
		let frames = db.query(view);
		assert!(
			frames[0].columns.iter().all(|column| !column.name.starts_with("__aggregate")),
			"{step}: {view} must not expose a slot column"
		);
	}
}

fn percentile_lifecycle(latency: Latency) {
	let db = memory();
	declare_percentile_views(&db, latency);
	let rows = corpus(latency);

	insert(&db, &rows);
	assert_percentile_views_match_batch(&db, "insert");

	db.command("DELETE app::t FILTER { id >= 100 and id < 400 }");
	assert_percentile_views_match_batch(&db, "delete a range");

	db.command(&format!("UPDATE app::t {{ latency: {} }} FILTER {{ id >= 400 and id < 500 }}", latency.moved()));
	assert_percentile_views_match_batch(&db, "update values within their groups");

	db.command("UPDATE app::t { latency: none } FILTER { id >= 500 and id < 550 }");
	assert_percentile_views_match_batch(&db, "update values to none");

	db.command("UPDATE app::t { g: 2 } FILTER { g == 1 and id >= 600 and id < 1000 }");
	assert_percentile_views_match_batch(&db, "update rows into another group");
}

#[test]
fn a_float8_percentile_view_equals_batch_through_inserts_deletes_and_updates() {
	// A shared slot that drifts on retraction gives every percentile read from it a wrong answer.
	percentile_lifecycle(Latency::Float8);
}

#[test]
fn an_int4_percentile_view_equals_batch_through_inserts_deletes_and_updates() {
	// An int4 input must read back as float8 percentiles exactly as the batch query reads them.
	percentile_lifecycle(Latency::Int4);
}

#[test]
fn a_duration_percentile_view_equals_batch_through_inserts_deletes_and_updates() {
	// A duration input must read back as duration percentiles, otherwise the view column loses its unit.
	percentile_lifecycle(Latency::Duration);
}

fn declare_frozen_view(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, latency: Option(float8), queue: Option(float8) }");
	db.admin(
		"CREATE DEFERRED VIEW app::src { id: int4, g: int4, latency: Option(float8), queue: Option(float8), lat: Option(digest(float8, 0.01)) } AS { FROM app::t | aggregate { latency: math::max(latency), queue: math::max(queue), lat: stats::digest(latency, 0.01) } by { id, g } }",
	);
	let columns = FROZEN_CALLS
		.iter()
		.map(|(name, _)| match *name {
			"d" => "d: Option(digest(float8, 0.01))".to_string(),
			name => format!("{name}: Option(float8)"),
		})
		.collect::<Vec<_>>()
		.join(", ");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::frozen {{ g: int4, {columns} }} AS {{ FROM app::src | aggregate {{ {} }} by {{ g }} }}",
		map_of(&FROZEN_CALLS)
	));
}

fn quarter(rng: &mut StdRng) -> String {
	// Quarters keep every partial sum exact, otherwise sum order alone makes a view sum differ from batch.
	match rng.random_range(0..10) {
		0 => "none".to_string(),
		_ => format!("{:?}", rng.random_range(-4_000_000..40_000_000i64) as f64 * 0.25),
	}
}

fn frozen_rows(from: usize, to: usize) -> Vec<String> {
	let mut rng = StdRng::seed_from_u64(SEED ^ 0x1D);
	let mut out: Vec<String> = (0..ROWS)
		.map(|id| {
			let g = rng.random_range(1..4);
			let (value, queue) = (quarter(&mut rng), quarter(&mut rng));
			format!("{{ id: {id}, g: {g}, latency: {value}, queue: {queue} }}")
		})
		.collect();
	out.drain(from..to).collect()
}

fn poisoned_before_flows_catch_up(db: &TestDb) -> Option<String> {
	let deadline = Clock::Real.instant() + TIMEOUT;
	loop {
		if db.await_all_flows(Duration::from_milliseconds(100).unwrap()) {
			return None;
		}
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status
			&& description.contains("poisoned")
		{
			return Some(description.clone());
		}
		assert!(
			Clock::Real.instant() < deadline,
			"the flows must either catch up or fail, last status: {status:?}"
		);
	}
}

fn frozen_answers(db: &TestDb, rql: &str) -> Vec<(Vec<String>, Vec<Value>)> {
	keyed(&db.query(rql), &["g"], &names_of(&FROZEN_CALLS))
}

fn assert_frozen_view_matches_batch(db: &TestDb, step: &str) -> Vec<(Vec<String>, Vec<Value>)> {
	let batch = format!("FROM app::src | aggregate {{ {} }} by {{ g }}", map_of(&FROZEN_CALLS));
	let source_caught_up =
		await_value(true, TIMEOUT, || db.row_count("FROM app::src") == db.row_count("FROM app::t"));
	assert!(source_caught_up, "{step}: the source view must hold one row per source row");
	let expected = frozen_answers(db, &batch);
	assert!(!expected.is_empty(), "{step}: the batch query must not be empty");
	let got = await_value(expected.clone(), TIMEOUT, || frozen_answers(db, "FROM app::frozen"));
	assert_eq!(got, expected, "{step}: the frozen view must equal the batch query over the same rows");
	got
}

#[test]
fn a_fixed_call_set_keeps_its_slot_layout_and_answers_across_a_restart() {
	// Slot state is stored by position, so a layout rebuilt differently on reopen reads old state as other slots.
	let path = TempDbPath::new("percentile_view_frozen_restart");
	let before = {
		let mut db = sqlite(&path);
		declare_frozen_view(&db);
		db.command(&format!("INSERT app::t [{}]", frozen_rows(0, ROWS / 2).join(", ")));
		assert!(db.await_all_flows(TIMEOUT), "precondition: the flows catch up before the restart");
		let before = assert_frozen_view_matches_batch(&db, "before the restart");
		db.stop();
		before
	};

	let mut db = sqlite(&path);
	let reopened = await_value(before.clone(), TIMEOUT, || frozen_answers(&db, "FROM app::frozen"));
	assert_eq!(reopened, before, "the reopened view must read back the answers stored before the restart");
	db.command(&format!("INSERT app::t [{}]", frozen_rows(ROWS / 2, ROWS).join(", ")));
	db.command("DELETE app::t FILTER { id >= 100 and id < 400 }");
	db.command("UPDATE app::t { latency: 99.5, queue: none } FILTER { id >= 400 and id < 450 }");
	assert!(db.await_all_flows(TIMEOUT), "the flows must process the changes after the restart");
	assert_frozen_view_matches_batch(&db, "retracting rows stored before the restart");
	db.stop();
}

#[test]
fn a_reopened_fixed_call_set_given_only_inserts_equals_batch_as_soon_as_its_flows_catch_up() {
	// Inserts merge into slots stored by position, so a layout reordered on reopen must show as wrong answers.
	let path = TempDbPath::new("percentile_view_frozen_reopen_inserts");
	{
		let mut db = sqlite(&path);
		declare_frozen_view(&db);
		db.command(&format!("INSERT app::t [{}]", frozen_rows(0, ROWS / 2).join(", ")));
		assert!(db.await_all_flows(TIMEOUT), "precondition: the flows catch up before the restart");
		assert_frozen_view_matches_batch(&db, "before the restart");
		db.stop();
	}

	let mut db = sqlite(&path);
	db.command(&format!("INSERT app::t [{}]", frozen_rows(ROWS / 2, ROWS).join(", ")));
	if let Some(poisoned) = poisoned_before_flows_catch_up(&db) {
		panic!("inserts after the restart: the frozen view flow failed instead of answering: {poisoned}");
	}
	let batch = format!("FROM app::src | aggregate {{ {} }} by {{ g }}", map_of(&FROZEN_CALLS));
	assert_eq!(db.row_count("FROM app::src"), ROWS, "the source view must hold one row per source row");
	assert_eq!(
		frozen_answers(&db, "FROM app::frozen"),
		frozen_answers(&db, &batch),
		"inserts after the restart: the frozen view must equal the batch query over the same rows"
	);
	db.stop();
}

#[test]
fn user_columns_named_like_slot_columns_neither_clash_nor_leak_into_the_view() {
	// A slot column resolved against a user column of the same name reads the wrong values into every answer.
	let db = memory();
	db.admin("CREATE NAMESPACE app");
	db.admin(
		"CREATE TABLE app::s { id: int4, g: int4, __aggregate0: Option(float8), __aggregate1: Option(float8) }",
	);
	let map = "__aggregate0: stats::approx_percentile(__aggregate1, 0.99, 0.01), __aggregate1: math::sum(__aggregate0), p50: stats::approx_percentile(__aggregate1, 0.5, 0.01)";
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::clash {{ g: int4, __aggregate0: Option(float8), __aggregate1: Option(float8), p50: Option(float8) }} AS {{ FROM app::s | aggregate {{ {map} }} by {{ g }} }}"
	));
	let rows: Vec<String> = (0..300)
		.map(|id| {
			format!(
				"{{ id: {id}, g: {}, __aggregate0: {}.5, __aggregate1: {} }}",
				id % 3,
				id % 7,
				(id * 37) % 1000 + 1
			)
		})
		.collect();
	let names = ["__aggregate0", "__aggregate1", "p50"];
	let batch = format!("FROM app::s | aggregate {{ {map} }} by {{ g }}");
	let assert_matches_batch = |step: &str| {
		assert!(db.await_all_flows(TIMEOUT), "{step}: the flow must process every change");
		let expected = keyed(&db.query(&batch), &["g"], &names);
		let got =
			await_value(expected.clone(), TIMEOUT, || keyed(&db.query("FROM app::clash"), &["g"], &names));
		assert_eq!(got, expected, "{step}: the view must equal the batch query");
		let view_names: Vec<String> =
			db.query("FROM app::clash")[0].columns.iter().map(|column| column.name.clone()).collect();
		let batch_names: Vec<String> =
			db.query(&batch)[0].columns.iter().map(|column| column.name.clone()).collect();
		assert_eq!(view_names, batch_names, "{step}: the view must expose exactly the declared outputs");
	};

	db.command(&format!("INSERT app::s [{}]", rows.join(", ")));
	assert_matches_batch("insert");
	db.command("DELETE app::s FILTER { id >= 50 and id < 120 }");
	assert_matches_batch("delete");
	db.command("UPDATE app::s { __aggregate1: 5000.0 } FILTER { id >= 200 and id < 230 }");
	assert_matches_batch("update");
}
