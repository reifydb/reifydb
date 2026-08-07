// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! `KeyedStateful` is the ergonomic way an SDK operator keeps per-key state, and it is what the
//! chaindex operators are written against. State addressed under node scope is unreachable by every
//! reclaim path, so these assert on the state rows themselves rather than only on a metrics row.

use std::time::Duration as StdDuration;

use reifydb::testing::db::TestDb;
use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_sdk::{
	config::Config,
	error::Result as SdkResult,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
	state::{RawStatefulOperator, keyed::KeyedStateful},
};
use reifydb_value::value::{constraint::TypeConstraint, duration::Duration, value_type::ValueType};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

// How long to wait before concluding a second row never appeared. Long enough to cover several flow
// ticks, short enough that an absence assertion does not cost a full timeout.
const SETTLE: StdDuration = StdDuration::from_secs(4);

// One second of event time, the same span custom_operator_reclaim's Tally declares, so the two
// tests differ in exactly one respect: how the operator addresses its state.
const SEAL_AFTER_MS: u64 = 1_000;

struct CounterRow {
	g: i32,
	ts: i64,
	total: i64,
}

row!(CounterRow {
	g: i32,
	ts: i64,
	total: i64
});

const COUNTER_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "g",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "group key",
	},
	OperatorColumn {
		name: "ts",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "event time in milliseconds",
	},
	OperatorColumn {
		name: "total",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "running total per group",
	},
];

// Written the way a chaindex operator is: `type State` plus `key_types`, taking the default
// `encode_state_key`. Nothing here mentions groups; that default alone decides reclaimability.
struct Counter;

impl RawStatefulOperator for Counter {}

impl KeyedStateful for Counter {
	type State = i64;

	fn key_types(&self) -> &[ValueType] {
		&[ValueType::Int4]
	}
}

impl OperatorMetadata for Counter {
	const NAME: &'static str = "counter";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only KeyedStateful counter with an operator-declared seal horizon";
	const INPUT_COLUMNS: &'static [OperatorColumn] = COUNTER_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = COUNTER_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for Counter {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Counter)
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(Duration::from_milliseconds_const(SEAL_AFTER_MS as i64))
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			if !matches!(diff.kind(), DiffType::Insert) {
				continue;
			}
			let Some(post) = diff.post() else {
				continue;
			};

			let mut rows = Vec::new();
			let mut row_numbers = Vec::new();
			for r in 0..post.row_count() {
				let row = post.row(r).expect("row");
				let g = row.i32("g").expect("g");
				let ts = row.datetime("ts").expect("ts").to_millis() as i64;

				let keys = [Value::Int4(g)];
				let total = self.load_state(ctx, &keys)?.unwrap_or(0) + 1;
				self.save_state(ctx, &keys, &total)?;

				// Interned from the raw key bytes rather than the serialized key values
				// KeyedStateful uses, so this is its own group; the point is only that the
				// output row number is group-scoped rather than node-scoped.
				let group = ctx.intern_group(&group_key(g))?;
				let (row_number, _is_new) = ctx.get_or_create_row_number(group, &group_key(g))?;
				row_numbers.push(row_number);
				rows.push(CounterRow {
					g,
					ts,
					total,
				});
			}
			if !rows.is_empty() {
				ctx.emit_insert(&rows, &row_numbers)?;
			}
		}
		Ok(())
	}
}

fn group_key(g: i32) -> reifydb_codec::key::encoded::EncodedKey {
	reifydb_codec::key::encoded::EncodedKey::new(g.to_be_bytes())
}

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Counter>())
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str = "from system::metrics::runtime::operators::current filter { metric == 'state_compaction_dropped' and value > 0.0 }";

#[test]
fn a_keyed_stateful_guests_idle_group_is_reclaimed() {
	// The ergonomic guest state path must be reachable by a sweep at all. Addressed at node scope
	// it never is, and it fails silently: the node is reported bounded while its state grows.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } \
		 AS { FROM app::t APPLY counter{} }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// A second key carries the node's event watermark past group 1's seal horizon, so group 1 goes
	// idle without being touched itself.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);

	assert_eq!(
		db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT),
		1,
		"a KeyedStateful guest's per-key state must be addressable by the group sweep"
	);
}

#[test]
fn a_woken_keys_state_restarted_because_the_sweep_actually_erased_it() {
	// The metrics row above does not discriminate: a group exists whatever the state addressing,
	// so the data phase reports work_done > 0 even when it erases an empty range. The counter's
	// own value is the honest witness - erased state restarts at 1, surviving state reaches 2.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } \
		 AS { FROM app::t APPLY counter{} }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v FILTER { g == 1 }", 1, TIMEOUT);
	assert_eq!(total_for(&db, 1), Some(1), "precondition: the first event counts to one");

	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);
	db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 3, g: 1, ts: "2026-01-01T00:10:00.001Z" }]"#);

	// Settling the flow, not polling for the value asserted: the total already read 1 from the
	// first event, so a poll would return before the third insert had been applied.
	assert!(db.await_all_flows(TIMEOUT), "the flow must settle before the total is evidence");

	assert_eq!(
		total_for(&db, 1),
		Some(1),
		"a woken key whose state was erased must restart from one; two would mean the sweep \
		 reported success while the guest's rows were sitting outside its reach"
	);

	// And the identity that outlived the data still resolves, so the woken key republishes under the
	// row it already owned rather than minting a second one beside it.
	assert_eq!(
		db.await_row_count("FROM app::v FILTER { g == 1 }", 2, SETTLE),
		1,
		"a woken key must own exactly one row"
	);
}

fn total_for(db: &TestDb, g: i32) -> Option<i64> {
	let frames = db.query(&format!("FROM app::v FILTER {{ g == {g} }}"));
	for frame in &frames {
		if frame.row_count() > 0 {
			return frame.get::<i64>("total", 0).expect("get total");
		}
	}
	None
}
