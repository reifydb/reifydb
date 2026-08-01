// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A custom operator interns its keys as substrate groups, and the host derives the node's horizon
//! by asking the operator (seal_after_ms) rather than reading the RQL node or a capability flag.
//! Driven through a real flow because the failure is quiet: state grows while the report says fine.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{Keyspace, OperatorGroupStateKey},
};
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
	state::RawStatefulOperator,
};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

// One second of event time, in the millisecond scale every coordinate in this substrate uses. The
// operator declares it as its seal span, which is what makes the node event-domain.
const SEAL_AFTER_MS: u64 = 1_000;

const TALLY_STATE: Keyspace = Keyspace::FIRST_CUSTOM;

struct TallyRow {
	g: i32,
	ts: i64,
	total: i64,
}

row!(TallyRow {
	g: i32,
	ts: i64,
	total: i64
});

const TALLY_COLUMNS: &[OperatorColumn] = &[
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

// One running total per `g`, addressed by the group the substrate interned for that key: per-key
// state inside a group range, plus a seal span the operator computes rather than a declared ttl.
struct Tally;

impl RawStatefulOperator for Tally {}

impl OperatorMetadata for Tally {
	const NAME: &'static str = "tally";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only per-group tally with an operator-declared seal horizon";
	const INPUT_COLUMNS: &'static [OperatorColumn] = TALLY_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = TALLY_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

fn group_key(g: i32) -> EncodedKey {
	EncodedKey::new(g.to_be_bytes())
}

impl OperatorLogic for Tally {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Tally)
	}

	fn seal_after_ms(&self) -> Option<u64> {
		// The host derives the node's horizon from this and `apply` stamps in the domain it
		// implies, so the two cannot disagree.
		Some(SEAL_AFTER_MS)
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

				// The substrate stamps this intern from the change coordinate - the row's
				// #time - and later compares that stamp against the seal cutoff.
				let key = group_key(g);
				let group = ctx.intern_group(&key)?;
				let state_key = OperatorGroupStateKey::inner_encoded(group, TALLY_STATE, []);

				let prior: i64 = self.state_get(ctx, &state_key)?.unwrap_or(0);
				let total = prior + 1;
				self.state_set(ctx, &state_key, &total)?;

				let (row_number, _is_new) = ctx.get_or_create_row_number(group, &key)?;
				row_numbers.push(row_number);
				rows.push(TallyRow {
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

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<Tally>())
			// The retention ledger is the only surface that reports what the tick pass actually
			// reclaimed; without a refresh cadence it stays empty (none means off).
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str =
	"from system::metrics::lifecycle::current filter { class == 'operator-group-data' and work_done > 0 }";

#[test]
fn a_custom_operators_idle_group_is_reclaimed_through_the_flow_tick() {
	// The operator declares a seal span, the host derives an event-domain horizon from it, the
	// intern stamps an event-time position, and the tick pass erases the idle group. A break
	// anywhere leaves the group retained with the ledger silent, hence asserting on work_done.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } with { time: event } AS { FROM app::t APPLY tally{} }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// A second key carries the node's event watermark far past group 1's seal horizon, so group 1
	// goes idle without being touched itself - the same shape as a quiet mint while others trade.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);

	// Asserted, not merely awaited: `await_row_count` returns its last observation on timeout, so
	// discarding it would pass against a chain that reclaims nothing.
	assert_eq!(
		db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT),
		1,
		"the guest operator must report reclamation work; a break anywhere in the chain leaves this at zero"
	);
}

#[test]
fn a_group_that_wakes_after_reclamation_publishes_under_its_original_row() {
	// A custom operator's group is coord-less, so a reclaimed key can receive events again. Its
	// data is gone by design, but its identity must outlive the sink row it names, or the woken
	// group mints a second row number and the view carries two rows for one key.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } with { time: event } AS { FROM app::t APPLY tally{} }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v FILTER { g == 1 }", 1, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);
	db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);

	// Group 1 wakes under the same key.
	db.command(r#"INSERT app::t [{ id: 3, g: 1, ts: "1970-01-01T00:10:00.001Z" }]"#);

	// The flow must have applied that insert before a count means anything: awaiting a count of 1
	// returns on the first poll, since g == 1 already holds a row from the first insert.
	assert!(db.await_all_flows(TIMEOUT), "the flow must settle before the row count is evidence");

	assert_eq!(
		db.row_count("FROM app::v FILTER { g == 1 }"),
		1,
		"a woken group must own exactly one row; view now: {:?}",
		db.query_as_root("FROM app::v", ())
	);
}
