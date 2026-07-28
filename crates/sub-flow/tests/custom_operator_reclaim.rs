// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A custom operator - the shape every chaindex operator has - interns its keys as substrate
//! groups and the host derives the node's horizon by ASKING THE OPERATOR (seal_after_ms) rather
//! than reading the RQL node or a capability flag. The operator no longer stamps activity at all:
//! the substrate stamps every intern from the change coordinate, which on an event-time source is
//! the row's #time. The source table therefore declares its populator (ts: datetime) so the stamp
//! the substrate takes is the same event time the operator used to pass explicitly. This drives
//! the whole chain through a real flow, because the failure mode it guards is quiet: state that
//! grows while the report calls the node healthy.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{Keyspace, OperatorStateKey},
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
use reifydb_value::value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType};

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

// Keeps one running total per `g`, addressed by the group the substrate interned for that key. This
// is deliberately the same shape as pumpfun-curve (state per mint) and the windowed chaindex
// drivers (state per window): per-key state inside a group range, plus a seal span the operator
// computes rather than a ttl a view author typed.
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
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(Tally)
	}

	// The operator answers the retention question itself. The host reads this to derive the node's
	// horizon, and `apply` stamps in the domain it implies, so the two cannot disagree.
	fn seal_after_ms(&self) -> Option<u64> {
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
				// #time, populated from the declared ts column - and that stamp is what it
				// later compares against the seal cutoff derived from seal_after_ms.
				let key = group_key(g);
				let group = ctx.intern_group(&key)?;
				let state_key = OperatorStateKey::inner_encoded(group, TALLY_STATE, []);

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
	// The chain under test: the operator declares a seal span -> the host derives an event-domain
	// horizon for the APPLY node from it (not from any ttl clause, and not gated on a capability)
	// -> the operator stamps event-time positions when it interns -> the tick pass finds the group
	// due and erases it. A break anywhere in that chain leaves the group retained with the ledger
	// reporting nothing, which is why the assertion is on work_done rather than on a row count.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } with { time: event } AS { FROM app::t APPLY tally{} }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// A second key carries the node's event watermark far past group 1's seal horizon, so group 1
	// goes idle without being touched itself - the same shape as a quiet mint while others trade.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);

	db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);
}

#[test]
fn a_group_that_wakes_after_reclamation_publishes_under_its_original_row() {
	// The two-phase split exists for exactly this: a custom operator's group is coord-less, so a
	// reclaimed key can receive events again. Its data is gone (fresh start, by design) but its
	// identity must survive until the sink row it names does, or the woken group mints a second row
	// number and the view carries two rows for one key - landmine L2, and invisible in any metric.
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

	let rows = db.await_row_count("FROM app::v FILTER { g == 1 }", 1, TIMEOUT);
	assert_eq!(
		rows,
		1,
		"a woken group must own exactly one row; view now: {:?}",
		db.query_as_root("FROM app::v", ())
	);
}
