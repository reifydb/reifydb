// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Reclaiming a group erases its PERSISTED state, but a guest operator is free to hold a RAM-side
//! mirror of that state - a cache, an accumulator, an interned reference table - which the engine
//! cannot see and cannot erase. invalidate_groups is the only signal that tells the operator its
//! mirror is now lying. Without it the persisted side restarts empty while the operator keeps
//! answering from a cache of a group the substrate has already forgotten, so the operator emits an
//! Update against state that no longer exists anywhere else.
//!
//! custom_operator_reclaim.rs proves the engine erases the persisted side. This file proves the
//! other half: that the operator is TOLD, with the group ids that went. Every chaindex operator
//! that keeps a RAM mirror depends on this callback firing, and nothing exercised it.

use std::{
	sync::{
		Mutex, OnceLock,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration as StdDuration,
};

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{GroupId, GroupSet, Keyspace, OperatorStateKey},
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
const SEAL_AFTER_MS: u64 = 1_000;
const WATCHER_STATE: Keyspace = Keyspace::FIRST_CUSTOM;

// The operator instance lives inside the flow engine, so the test cannot hold a reference to it.
// These record what the callback saw. A Mutex rather than a channel because the assertion is about
// the accumulated set, not about ordering.
fn invalidated() -> &'static Mutex<Vec<GroupId>> {
	static INVALIDATED: OnceLock<Mutex<Vec<GroupId>>> = OnceLock::new();
	INVALIDATED.get_or_init(|| Mutex::new(Vec::new()))
}

static INVALIDATE_CALLS: AtomicU64 = AtomicU64::new(0);

struct WatcherRow {
	g: i32,
	ts: i64,
	total: i64,
}

row!(WatcherRow {
	g: i32,
	ts: i64,
	total: i64
});

const WATCHER_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "g",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "group key",
	},
	OperatorColumn {
		name: "ts",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "event time in millis",
	},
	OperatorColumn {
		name: "total",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "running count per group",
	},
];

// Identical to Tally in custom_operator_reclaim.rs except that it records the invalidation callback.
struct Watcher;

impl RawStatefulOperator for Watcher {}

impl OperatorMetadata for Watcher {
	const NAME: &'static str = "watcher";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only per-group tally that records its group invalidations";
	const INPUT_COLUMNS: &'static [OperatorColumn] = WATCHER_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = WATCHER_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

fn group_key(g: i32) -> EncodedKey {
	EncodedKey::new(g.to_be_bytes())
}

impl OperatorLogic for Watcher {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(Watcher)
	}

	fn seal_after_ms(&self) -> Option<u64> {
		Some(SEAL_AFTER_MS)
	}

	fn invalidate_groups(&mut self, groups: &GroupSet) {
		INVALIDATE_CALLS.fetch_add(1, Ordering::SeqCst);
		invalidated().lock().expect("invalidation log").extend(groups.as_slice().iter().copied());
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

				let key = group_key(g);
				let group = ctx.intern_group(&key)?;
				let state_key = OperatorStateKey::inner_encoded(group, WATCHER_STATE, []);

				let prior: i64 = self.state_get(ctx, &state_key)?.unwrap_or(0);
				let total = prior + 1;
				self.state_set(ctx, &state_key, &total)?;

				let (row_number, _is_new) = ctx.get_or_create_row_number(group, &key)?;
				row_numbers.push(row_number);
				rows.push(WatcherRow {
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
			.with_flow(|f| f.register_operator::<Watcher>())
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str =
	"from system::metrics::lifecycle::current filter { class == 'operator-group-data' and work_done > 0 }";

#[test]
fn reclaiming_a_sealed_group_tells_the_operator_which_group_went() {
	// This is the callback the normalized-block seal semantics rest on: the engine decides a slot is
	// finalized, erases its persisted accumulator and row-number mapping, and the operator learns of
	// it here. An operator holding a RAM mirror keyed by group - which is what a slot-keyed reference
	// table is - has no other way to know, and would keep serving a group whose persisted state is
	// gone. The engine restarting that group fresh while the operator answers from cache is a silent
	// divergence between the view and the operator's own arithmetic.
	// Mutation: drop the invalidate_groups call from reclaim_data's caller in execution/reclaim.rs and
	// this fails while every assertion in custom_operator_reclaim.rs still passes, because the
	// persisted side is erased either way.
	invalidated().lock().expect("invalidation log").clear();
	INVALIDATE_CALLS.store(0, Ordering::SeqCst);

	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } with { time: event } AS { FROM app::t APPLY watcher{} }",
	);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// Group 2 carries the node watermark past group 1's seal horizon without touching group 1.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);
	db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT);

	let seen = invalidated().lock().expect("invalidation log").clone();
	assert!(
		!seen.is_empty(),
		"the engine erased a group's persisted state without telling the operator; \
		 invalidate_groups was called {} times",
		INVALIDATE_CALLS.load(Ordering::SeqCst)
	);

	// The callback must name the group, not merely fire. A blanket "something was reclaimed" signal
	// would force every operator to drop its whole cache on any reclamation.
	assert!(
		seen.iter().all(|group| !group.is_node_scope()),
		"node scope is the operator's own identity space and must never be reported reclaimed: {seen:?}"
	);
}

#[test]
fn a_group_that_is_still_live_is_never_reported_invalidated() {
	// Without this, invalidate_groups could name every group it has ever seen and the test above
	// would pass. An operator told a live group was reclaimed drops a cache entry it still needs,
	// turning a correct Update into a spurious Insert - the same corruption as the missing callback,
	// in the opposite direction.
	invalidated().lock().expect("invalidation log").clear();
	INVALIDATE_CALLS.store(0, Ordering::SeqCst);

	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, ts: int8, total: int8 } with { time: event } AS { FROM app::t APPLY watcher{} }",
	);

	// Two keys, both written well inside the seal span, so neither is ever due.
	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:00:00.100Z" }]"#);
	db.await_row_count("FROM app::v", 2, TIMEOUT);

	assert!(
		invalidated().lock().expect("invalidation log").is_empty(),
		"no group passed its horizon, so nothing may be reported reclaimed"
	);
}
