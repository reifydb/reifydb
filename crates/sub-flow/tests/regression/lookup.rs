// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, LazyLock},
	thread,
};

use reifydb::{
	SqliteConfig, Value, WithSubsystem, embedded,
	multi_storage::MultiStore,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::{
	common::{CommitVersion, WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	lifecycle::task::LifecycleTask,
	operator_with::ApplyWith,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig, sync::mutex::Mutex};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_sub_lifecycle::{
	gc::historical::actor::HistoricalGcTask,
	plane::{RetentionPlane, ledger::EngineFloors},
};
use reifydb_value::{
	config::ExtensionParams,
	value::{
		constraint::TypeConstraint, diff_type::DiffType, duration::Duration, row_number::RowNumber,
		value_type::ValueType,
	},
};

const SETTLE: Duration = Duration::from_seconds_const(10);
const HELD: Duration = Duration::from_seconds_const(1);
const T0: &str = "2026-01-01T00:00:00Z";

#[derive(Default, Clone, Copy)]
struct Gate {
	open: bool,
	entered: bool,
}

static GATES: LazyLock<Mutex<HashMap<String, Gate>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn set_gate(name: &str, open: bool) {
	GATES.lock().entry(name.to_string()).or_default().open = open;
}

fn gate(name: &str) -> Gate {
	GATES.lock().get(name).copied().unwrap_or_default()
}

struct GatedPassThrough {
	gate: String,
}

struct KeyValue {
	k: String,
	v: i64,
}

row!(KeyValue {
	k: String,
	v: i64
});

const KEY_VALUE_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "k",
		type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
		description: "key, copied from the input",
	},
	OperatorColumn {
		name: "v",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "value, copied from the input",
	},
];

impl OperatorMetadata for GatedPassThrough {
	const NAME: &'static str = "gated_pass_through";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds every step until its named gate opens, then passes k and v through";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = KEY_VALUE_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn key_values(columns: &impl ColumnsView) -> SdkResult<(Vec<KeyValue>, Vec<RowNumber>)> {
	let mut rows = Vec::with_capacity(columns.row_count());
	let mut numbers = Vec::with_capacity(columns.row_count());
	for index in 0..columns.row_count() {
		let row = columns.row(index).expect("a row inside row_count must exist");
		rows.push(KeyValue {
			k: row.utf8("k")?.expect("k is set").to_string(),
			v: row.i64("v")?.expect("v is set"),
		});
		numbers.push(row.row_number().expect("a flow row carries its row number"));
	}
	Ok((rows, numbers))
}

impl UnmanagedOperator for GatedPassThrough {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
		throttles: false,
	};

	fn create(_operator_id: OperatorId, params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(GatedPassThrough {
			gate: params.require_str("gate"),
		})
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		GATES.lock().entry(self.gate.clone()).or_default().entered = true;
		while !gate(&self.gate).open {
			thread::sleep(Duration::from_milliseconds_const(10).to_std());
		}
		for index in 0..change.diff_count() {
			let Some(diff) = change.diff(index) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					let (rows, numbers) = key_values(&diff.post().expect("an insert has a post"))?;
					ctx.emit_insert(&rows, &numbers)?;
				}
				DiffType::Update => {
					let (pre, numbers) = key_values(&diff.pre().expect("an update has a pre"))?;
					let (post, _) = key_values(&diff.post().expect("an update has a post"))?;
					ctx.emit_update(&pre, &post, &numbers)?;
				}
				DiffType::Remove => {
					let (rows, numbers) = key_values(&diff.pre().expect("a remove has a pre"))?;
					ctx.emit_remove(&rows, &numbers)?;
				}
			}
		}
		Ok(())
	}
}

fn disarmed() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory_db() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(disarmed())
			.with_flow(|f| f.register_unmanaged_operator::<GatedPassThrough>())
			.build()
			.expect("build memory db with flow"),
	)
}

fn settle(db: &TestDb) {
	assert!(db.await_all_flows(SETTLE), "flows never caught up");
}

fn create_level(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::level { id: int4, k: utf8, at: datetime } WITH { time: event(at) }");
}

fn create_price_table(db: &TestDb) {
	db.admin("CREATE TABLE app::price { k: utf8, v: int8 } WITH { partition: { by: { k } } }");
}

fn create_price_view(db: &TestDb, body: &str) {
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::price {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ FROM app::src {body} }}"
	));
}

fn create_cost(db: &TestDb, form: &str) {
	db.admin(&format!("CREATE DEFERRED VIEW app::cost {{ id: int4, usd: Option(int8) }} AS {{ \
		 FROM app::level \
		 {form} LOOKUP {{ FROM app::price }} AS p USING (k, p.k) WITH {{ retention: {{ left: 10s }} }} \
		 MAP {{ id, usd: p_v }} }}"));
}

fn create_total(db: &TestDb) {
	db.admin("CREATE DEFERRED VIEW app::total { total: int8 } AS { \
		 FROM app::level \
		 INNER LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { retention: { left: 10s } } \
		 AGGREGATE { total: math::sum(p_v) } BY {} }");
}

fn insert_level(db: &TestDb, id: i32, k: &str, at: &str) {
	db.command(&format!(r#"INSERT app::level [{{ id: {id}, k: '{k}', at: "{at}" }}]"#));
}

fn put_price(db: &TestDb, table: &str, k: &str, v: i64) {
	db.command(&format!("INSERT app::{table} [{{ k: '{k}', v: {v} }}]"));
}

fn set_price(db: &TestDb, table: &str, k: &str, v: i64) {
	db.command(&format!("UPDATE app::{table} {{ v: {v} }} FILTER {{ k == '{k}' }}"));
}

fn cost_rows(db: &TestDb) -> Vec<(i32, Option<i64>)> {
	let mut rows = Vec::new();
	for frame in db.query("FROM app::cost") {
		let ids = frame.column("id").expect("id reads").expect("cost must expose id");
		let usds = frame.column("usd").expect("usd reads").expect("cost must expose usd");
		for row in 0..ids.len() {
			let id = match ids.get_value(row) {
				Value::Int4(id) => id,
				other => panic!("id must be int4, got {other:?}"),
			};
			let usd = match usds.get_value(row) {
				Value::Int8(usd) => Some(usd),
				Value::None {
					..
				} => None,
				other => panic!("usd must be int8 or none, got {other:?}"),
			};
			rows.push((id, usd));
		}
	}
	rows.sort();
	rows
}

fn total(db: &TestDb) -> Option<i64> {
	let frames = db.query("FROM app::total");
	let frame = frames.first()?;
	let column = frame.column("total").expect("total reads")?;
	match column.len() {
		0 => None,
		1 => match column.get_value(0) {
			Value::Int8(v) => Some(v),
			other => panic!("total must be int8, got {other:?}"),
		},
		n => panic!("the ungrouped total must hold at most one row, got {n}"),
	}
}

fn await_total(db: &TestDb, want: i64) -> Option<i64> {
	poll_until(|| (total(db) == Some(want)).then_some(want), SETTLE).or_else(|| total(db))
}

#[test]
fn an_inner_lookup_on_a_table_returns_the_row_of_the_matching_partition() {
	// S2 thin slice: the right row of the left key's partition, never the other partition's row.
	let db = memory_db();
	create_level(&db);
	create_price_table(&db);
	create_cost(&db, "INNER");
	put_price(&db, "price", "a", 10);
	put_price(&db, "price", "b", 20);

	insert_level(&db, 1, "a", T0);
	insert_level(&db, 2, "b", T0);
	settle(&db);

	assert_eq!(cost_rows(&db), vec![(1, Some(10)), (2, Some(20))]);
}

#[test]
fn an_inner_lookup_on_a_table_with_no_match_emits_no_row() {
	// LT1 table form: an unmatched left row must not leak into the output.
	let db = memory_db();
	create_level(&db);
	create_price_table(&db);
	create_cost(&db, "INNER");
	put_price(&db, "price", "a", 10);

	insert_level(&db, 1, "zz", T0);
	insert_level(&db, 2, "a", T0);
	settle(&db);

	assert_eq!(cost_rows(&db), vec![(2, Some(10))], "the unmatched row 1 must not appear");
}

#[test]
fn a_left_lookup_on_a_table_with_no_match_keeps_the_row_with_none() {
	// LT2 end to end: the left form must keep the unmatched row with none right columns.
	let db = memory_db();
	create_level(&db);
	create_price_table(&db);
	create_cost(&db, "LEFT");
	put_price(&db, "price", "a", 10);

	insert_level(&db, 1, "zz", T0);
	insert_level(&db, 2, "a", T0);
	settle(&db);

	assert_eq!(cost_rows(&db), vec![(1, None), (2, Some(10))]);
}

#[test]
fn a_table_lookup_keeps_the_price_of_its_step_and_a_later_right_change_emits_nothing() {
	// LT3 and LT5 table form: a right change alone must not re-emit, and a new left row must see the new price.
	let db = memory_db();
	create_level(&db);
	create_price_table(&db);
	create_cost(&db, "INNER");
	put_price(&db, "price", "a", 10);
	insert_level(&db, 1, "a", T0);
	settle(&db);
	assert_eq!(cost_rows(&db), vec![(1, Some(10))]);

	set_price(&db, "price", "a", 20);
	settle(&db);
	assert_eq!(cost_rows(&db), vec![(1, Some(10))], "a right-side change alone must produce no output");

	insert_level(&db, 2, "a", T0);
	settle(&db);
	assert_eq!(cost_rows(&db), vec![(1, Some(10)), (2, Some(20))], "a new left row reads the newest price");
}

#[test]
fn a_dictionary_partitioned_table_matches_on_both_keys_and_a_miss_does_not_grow_the_dictionary() {
	// S3 on polaris::pool's shape and LT6: a miss must be no match and must never intern the value.
	let db = memory_db();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE DICTIONARY app::tokens FOR utf8 AS uint4");
	db.admin(
		"CREATE TABLE app::pool { pool: utf8 with { dictionary: app::tokens }, pair: utf8 with { dictionary: app::tokens }, fee: int8 } \
		 WITH { partition: { by: { pool, pair } } }",
	);
	db.admin("CREATE TABLE app::level { id: int4, pool: utf8, pair: utf8, at: datetime } WITH { time: event(at) }");
	db.admin("CREATE DEFERRED VIEW app::cost { id: int4, usd: Option(int8) } AS { \
		 FROM app::level \
		 INNER LOOKUP { FROM app::pool } AS info USING (pair, info.pair) and (pool, info.pool) \
		 WITH { retention: { left: 10s } } \
		 MAP { id, usd: info_fee } }");
	db.command("INSERT app::pool [{ pool: 'p1', pair: 'x', fee: 5 }, { pool: 'p1', pair: 'y', fee: 7 }]");
	let entries = db.row_count("FROM app::tokens");
	assert_eq!(entries, 3, "precondition: p1, x and y are interned");

	for (id, pool, pair) in [(1, "p1", "x"), (2, "p1", "y"), (3, "p1", "nope"), (4, "x", "p1"), (5, "q9", "x")] {
		db.command(&format!(
			r#"INSERT app::level [{{ id: {id}, pool: '{pool}', pair: '{pair}', at: "{T0}" }}]"#
		));
	}
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![(1, Some(5)), (2, Some(7))],
		"only exact (pool, pair) partitions match; swapped keys and unknown values match nothing"
	);
	assert_eq!(db.row_count("FROM app::tokens"), entries, "the lookup must never intern a left value");
}

#[test]
fn a_lagging_producer_holds_the_lookup_then_the_price_its_commit_made_comes_out() {
	// S5 and LT3 view form: reading at S finds nothing and at latest finds 20, so only the MD25 read gives 10.
	let db = memory_db();
	set_gate("lt3", false);
	create_level(&db);
	create_price_view(&db, "APPLY gated_pass_through { gate: 'lt3' }");
	create_cost(&db, "INNER");
	settle(&db);

	put_price(&db, "src", "a", 10);
	assert!(
		poll_until(|| gate("lt3").entered.then_some(()), SETTLE).is_some(),
		"the producer never started its step"
	);
	insert_level(&db, 1, "a", T0);
	assert_eq!(
		db.await_row_count("FROM app::cost", 1, HELD),
		0,
		"the lookup ran before its producer completed the source it needs"
	);

	set_price(&db, "src", "a", 20);
	set_gate("lt3", true);
	settle(&db);

	assert_eq!(db.row_count("FROM app::price"), 1, "precondition: one row per key in the looked-up view");
	assert_eq!(cost_rows(&db), vec![(1, Some(10))], "the left row must pair with the commit made for its source");

	insert_level(&db, 2, "a", T0);
	settle(&db);
	assert_eq!(cost_rows(&db), vec![(1, Some(10)), (2, Some(20))]);
}

#[test]
fn a_right_change_alone_on_a_looked_up_view_emits_nothing() {
	// LT5 view form: a price row change must cost the lookup flow no output (MG1).
	let db = memory_db();
	create_level(&db);
	create_price_view(&db, "MAP { k, v }");
	create_cost(&db, "INNER");
	create_total(&db);
	put_price(&db, "src", "a", 10);
	settle(&db);
	insert_level(&db, 1, "a", T0);
	settle(&db);
	assert_eq!(cost_rows(&db), vec![(1, Some(10))]);
	assert_eq!(total(&db), Some(10));

	for v in 11..=15 {
		set_price(&db, "src", "a", v);
	}
	settle(&db);

	assert_eq!(cost_rows(&db), vec![(1, Some(10))], "a right-side change alone must produce no output");
	assert_eq!(total(&db), Some(10), "no retraction or re-emission may reach the aggregate");
}

#[test]
fn a_removed_left_row_retracts_exactly_the_view_row_it_published() {
	// LT4 end to end: a retraction built from the newest price instead of the stored version drifts the total.
	let db = memory_db();
	create_level(&db);
	create_price_view(&db, "MAP { k, v }");
	create_total(&db);
	put_price(&db, "src", "a", 10);
	settle(&db);
	insert_level(&db, 1, "a", T0);
	settle(&db);
	set_price(&db, "src", "a", 30);
	settle(&db);
	insert_level(&db, 2, "a", T0);
	settle(&db);
	assert_eq!(total(&db), Some(40));

	db.command("DELETE app::level FILTER { id == 1 }");
	settle(&db);

	assert_eq!(await_total(&db, 30), Some(30), "retracting 30 instead of the published 10 would leave 10");
}

#[test]
fn the_lookup_lease_moves_past_the_read_versions_of_expired_left_rows() {
	// LT11: once a left row expires its read version must stop holding GC, otherwise the lease pins it forever.
	let db = memory_db();
	create_level(&db);
	create_price_table(&db);
	create_cost(&db, "INNER");
	put_price(&db, "price", "a", 10);
	insert_level(&db, 1, "a", T0);
	settle(&db);
	let first = db.engine().current_version().expect("current version");
	let leases = db.engine().multi().leases().clone();
	assert!(
		leases.min_active().is_some_and(|lease| lease <= first),
		"precondition: the lookup holds a lease at or below its first read, got {:?}",
		leases.min_active()
	);

	insert_level(&db, 2, "a", "2026-01-01T00:01:00Z");
	insert_level(&db, 3, "a", "2026-01-01T00:02:00Z");
	settle(&db);

	let moved = poll_until(|| leases.min_active().filter(|lease| *lease > first), SETTLE);
	assert!(
		moved.is_some(),
		"the lease must move above {first:?} once row 1 expired, still at {:?}",
		leases.min_active()
	);
}

fn force_gc_and_flush(db: &TestDb) -> CommitVersion {
	let engine = db.engine().clone();
	let store = match engine.multi_owned().store() {
		MultiStore::Standard(store) => store.clone(),
	};
	let plane = RetentionPlane::new(Arc::new(EngineFloors::new(engine.clone())), engine.version_epoch().clone());
	let mut gc = HistoricalGcTask::new(store.clone(), plane, engine.clock().clone(), Arc::new(engine.catalog()));
	while gc.run_slice().is_yielded() {}
	store.flush_pending_blocking();
	engine.multi().leases().min_active().unwrap_or(CommitVersion(u64::MAX)).min(engine.query_done_until())
}

#[test]
fn a_retraction_after_gc_and_flush_equals_the_published_row() {
	// LT9 (MD29): without the lease holding the read version through GC and flush the retraction must drift.
	let (config, _guard) = SqliteConfig::in_memory();
	let db = TestDb::from(
		embedded::sqlite(config)
			.with_runtime_config(disarmed())
			.with_flow(|f| f.register_unmanaged_operator::<GatedPassThrough>())
			.build()
			.expect("build sqlite db with flow"),
	);
	create_level(&db);
	create_price_view(&db, "MAP { k, v }");
	create_total(&db);
	put_price(&db, "src", "a", 10);
	settle(&db);
	insert_level(&db, 1, "a", T0);
	settle(&db);
	let published = db.engine().current_version().expect("current version");
	assert_eq!(total(&db), Some(10));

	for v in 11..=15 {
		set_price(&db, "src", "a", v);
		settle(&db);
	}
	insert_level(&db, 2, "a", T0);
	settle(&db);
	assert_eq!(total(&db), Some(25), "row 2 reads the newest price, 15");

	let cutoff = force_gc_and_flush(&db);
	assert!(
		cutoff <= published,
		"the gc cutoff {cutoff:?} must be held at or below the read of row 1 ({published:?})"
	);

	db.command("DELETE app::level FILTER { id == 1 }");
	settle(&db);

	assert_eq!(
		await_total(&db, 15),
		Some(15),
		"the retraction of row 1 must carry 10, the price it was published with"
	);
}
