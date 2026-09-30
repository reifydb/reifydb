// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc, thread};

use arrow_array::RecordBatch;
use reifydb::{Frame, HydrationConfig, Params, Subscription, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			id::SubscriptionId,
			object::ObjectId,
			subscription::{SubscribeOptions, SubscribeOutcome},
		},
		change::StagedBatch,
	},
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{
		HandOffHooks, HydrateError, HydrateOutcome, HydrationBound, InstalledHandOffHooks,
		SubscriptionServiceRef,
	},
};
use reifydb_flow::backfill::testing::{InstalledScanHooks, NoFaults, ScanHooks};
use reifydb_runtime::{
	RuntimeConfig,
	context::clock::{Clock, MockClock},
	fatal::FatalConfig,
	sync::mutex::Mutex,
};
use reifydb_sub_subscription::{store::SubscriptionStore, subsystem::SubscriptionSubsystem};
use reifydb_transaction::{multi::lease::VersionLeaseGuard, transaction::Transaction};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	datetime::DateTime,
	diff_type::DiffType,
	duration::Duration,
	identity::IdentityId,
	system_columns::{column_view, row_numbers, user_columns},
};

const MAX_ROWS: u64 = 100_000;

const HISTORY: &[&str] = &[
	"INSERT app::t [{id: 1, grp: 1, qty: 10}, {id: 2, grp: 2, qty: 60}, {id: 3, grp: 1, qty: 70}, {id: 4, grp: 2, qty: 20}, {id: 5, grp: 1, qty: 90}, {id: 6, grp: 3, qty: 40}]",
	"INSERT app::t [{id: 7, grp: 3, qty: 80}, {id: 8, grp: 2, qty: 55}]",
	"UPDATE app::t { qty: 65 } FILTER { id == 1 }",
	"DELETE app::t FILTER { id == 3 }",
	"UPDATE app::t { qty: 30 } FILTER { id == 5 }",
	"INSERT app::t [{id: 9, grp: 1, qty: 75}, {id: 10, grp: 3, qty: 15}]; UPDATE app::t { qty: 85 } FILTER { id == 10 }",
	"UPDATE app::t { qty: qty + 1 } FILTER { grp == 2 }",
	"DELETE app::t FILTER { id == 6 or id == 9 }",
	"INSERT app::labels [{id: 1, label: 'a'}, {id: 2, label: 'b'}, {id: 7, label: 'c'}, {id: 10, label: 'd'}, {id: 11, label: 'e'}]",
	"UPDATE app::labels { label: 'aa' } FILTER { id == 1 }",
];

const LATER: &[&str] = &[
	"INSERT app::t [{id: 11, grp: 2, qty: 95}]",
	"UPDATE app::t { qty: 5 } FILTER { id == 8 }",
	"UPDATE app::t { qty: 51 } FILTER { id == 5 }",
	"DELETE app::t FILTER { id == 10 }",
	"INSERT app::t [{id: 12, grp: 1, qty: 45}]; DELETE app::t FILTER { id == 12 }",
	"UPDATE app::t { grp: 3 } FILTER { id == 2 }",
	"UPDATE app::labels { label: 'bb' } FILTER { id == 2 }",
	"DELETE app::labels FILTER { id == 7 }",
	"INSERT app::labels [{id: 8, label: 'f'}]",
];

const TEN_ROWS: &str = "INSERT app::t [{id: 1, grp: 1, qty: 10}, {id: 2, grp: 2, qty: 20}, {id: 3, grp: 0, qty: 30}, {id: 4, grp: 1, qty: 40}, {id: 5, grp: 2, qty: 50}, {id: 6, grp: 0, qty: 60}, {id: 7, grp: 1, qty: 70}, {id: 8, grp: 2, qty: 80}, {id: 9, grp: 0, qty: 90}, {id: 10, grp: 1, qty: 100}]";

fn make_db() -> TestDb {
	tables(TestDb::builder().mock_time(DateTime::from_millis(1_000)).memory())
}

fn make_db_with_flows() -> TestDb {
	let clock = Clock::Mock(MockClock::from_millis(1_000));
	let config = RuntimeConfig::default().fatal(FatalConfig::disarmed()).clock(clock);
	tables(TestDb::from(
		embedded::memory().with_flow(|flow| flow).with_runtime_config(config).build().expect("db with flows"),
	))
}

fn tables(db: TestDb) -> TestDb {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, grp: int4, qty: int4 }");
	db.admin("CREATE TABLE app::labels { id: int4, label: utf8 }");
	db
}

fn ten_rows() -> TestDb {
	let db = make_db();
	write(&db, TEN_ROWS);
	db
}

fn write(db: &TestDb, rql: &str) {
	db.mock_clock().advance_secs(1);
	db.command(rql);
}

fn current(db: &TestDb) -> CommitVersion {
	db.engine().current_version().expect("current version")
}

fn settle_flows(db: &TestDb) {
	assert!(db.await_all_flows(Duration::from_seconds(10).unwrap()), "the deferred flows did not catch up");
}

fn settle(db: &TestDb) {
	let target = db.watermarks().tx().current().expect("current version");
	assert!(
		db.watermarks().cdc().wait_for_consumer(target, Duration::from_seconds(10).unwrap()),
		"the subscription consumer did not reach {target:?}"
	);
}

fn subscribe(db: &TestDb, identity: IdentityId, rql: &str) -> SubscriptionId {
	match db.engine().subscribe_as(identity, rql, Params::None, SubscribeOptions::default()).expect("subscribe") {
		SubscribeOutcome::Local {
			id,
		} => id,
		SubscribeOutcome::Remote {
			address,
			..
		} => panic!("expected a local subscription, got a remote one at {address}"),
	}
}

fn lease(db: &TestDb) -> (CommitVersion, VersionLeaseGuard) {
	db.engine().acquire_current_snapshot_lease().expect("acquire lease")
}

fn hydrate(
	db: &TestDb,
	id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<HydrateOutcome, HydrateError> {
	let engine: StandardEngine = db.engine().clone();
	let service = engine.services().ioc.resolve::<SubscriptionServiceRef>().expect("subscription service");
	service.hydrate(id, &engine, identity, lease, max_rows)
}

fn drain(db: &TestDb, id: SubscriptionId) -> Vec<StagedBatch> {
	settle(db);
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let store = subsystem.store();
	let mut out = store.drain(&id, usize::MAX);
	let mut quiet = 0;
	while quiet < 3 {
		thread::sleep(Duration::from_milliseconds(20).unwrap().to_std());
		let more = store.drain(&id, usize::MAX);
		if more.is_empty() {
			quiet += 1;
		} else {
			quiet = 0;
			out.extend(more);
		}
	}
	out
}

fn render(batch: &RecordBatch) -> Vec<String> {
	let columns: Vec<(String, ColumnView<'_>)> = user_columns(batch)
		.map(|(field, array)| {
			(field.name().to_string(), ColumnView::try_from((array, field.as_ref())).expect("column view"))
		})
		.collect();
	(0..batch.num_rows())
		.map(|i| {
			columns.iter()
				.map(|(name, view)| format!("{name}={}", view.get_value(i)))
				.collect::<Vec<_>>()
				.join(", ")
		})
		.collect()
}

fn query_rows(frames: &[Frame]) -> Vec<String> {
	let mut rows: Vec<String> = frames.iter().flat_map(|frame| render(&frame.batch)).collect();
	rows.sort();
	rows
}

fn sorted(rows: &[&str]) -> Vec<String> {
	let mut rows: Vec<String> = rows.iter().map(|row| row.to_string()).collect();
	rows.sort();
	rows
}

fn announced(batches: &[StagedBatch]) -> Vec<(DiffType, i32)> {
	let mut out = Vec::new();
	for (op, batch) in batches {
		if batch.num_rows() == 0 {
			continue;
		}
		let ids = column_view(batch, "id").unwrap().expect("id column");
		for i in 0..batch.num_rows() {
			match ids.get_value(i) {
				Value::Int4(id) => out.push((*op, id)),
				other => panic!("expected an Int4 id, got {other:?}"),
			}
		}
	}
	out
}

fn inserted_ids(batches: &[StagedBatch]) -> Vec<i32> {
	let mut ids: Vec<i32> = announced(batches)
		.into_iter()
		.map(|(op, id)| {
			assert_eq!(
				op,
				DiffType::Insert,
				"a snapshot may only announce inserts, got {op:?} for id {id}"
			);
			id
		})
		.collect();
	ids.sort();
	ids
}

#[derive(Default)]
struct Rows {
	held: BTreeMap<u64, String>,
	faults: Vec<String>,
}

impl Rows {
	fn apply(&mut self, channel: &str, batches: &[StagedBatch]) {
		for (op, batch) in batches {
			if batch.num_rows() == 0 {
				continue;
			}
			let numbers = row_numbers(batch).expect("row numbers");
			assert_eq!(
				numbers.len(),
				batch.num_rows(),
				"{channel}: a delivered {op:?} batch must carry #rownum or no subscriber can apply it"
			);
			for (number, row) in numbers.iter().zip(render(batch)) {
				let key = number.value();
				match op {
					DiffType::Insert => {
						if let Some(old) = self.held.insert(key, row.clone()) {
							self.faults.push(format!(
								"{channel}: insert of row {key} ({row}) while it is held as ({old})"
							));
						}
					}
					DiffType::Update => {
						if self.held.insert(key, row.clone()).is_none() {
							self.faults.push(format!(
								"{channel}: update of row {key} ({row}) that was never announced"
							));
						}
					}
					DiffType::Remove => {
						if self.held.remove(&key).is_none() {
							self.faults.push(format!(
								"{channel}: remove of row {key} ({row}) that was never announced"
							));
						}
					}
				}
			}
		}
	}

	fn values(&self) -> Vec<String> {
		let mut rows: Vec<String> = self.held.values().cloned().collect();
		rows.sort();
		rows
	}
}

fn held_ids(rows: &Rows) -> Vec<i32> {
	let mut ids: Vec<i32> = rows
		.held
		.values()
		.map(|row| {
			let first = row.split(", ").next().expect("a rendered row");
			first.strip_prefix("id=").expect("id is the first column").parse().expect("an integer id")
		})
		.collect();
	ids.sort();
	ids
}

struct Watcher {
	id: SubscriptionId,
	version: CommitVersion,
	snapshot: Vec<StagedBatch>,
	live: Vec<StagedBatch>,
	rows: Rows,
}

impl Watcher {
	fn open(db: &TestDb, identity: IdentityId, rql: &str) -> Self {
		let id = subscribe(db, identity, rql);
		let (version, lease) = lease(db);
		let outcome = hydrate(db, id, identity, lease, MAX_ROWS).expect("the backfill must succeed");
		Self::hydrated(id, version, outcome)
	}

	fn hydrated(id: SubscriptionId, version: CommitVersion, outcome: HydrateOutcome) -> Self {
		assert_eq!(outcome.version, version, "the backfill must read the sources at the leased version");
		let mut rows = Rows::default();
		rows.apply("snapshot", &outcome.batches);
		Self {
			id,
			version,
			snapshot: outcome.batches,
			live: Vec::new(),
			rows,
		}
	}

	fn catch_up(&mut self, db: &TestDb) {
		let batches = drain(db, self.id);
		self.rows.apply("live", &batches);
		self.live.extend(batches);
	}

	fn assert_clean(&self, when: &str) {
		assert!(self.rows.faults.is_empty(), "{when}: the subscriber saw {:#?}", self.rows.faults);
	}
}

struct Case {
	flows: bool,
	ddl: &'static [&'static str],
	rql: &'static str,
	history: &'static [&'static str],
	later: &'static [&'static str],
	oracle: bool,
	after_history: &'static [&'static str],
	after_later: &'static [&'static str],
}

fn late_equals_early(case: Case) {
	let db = if case.flows {
		make_db_with_flows()
	} else {
		make_db()
	};
	for ddl in case.ddl {
		db.admin(ddl);
	}
	let mut early = Watcher::open(&db, IdentityId::root(), case.rql);
	assert!(early.snapshot.iter().all(|(_, batch)| batch.num_rows() == 0), "nothing exists yet to backfill");
	for rql in case.history {
		write(&db, rql);
	}
	if case.flows {
		settle_flows(&db);
	}
	settle(&db);
	early.catch_up(&db);
	let mut late = Watcher::open(&db, IdentityId::root(), case.rql);
	late.catch_up(&db);
	assert!(late.live.is_empty(), "nothing was written after the late backfill, so nothing may arrive live");
	compare(&db, &case, &early, &late, case.after_history, "after the history");
	for rql in case.later {
		write(&db, rql);
	}
	if case.flows {
		settle_flows(&db);
	}
	settle(&db);
	early.catch_up(&db);
	late.catch_up(&db);
	compare(&db, &case, &early, &late, case.after_later, "after the later writes");
}

fn compare(db: &TestDb, case: &Case, early: &Watcher, late: &Watcher, want: &[&str], when: &str) {
	early.assert_clean(&format!("{when}, early"));
	late.assert_clean(&format!("{when}, late"));
	assert_eq!(early.rows.values(), sorted(want), "{when}: the early subscription holds the wrong rows");
	assert_eq!(
		late.rows.held, early.rows.held,
		"{when}: a subscription created late must hold the early one's rows under the same row numbers"
	);
	if case.oracle {
		assert_eq!(
			early.rows.values(),
			query_rows(&db.query(case.rql)),
			"{when}: the rows differ from a fresh query"
		);
	}
}

const FILTER_AFTER_HISTORY: &[&str] = &[
	"id=1, grp=1, qty=65",
	"id=2, grp=2, qty=61",
	"id=7, grp=3, qty=80",
	"id=8, grp=2, qty=56",
	"id=10, grp=3, qty=85",
];

const FILTER_AFTER_LATER: &[&str] = &[
	"id=1, grp=1, qty=65",
	"id=2, grp=3, qty=61",
	"id=5, grp=1, qty=51",
	"id=7, grp=3, qty=80",
	"id=11, grp=2, qty=95",
];

#[test]
fn a_late_filter_subscription_equals_an_early_one() {
	// Rows that cross the filter both ways before the late create must land exactly as the live path left them.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | filter { qty > 50 }",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: FILTER_AFTER_HISTORY,
		after_later: FILTER_AFTER_LATER,
	});
}

#[test]
fn a_late_map_subscription_equals_an_early_one() {
	// A backfill that skips the map or computes it on stale values holds different doubled values.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | map { id, doubled: qty * 2 }",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &[
			"id=1, doubled=130",
			"id=2, doubled=122",
			"id=4, doubled=42",
			"id=5, doubled=60",
			"id=7, doubled=160",
			"id=8, doubled=112",
			"id=10, doubled=170",
		],
		after_later: &[
			"id=1, doubled=130",
			"id=2, doubled=122",
			"id=4, doubled=42",
			"id=5, doubled=102",
			"id=7, doubled=160",
			"id=8, doubled=10",
			"id=11, doubled=190",
		],
	});
}

#[test]
fn a_late_extend_subscription_equals_an_early_one() {
	// The extended column must be computed from the row at V, never from a value overwritten before it.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | extend { big: qty > 50 }",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &[
			"id=1, grp=1, qty=65, big=true",
			"id=2, grp=2, qty=61, big=true",
			"id=4, grp=2, qty=21, big=false",
			"id=5, grp=1, qty=30, big=false",
			"id=7, grp=3, qty=80, big=true",
			"id=8, grp=2, qty=56, big=true",
			"id=10, grp=3, qty=85, big=true",
		],
		after_later: &[
			"id=1, grp=1, qty=65, big=true",
			"id=2, grp=3, qty=61, big=true",
			"id=4, grp=2, qty=21, big=false",
			"id=5, grp=1, qty=51, big=true",
			"id=7, grp=3, qty=80, big=true",
			"id=8, grp=2, qty=5, big=false",
			"id=11, grp=2, qty=95, big=true",
		],
	});
}

#[test]
fn a_late_filter_then_map_subscription_equals_an_early_one() {
	// A row that leaves the filter through an update of a column the map drops must still leave the late set.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | filter { grp != 3 } | map { id, qty }",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &["id=1, qty=65", "id=2, qty=61", "id=4, qty=21", "id=5, qty=30", "id=8, qty=56"],
		after_later: &["id=1, qty=65", "id=4, qty=21", "id=5, qty=51", "id=8, qty=5", "id=11, qty=95"],
	});
}

#[test]
fn a_late_take_subscription_equals_an_early_one() {
	// A window row deleted before the late create must be replaced by the next newest row in both subscriptions.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | take 3",
		history: HISTORY,
		later: LATER,
		oracle: false,
		after_history: &["id=7, grp=3, qty=80", "id=8, grp=2, qty=56", "id=10, grp=3, qty=85"],
		after_later: &["id=7, grp=3, qty=80", "id=8, grp=2, qty=5", "id=11, grp=2, qty=95"],
	});
}

#[test]
fn a_late_filter_then_take_subscription_equals_an_early_one() {
	// A window row that leaves the filter live must pull the next newest match into both windows.
	late_equals_early(Case {
		flows: false,
		ddl: &[],
		rql: "from app::t | filter { qty > 20 } | take 2",
		history: HISTORY,
		later: LATER,
		oracle: false,
		after_history: &["id=8, grp=2, qty=56", "id=10, grp=3, qty=85"],
		after_later: &["id=7, grp=3, qty=80", "id=11, grp=2, qty=95"],
	});
}

#[test]
fn a_late_subscription_over_an_append_view_equals_an_early_one() {
	// Both append branches reach the view, so a backfill that scans the view short or twice breaks the pair.
	late_equals_early(Case {
		flows: false,
		ddl: &[
			"CREATE TRANSACTIONAL VIEW app::both { id: int4, grp: int4, qty: int4 } AS { FROM app::t | append { FROM app::t | filter { qty > 50 } } }",
		],
		rql: "from app::both",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &[
			"id=1, grp=1, qty=65",
			"id=1, grp=1, qty=65",
			"id=2, grp=2, qty=61",
			"id=2, grp=2, qty=61",
			"id=4, grp=2, qty=21",
			"id=5, grp=1, qty=30",
			"id=7, grp=3, qty=80",
			"id=7, grp=3, qty=80",
			"id=8, grp=2, qty=56",
			"id=8, grp=2, qty=56",
			"id=10, grp=3, qty=85",
			"id=10, grp=3, qty=85",
		],
		after_later: &[
			"id=1, grp=1, qty=65",
			"id=1, grp=1, qty=65",
			"id=2, grp=3, qty=61",
			"id=2, grp=3, qty=61",
			"id=4, grp=2, qty=21",
			"id=5, grp=1, qty=51",
			"id=5, grp=1, qty=51",
			"id=7, grp=3, qty=80",
			"id=7, grp=3, qty=80",
			"id=8, grp=2, qty=5",
			"id=11, grp=2, qty=95",
			"id=11, grp=2, qty=95",
		],
	});
}

#[test]
fn a_late_subscription_over_a_sorted_view_equals_an_early_one() {
	// A sorted view rekeys a row when its sort value moves, so a stale key in the snapshot leaves a ghost row.
	late_equals_early(Case {
		flows: false,
		ddl: &[
			"CREATE TRANSACTIONAL VIEW app::sorted { id: int4, qty: int4 } AS { FROM app::t | filter { qty > 20 } | map { id, qty } | sort { qty } }",
		],
		rql: "from app::sorted",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &[
			"id=1, qty=65",
			"id=2, qty=61",
			"id=4, qty=21",
			"id=5, qty=30",
			"id=7, qty=80",
			"id=8, qty=56",
			"id=10, qty=85",
		],
		after_later: &[
			"id=1, qty=65",
			"id=2, qty=61",
			"id=4, qty=21",
			"id=5, qty=51",
			"id=7, qty=80",
			"id=11, qty=95",
		],
	});
}

#[test]
fn a_late_subscription_over_an_aggregate_view_equals_an_early_one() {
	// Aggregate values must equal the fold of the live rows at V, whatever updates and deletes came before it.
	late_equals_early(Case {
		flows: true,
		ddl: &[
			"CREATE DEFERRED VIEW app::totals { grp: int4, total: int4, n: int8 } AS { FROM app::t | aggregate { total: math::sum(qty), n: math::count(qty) } by { grp } }",
		],
		rql: "from app::totals",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &["grp=1, total=95, n=2", "grp=2, total=138, n=3", "grp=3, total=165, n=2"],
		after_later: &["grp=1, total=116, n=2", "grp=2, total=121, n=3", "grp=3, total=141, n=2"],
	});
}

#[test]
fn a_late_subscription_over_a_join_view_equals_an_early_one() {
	// Joined rows whose either side changed before the late create must appear once, with both sides at V.
	late_equals_early(Case {
		flows: true,
		ddl: &[
			"CREATE DEFERRED VIEW app::joined { id: int4, qty: int4, label: utf8 } AS { FROM app::t inner join { FROM app::labels } as l using (id, l.id) map { id: id, qty: qty, label: l_label } }",
		],
		rql: "from app::joined",
		history: HISTORY,
		later: LATER,
		oracle: true,
		after_history: &[
			"id=1, qty=65, label=aa",
			"id=2, qty=61, label=b",
			"id=7, qty=80, label=c",
			"id=10, qty=85, label=d",
		],
		after_later: &[
			"id=1, qty=65, label=aa",
			"id=2, qty=61, label=bb",
			"id=8, qty=5, label=f",
			"id=11, qty=95, label=e",
		],
	});
}

const RING_HISTORY: &[&str] = &[
	"INSERT app::rb [{id: 1, qty: 10}, {id: 2, qty: 60}, {id: 3, qty: 70}]",
	"INSERT app::rb [{id: 4, qty: 20}, {id: 5, qty: 90}]",
	"UPDATE app::rb { qty: 65 } FILTER { id == 4 }",
	"DELETE app::rb FILTER { id == 3 }",
	"INSERT app::rb [{id: 6, qty: 55}, {id: 7, qty: 15}]",
];

const RING_LATER: &[&str] = &[
	"INSERT app::rb [{id: 8, qty: 99}]",
	"UPDATE app::rb { qty: 51 } FILTER { id == 7 }",
	"DELETE app::rb FILTER { id == 5 }",
];

#[test]
fn a_late_subscription_over_a_ring_buffer_equals_an_early_one() {
	// Rows evicted from the ring before the late create must stay out of its backfill, as they left the early one.
	late_equals_early(Case {
		flows: false,
		ddl: &["CREATE RINGBUFFER app::rb { id: int4, qty: int4 } WITH { capacity: 4 }"],
		rql: "from app::rb | filter { qty > 50 }",
		history: RING_HISTORY,
		later: RING_LATER,
		oracle: true,
		after_history: &["id=4, qty=65", "id=5, qty=90", "id=6, qty=55"],
		after_later: &["id=6, qty=55", "id=7, qty=51", "id=8, qty=99"],
	});
}

fn collect_embedded(sub: &Subscription) -> Vec<StagedBatch> {
	let mut out: Vec<StagedBatch> = Vec::new();
	let mut quiet = 0;
	let deadline = Clock::Real.instant() + Duration::from_seconds(10).unwrap().to_std();
	while quiet < 5 && Clock::Real.instant() < deadline {
		let frames = sub.drain(usize::MAX);
		if frames.is_empty() {
			quiet += 1;
		} else {
			quiet = 0;
			out.extend(frames.into_iter().map(|frame| (frame.op.unwrap_or(DiffType::Insert), frame.batch)));
		}
		thread::sleep(Duration::from_milliseconds(20).unwrap().to_std());
	}
	out
}

#[test]
fn a_late_embedded_subscription_equals_an_early_one() {
	// The embedded handle leases and backfills by itself; its prelude must hand off to live with no gap or double.
	let db = make_db();
	let rql = "from app::t | filter { qty > 50 }";
	let early = db.subscribe_as_root(rql, Params::None, HydrationConfig::default()).expect("early subscribe");
	let mut early_rows = Rows::default();
	early_rows.apply("early", &collect_embedded(&early));
	for rql in HISTORY {
		write(&db, rql);
	}
	settle(&db);
	early_rows.apply("early", &collect_embedded(&early));
	let late = db.subscribe_as_root(rql, Params::None, HydrationConfig::default()).expect("late subscribe");
	let mut late_rows = Rows::default();
	late_rows.apply("late", &collect_embedded(&late));
	assert_eq!(early_rows.values(), sorted(FILTER_AFTER_HISTORY), "the early handle holds the wrong rows");
	assert_eq!(late_rows.held, early_rows.held, "the late handle must hold the early one's rows");
	for rql in LATER {
		write(&db, rql);
	}
	settle(&db);
	early_rows.apply("early", &collect_embedded(&early));
	late_rows.apply("late", &collect_embedded(&late));
	assert!(early_rows.faults.is_empty(), "the early handle saw {:#?}", early_rows.faults);
	assert!(late_rows.faults.is_empty(), "the late handle saw {:#?}", late_rows.faults);
	assert_eq!(early_rows.values(), sorted(FILTER_AFTER_LATER), "the early handle holds the wrong rows");
	assert_eq!(late_rows.held, early_rows.held, "the late handle must still hold the early one's rows");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
	Open,
	Next(u64),
}

struct WriteDuring {
	engine: StandardEngine,
	at: Point,
	writes: Vec<String>,
	seen: Mutex<Vec<(ObjectId, Point)>>,
	landed: Mutex<Option<CommitVersion>>,
}

impl WriteDuring {
	fn reach(&self, source: ObjectId, point: Point) {
		self.seen.lock().push((source, point));
		if point != self.at || self.landed.lock().is_some() || self.writes.is_empty() {
			return;
		}
		for rql in &self.writes {
			self.engine.clock().as_mock().expect("mock clock").advance_secs(1);
			if let Some(error) = self.engine.command_as(IdentityId::root(), rql, Params::None).error {
				panic!("the write `{rql}` inside the backfill failed: {error:?}");
			}
		}
		*self.landed.lock() = Some(self.engine.current_version().expect("current version"));
	}

	fn landed(&self) -> CommitVersion {
		self.landed.lock().expect("the hook never wrote, so this test checks nothing")
	}

	fn pulls(&self, source: ObjectId) -> usize {
		self.seen
			.lock()
			.iter()
			.filter(|(seen, point)| *seen == source && matches!(point, Point::Next(_)))
			.count()
	}
}

impl ScanHooks for WriteDuring {
	fn during_open(&self, source: ObjectId) {
		self.reach(source, Point::Open);
	}

	fn during_next(&self, source: ObjectId, pull: u64) {
		self.reach(source, Point::Next(pull));
	}
}

fn install(db: &TestDb, at: Point, writes: &[&str]) -> Arc<WriteDuring> {
	let hook = Arc::new(WriteDuring {
		engine: db.engine().clone(),
		at,
		writes: writes.iter().map(|rql| rql.to_string()).collect(),
		seen: Mutex::new(Vec::new()),
		landed: Mutex::new(None),
	});
	db.engine().ioc().register_service(InstalledScanHooks(hook.clone()));
	hook
}

fn uninstall(db: &TestDb) {
	db.engine().ioc().register_service(InstalledScanHooks(Arc::new(NoFaults)));
}

fn table_object(db: &TestDb, name: &str) -> ObjectId {
	let engine = db.engine();
	let mut query = engine.begin_query(IdentityId::system()).expect("query transaction");
	let mut txn = Transaction::Query(&mut query);
	let catalog = engine.catalog();
	let namespace =
		catalog.find_namespace_by_name(&mut txn, "app").expect("namespace lookup").expect("namespace app");
	let table = catalog.find_table_by_name(&mut txn, namespace.id(), name).expect("table lookup").expect("table");
	ObjectId::Table(table.id)
}

fn backfill_with_writes(db: &TestDb, rql: &str, at: Point, writes: &[&str]) -> (Watcher, Arc<WriteDuring>) {
	let hook = install(db, at, writes);
	let mut watcher = Watcher::open(db, IdentityId::root(), rql);
	uninstall(db);
	let landed = hook.landed();
	assert!(landed > watcher.version, "the hook wrote at {landed:?}, not after V = {:?}", watcher.version);
	let table = table_object(db, "t");
	let seen = hook.seen.lock().clone();
	assert!(seen.iter().all(|(source, _)| *source == table), "the backfill scanned more than app::t: {seen:?}");
	watcher.catch_up(db);
	(watcher, hook)
}

#[test]
fn writes_before_the_first_chunk_arrive_live_exactly_once() {
	// A write after V that the snapshot also sees, or that the hand-off drops, shows up as a double or a gap.
	let db = ten_rows();
	let (watcher, _) = backfill_with_writes(
		&db,
		"from app::t",
		Point::Open,
		&[
			"INSERT app::t [{id: 11, grp: 2, qty: 110}]",
			"UPDATE app::t { qty: 35 } FILTER { id == 3 }",
			"DELETE app::t FILTER { id == 5 }",
		],
	);
	assert_eq!(inserted_ids(&watcher.snapshot), (1..=10).collect::<Vec<_>>(), "the snapshot must be the rows at V");
	assert!(
		render_all(&watcher.snapshot).contains(&"id=3, grp=0, qty=30".to_string()),
		"the snapshot must hold the value at V, not the later update"
	);
	assert_eq!(
		announced(&watcher.live),
		vec![(DiffType::Insert, 11), (DiffType::Update, 3), (DiffType::Remove, 5)],
		"each write after V must arrive live exactly once, in commit order"
	);
	watcher.assert_clean("after the hand-off");
	assert_eq!(watcher.rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
}

#[test]
fn writes_between_chunks_arrive_live_exactly_once() {
	// Writes after the first chunk hit rows on both sides of the scan position; a scan must never read past V.
	let db = ten_rows();
	let (watcher, hook) = backfill_with_writes(
		&db,
		"from app::t",
		Point::Next(1),
		&[
			"INSERT app::t [{id: 11, grp: 2, qty: 110}]",
			"UPDATE app::t { qty: 15 } FILTER { id == 1 }",
			"UPDATE app::t { qty: 105 } FILTER { id == 10 }",
			"DELETE app::t FILTER { id == 2 }",
			"DELETE app::t FILTER { id == 9 }",
		],
	);
	assert!(
		hook.pulls(table_object(&db, "t")) >= 3,
		"ten rows at batch size 4 need at least three pulls, so the writes did not land between chunks"
	);
	assert_eq!(inserted_ids(&watcher.snapshot), (1..=10).collect::<Vec<_>>(), "the snapshot must be the rows at V");
	let snapshot = render_all(&watcher.snapshot);
	for at_v in ["id=1, grp=1, qty=10", "id=10, grp=1, qty=100"] {
		assert!(snapshot.contains(&at_v.to_string()), "the snapshot must hold {at_v}, the value at V");
	}
	assert_eq!(
		announced(&watcher.live),
		vec![
			(DiffType::Insert, 11),
			(DiffType::Update, 1),
			(DiffType::Update, 10),
			(DiffType::Remove, 2),
			(DiffType::Remove, 9)
		],
		"each write after V must arrive live exactly once, in commit order"
	);
	watcher.assert_clean("after the hand-off");
	assert_eq!(watcher.rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
}

#[test]
fn writes_between_chunks_cross_a_filter_exactly_once() {
	// Live updates must meet the filter state the snapshot left, or a row crossing the filter is lost or doubled.
	let db = ten_rows();
	let rql = "from app::t | filter { qty > 50 }";
	let (watcher, _) = backfill_with_writes(
		&db,
		rql,
		Point::Next(1),
		&[
			"UPDATE app::t { qty: 95 } FILTER { id == 2 }",
			"UPDATE app::t { qty: 15 } FILTER { id == 9 }",
			"INSERT app::t [{id: 11, grp: 2, qty: 5}, {id: 12, grp: 0, qty: 99}]",
			"DELETE app::t FILTER { id == 10 }",
		],
	);
	assert_eq!(
		inserted_ids(&watcher.snapshot),
		vec![6, 7, 8, 9, 10],
		"the snapshot must be the matching rows at V"
	);
	watcher.assert_clean("after the hand-off");
	assert_eq!(
		watcher.rows.values(),
		sorted(&[
			"id=2, grp=2, qty=95",
			"id=6, grp=0, qty=60",
			"id=7, grp=1, qty=70",
			"id=8, grp=2, qty=80",
			"id=12, grp=0, qty=99"
		]),
		"the subscriber holds the wrong rows"
	);
	assert_eq!(watcher.rows.values(), query_rows(&db.query(rql)), "the rows differ from a fresh query");
}

#[test]
fn writes_before_the_first_chunk_move_a_take_window_exactly_once() {
	// A live remove applied before the snapshot leaves the deleted row in the window for good.
	let db = ten_rows();
	let (watcher, _) = backfill_with_writes(
		&db,
		"from app::t | take 3",
		Point::Open,
		&["DELETE app::t FILTER { id == 9 }", "INSERT app::t [{id: 11, grp: 2, qty: 110}]"],
	);
	assert_eq!(inserted_ids(&watcher.snapshot), vec![8, 9, 10], "the snapshot window must be the newest rows at V");
	watcher.assert_clean("after the hand-off");
	assert_eq!(
		watcher.rows.values(),
		sorted(&["id=8, grp=2, qty=80", "id=10, grp=1, qty=100", "id=11, grp=2, qty=110"]),
		"the window must be the three newest rows after the live writes"
	);
}

#[test]
fn writes_between_chunks_move_a_take_window_only_after_the_whole_snapshot() {
	// A live remove applied between chunks can be undone by a later chunk that still holds the row at V.
	let db = ten_rows();
	let (watcher, hook) = backfill_with_writes(
		&db,
		"from app::t | take 3",
		Point::Next(1),
		&["DELETE app::t FILTER { id == 9 }", "INSERT app::t [{id: 11, grp: 2, qty: 110}]"],
	);
	assert!(hook.pulls(table_object(&db, "t")) >= 3, "the writes must land between chunks of the backfill");
	assert_eq!(inserted_ids(&watcher.snapshot), vec![8, 9, 10], "the snapshot window must be the newest rows at V");
	watcher.assert_clean("after the hand-off");
	assert_eq!(
		watcher.rows.values(),
		sorted(&["id=8, grp=2, qty=80", "id=10, grp=1, qty=100", "id=11, grp=2, qty=110"]),
		"the window must be the three newest rows after the live writes"
	);
}

fn render_all(batches: &[StagedBatch]) -> Vec<String> {
	batches.iter().flat_map(|(_, batch)| render(batch)).collect()
}

const WINDOW_WRITES: &[&str] = &[
	"INSERT app::t [{id: 11, grp: 2, qty: 110}]",
	"UPDATE app::t { qty: 35 } FILTER { id == 3 }",
	"DELETE app::t FILTER { id == 5 }",
];

#[test]
fn a_write_after_register_and_at_or_below_v_arrives_once_from_the_snapshot() {
	// The subscriber sees these writes live before V is leased, so keeping their live change announces them twice.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t");
	let registered = current(&db);
	for rql in WINDOW_WRITES {
		write(&db, rql);
	}
	settle(&db);
	let written = current(&db);
	let (version, lease) = lease(&db);
	assert!(registered < written && written <= version, "the writes must commit in (register, V]");
	let outcome = hydrate(&db, id, IdentityId::root(), lease, MAX_ROWS).expect("the backfill must succeed");
	let mut watcher = Watcher::hydrated(id, version, outcome);
	watcher.catch_up(&db);
	assert_eq!(
		inserted_ids(&watcher.snapshot),
		vec![1, 2, 3, 4, 6, 7, 8, 9, 10, 11],
		"the snapshot must hold every write at or below V"
	);
	assert!(
		render_all(&watcher.snapshot).contains(&"id=3, grp=0, qty=35".to_string()),
		"the snapshot must hold the update at or below V"
	);
	assert_eq!(announced(&watcher.live), Vec::new(), "every live change at or below V must be dropped");
	watcher.assert_clean("after the hand-off");
	assert_eq!(watcher.rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
	write(&db, "UPDATE app::t { qty: 111 } FILTER { id == 11 }");
	watcher.catch_up(&db);
	assert_eq!(
		announced(&watcher.live),
		vec![(DiffType::Update, 11)],
		"a write after the hand-off must still arrive live, once"
	);
	watcher.assert_clean("after a later write");
	assert_eq!(watcher.rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
}

#[test]
fn a_write_after_register_and_at_or_below_v_moves_a_take_window_once() {
	// Take state fed the live remove before the snapshot would re-admit the deleted row or evict a survivor.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t | take 3");
	write(&db, "DELETE app::t FILTER { id == 9 }");
	write(&db, "INSERT app::t [{id: 11, grp: 2, qty: 110}]");
	settle(&db);
	let (version, lease) = lease(&db);
	let outcome = hydrate(&db, id, IdentityId::root(), lease, MAX_ROWS).expect("the backfill must succeed");
	let mut watcher = Watcher::hydrated(id, version, outcome);
	watcher.catch_up(&db);
	assert_eq!(
		inserted_ids(&watcher.snapshot),
		vec![8, 10, 11],
		"the snapshot window must be the newest rows at V"
	);
	assert_eq!(announced(&watcher.live), Vec::new(), "every live change at or below V must be dropped");
	watcher.assert_clean("after the hand-off");
}

#[test]
fn a_write_after_the_lease_and_before_the_backfill_arrives_once_live() {
	// The snapshot at V cannot see these writes, so dropping their live change loses them for good.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t");
	let (version, lease) = lease(&db);
	for rql in WINDOW_WRITES {
		write(&db, rql);
	}
	settle(&db);
	assert!(current(&db) > version, "the writes must commit after V");
	let outcome = hydrate(&db, id, IdentityId::root(), lease, MAX_ROWS).expect("the backfill must succeed");
	let mut watcher = Watcher::hydrated(id, version, outcome);
	watcher.catch_up(&db);
	assert_eq!(inserted_ids(&watcher.snapshot), (1..=10).collect::<Vec<_>>(), "the snapshot must be the rows at V");
	assert!(
		render_all(&watcher.snapshot).contains(&"id=3, grp=0, qty=30".to_string()),
		"the snapshot must hold the value at V, not the later update"
	);
	assert_eq!(
		announced(&watcher.live),
		vec![(DiffType::Insert, 11), (DiffType::Update, 3), (DiffType::Remove, 5)],
		"each write after V must arrive live exactly once, in commit order"
	);
	watcher.assert_clean("after the hand-off");
	assert_eq!(watcher.rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
}

#[test]
fn a_write_after_the_lease_and_before_the_backfill_moves_a_take_window_once() {
	// Take state fed these live changes before the snapshot keeps the deleted row 9 in the window.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t | take 3");
	let (version, lease) = lease(&db);
	write(&db, "DELETE app::t FILTER { id == 9 }");
	write(&db, "INSERT app::t [{id: 11, grp: 2, qty: 110}]");
	settle(&db);
	let outcome = hydrate(&db, id, IdentityId::root(), lease, MAX_ROWS).expect("the backfill must succeed");
	let mut watcher = Watcher::hydrated(id, version, outcome);
	watcher.catch_up(&db);
	assert_eq!(inserted_ids(&watcher.snapshot), vec![8, 9, 10], "the snapshot window must be the newest rows at V");
	watcher.assert_clean("after the hand-off");
	assert_eq!(
		watcher.rows.values(),
		sorted(&["id=8, grp=2, qty=80", "id=10, grp=1, qty=100", "id=11, grp=2, qty=110"]),
		"the window must be the three newest rows after the live writes"
	);
}

struct WriteDuringHandOff {
	engine: StandardEngine,
	store: Arc<SubscriptionStore>,
	writes: Vec<String>,
	seen: Mutex<Vec<(SubscriptionId, bool)>>,
	window: Mutex<Option<(CommitVersion, CommitVersion)>>,
}

impl WriteDuringHandOff {
	fn window(&self) -> (CommitVersion, CommitVersion) {
		self.window.lock().expect("the hand-off hook never wrote, so this test checks nothing")
	}
}

impl HandOffHooks for WriteDuringHandOff {
	fn during_hand_off(&self, subscription: SubscriptionId) {
		self.seen.lock().push((subscription, self.store.contains(&subscription)));
		if self.window.lock().is_some() {
			return;
		}
		let before = self.engine.current_version().expect("current version");
		for rql in &self.writes {
			self.engine.clock().as_mock().expect("mock clock").advance_secs(1);
			if let Some(error) = self.engine.command_as(IdentityId::root(), rql, Params::None).error {
				panic!("the write `{rql}` inside the hand-off failed: {error:?}");
			}
		}
		let after = self.engine.current_version().expect("current version");
		*self.window.lock() = Some((before, after));
	}
}

struct NoHandOff;

impl HandOffHooks for NoHandOff {}

#[test]
fn a_write_inside_the_embedded_hand_off_window_arrives_once_from_the_prelude() {
	// A write between register and the lease is both live and in the snapshot; keeping both announces it twice.
	let db = ten_rows();
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let hook = Arc::new(WriteDuringHandOff {
		engine: db.engine().clone(),
		store: subsystem.store().clone(),
		writes: WINDOW_WRITES.iter().map(|rql| rql.to_string()).collect(),
		seen: Mutex::new(Vec::new()),
		window: Mutex::new(None),
	});
	db.engine().ioc().register_service(InstalledHandOffHooks(hook.clone()));
	let sub = db.subscribe_as_root("from app::t", Params::None, HydrationConfig::default()).expect("subscribe");
	db.engine().ioc().register_service(InstalledHandOffHooks(Arc::new(NoHandOff)));
	assert_eq!(
		hook.seen.lock().clone(),
		vec![(sub.id(), true)],
		"the hand-off hook must fire once, for this subscription, after it is registered"
	);
	let (before, after) = hook.window();
	assert!(before < after, "the hook writes must commit inside the hand-off window");
	settle(&db);
	let prelude = collect_embedded(&sub);
	assert_eq!(
		inserted_ids(&prelude),
		vec![1, 2, 3, 4, 6, 7, 8, 9, 10, 11],
		"the prelude must hold each window write once from the snapshot, and no live change for them"
	);
	assert!(
		render_all(&prelude).contains(&"id=3, grp=0, qty=35".to_string()),
		"the prelude must hold the update made inside the window"
	);
	let mut rows = Rows::default();
	rows.apply("prelude", &prelude);
	assert!(rows.faults.is_empty(), "the prelude handle saw {:#?}", rows.faults);
	assert_eq!(rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
	write(&db, "UPDATE app::t { qty: 111 } FILTER { id == 11 }");
	settle(&db);
	let live = collect_embedded(&sub);
	assert_eq!(
		announced(&live),
		vec![(DiffType::Update, 11)],
		"no window write may arrive live late, and a write after the hand-off must arrive live once"
	);
	rows.apply("live", &live);
	assert!(rows.faults.is_empty(), "the handle saw {:#?} after a later write", rows.faults);
	assert_eq!(rows.values(), query_rows(&db.query("from app::t")), "the rows differ from a fresh query");
}

fn lookup_identity(db: &TestDb, name: &str) -> IdentityId {
	let frames = db.query(&format!("from system::identities filter {{ name == '{name}' }}"));
	let column = frames.first().expect("identity frame").column("id").unwrap().expect("id column");
	match column.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value: {other:?}"),
	}
}

fn owner(id: IdentityId) -> String {
	format!("cast('{id}', identity_id)")
}

#[test]
fn a_policy_scoped_subscriber_backfills_only_the_rows_the_policy_allows() {
	// Scan nodes apply no row policy, so a backfill that skips the subscriber's policy hands alice bob's rows.
	let db = make_db();
	db.admin("CREATE TABLE app::docs { id: int4, owner: identity_id, body: utf8 }");
	db.admin("create user alice");
	db.admin("create user bob");
	db.admin("create session policy allow_subscribe { subscription: { filter { true } } }");
	db.admin("create table policy docs_owner on app::docs { from: { filter { owner == $identity.id } } }");
	let alice = lookup_identity(&db, "alice");
	let bob = lookup_identity(&db, "bob");
	let (a, b) = (owner(alice), owner(bob));
	let rql = "from app::docs";
	let mut early = Watcher::open(&db, alice, rql);
	for statement in [
		format!(
			"INSERT app::docs [{{id: 1, owner: {a}, body: 'a1'}}, {{id: 2, owner: {b}, body: 'b2'}}, {{id: 3, owner: {a}, body: 'a3'}}, {{id: 4, owner: {b}, body: 'b4'}}, {{id: 5, owner: {a}, body: 'a5'}}, {{id: 6, owner: {b}, body: 'b6'}}]"
		),
		format!("UPDATE app::docs {{ owner: {a} }} FILTER {{ id == 2 }}"),
		format!("UPDATE app::docs {{ owner: {b} }} FILTER {{ id == 3 }}"),
		"DELETE app::docs FILTER { id == 5 }".to_string(),
		"UPDATE app::docs { body: 'x1' } FILTER { id == 1 }".to_string(),
	] {
		write(&db, &statement);
	}
	settle(&db);
	early.catch_up(&db);
	let mut late = Watcher::open(&db, alice, rql);
	let mut root = Watcher::open(&db, IdentityId::root(), rql);
	assert_eq!(inserted_ids(&late.snapshot), vec![1, 2], "alice's backfill must hold exactly her rows at V");
	assert_eq!(inserted_ids(&root.snapshot), vec![1, 2, 3, 4, 6], "root bypasses the policy and sees every row");
	let alice_rows = || query_rows(&db.query_as(alice, rql, Params::None).expect("query as alice"));
	assert_eq!(late.rows.held, early.rows.held, "alice's late backfill must equal her live-built rows");
	assert_eq!(late.rows.values(), alice_rows(), "alice's backfill must equal her own fresh query");
	for statement in [
		format!("INSERT app::docs [{{id: 7, owner: {b}, body: 'b7'}}, {{id: 8, owner: {a}, body: 'a8'}}]"),
		format!("UPDATE app::docs {{ owner: {a} }} FILTER {{ id == 4 }}"),
		format!("UPDATE app::docs {{ owner: {b} }} FILTER {{ id == 2 }}"),
	] {
		write(&db, &statement);
	}
	settle(&db);
	early.catch_up(&db);
	late.catch_up(&db);
	root.catch_up(&db);
	early.assert_clean("early");
	late.assert_clean("late");
	root.assert_clean("root");
	assert_eq!(held_ids(&late.rows), vec![1, 4, 8], "alice must hold exactly her rows after the live writes");
	assert_eq!(late.rows.held, early.rows.held, "alice's late subscription must still equal her early one");
	assert_eq!(late.rows.values(), alice_rows(), "alice's rows must equal her own fresh query");
	assert_eq!(root.rows.values(), query_rows(&db.query(rql)), "root's rows must equal a fresh query");
}

fn insert_ids(table: &str, ids: impl Iterator<Item = i32>) -> String {
	let rows: Vec<String> = ids.map(|id| format!("{{id: {id}, grp: {}, qty: {}}}", id % 3, id * 10)).collect();
	format!("INSERT {table} [{}]", rows.join(", "))
}

#[test]
fn a_backfill_of_exactly_max_rows_delivers_every_row() {
	// A cap check that counts one chunk too many or uses >= refuses a snapshot that fits.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t");
	let (version, lease) = lease(&db);
	let outcome = hydrate(&db, id, IdentityId::root(), lease, 10).expect("ten rows fit a cap of ten");
	let watcher = Watcher::hydrated(id, version, outcome);
	assert_eq!(inserted_ids(&watcher.snapshot), (1..=10).collect::<Vec<_>>(), "every row at V must be delivered");
}

#[test]
fn a_backfill_one_row_over_max_rows_fails_with_row_cap_exceeded() {
	// Nine is not a multiple of the batch size, so a cap checked only per full chunk lets the tenth row through.
	let db = ten_rows();
	let id = subscribe(&db, IdentityId::root(), "from app::t");
	let (_, lease) = lease(&db);
	match hydrate(&db, id, IdentityId::root(), lease, 9) {
		Err(HydrateError::RowCapExceeded {
			cap,
			bound,
		}) => {
			assert_eq!(cap, 9, "the error must name the cap the subscriber asked for");
			assert_eq!(
				bound,
				HydrationBound::Absent,
				"a plain from has no bound, so the advice must be to add one"
			);
		}
		Err(other) => panic!("expected RowCapExceeded, got {other:?}"),
		Ok(outcome) => {
			panic!("ten rows passed a cap of nine and delivered {:?}", inserted_ids(&outcome.batches))
		}
	}
}

#[test]
fn the_row_cap_counts_the_rows_at_v_only() {
	// Rows deleted before V or written after V are not in the snapshot; counting them refuses a fitting backfill.
	let db = make_db();
	write(&db, &insert_ids("app::t", 1..=14));
	write(&db, "DELETE app::t FILTER { id > 10 }");
	let hook = install(&db, Point::Open, &[&insert_ids("app::t", 21..=23)]);
	let id = subscribe(&db, IdentityId::root(), "from app::t");
	let (version, lease) = lease(&db);
	let outcome = hydrate(&db, id, IdentityId::root(), lease, 10);
	uninstall(&db);
	let outcome = outcome.expect("ten rows at V fit a cap of ten");
	assert!(hook.landed() > version, "the hook must write after V");
	let mut watcher = Watcher::hydrated(id, version, outcome);
	watcher.catch_up(&db);
	assert_eq!(inserted_ids(&watcher.snapshot), (1..=10).collect::<Vec<_>>(), "the snapshot must be the rows at V");
	assert_eq!(
		announced(&watcher.live),
		vec![(DiffType::Insert, 21), (DiffType::Insert, 22), (DiffType::Insert, 23)],
		"the writes after V must arrive live, outside the cap"
	);
	watcher.assert_clean("after the hand-off");
}

#[test]
fn a_source_larger_than_the_batch_size_is_backfilled_completely_and_exactly_once() {
	// Holes from deleted rows end chunks early; a resume that skips or repeats a key shows as a gap or double.
	let db = make_db();
	write(&db, &insert_ids("app::t", 1..=10));
	write(&db, &insert_ids("app::t", 11..=20));
	write(&db, &insert_ids("app::t", 21..=23));
	write(&db, "DELETE app::t FILTER { id == 4 or id == 5 or id == 17 }");
	let hook = install(&db, Point::Open, &[]);
	let watcher = Watcher::open(&db, IdentityId::root(), "from app::t");
	uninstall(&db);
	let want: Vec<i32> = (1..=23).filter(|id| ![4, 5, 17].contains(id)).collect();
	assert_eq!(inserted_ids(&watcher.snapshot), want, "every row at V must be delivered exactly once");
	watcher.assert_clean("after the backfill");
	assert!(
		hook.pulls(table_object(&db, "t")) >= 5,
		"20 rows at batch size 4 need at least five pulls, so the batch size was not honoured: {:?}",
		hook.seen.lock()
	);
}
