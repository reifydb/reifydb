// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	thread,
};

use reifydb::{Params, testing::db::TestDb};
use reifydb_core::interface::{catalog::id::SubscriptionId, change::StagedBatch};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, HydrationBound, SubscriptionServiceRef},
};
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::value::{
	Value, datetime::DateTime, diff_type::DiffType, duration::Duration, frame::frame::Frame, identity::IdentityId,
};

fn extract_sub_id(frames: &[Frame]) -> SubscriptionId {
	let frame = frames.first().expect("subscription frame");
	let value = frame
		.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.and_then(|c| {
			if c.data.is_empty() {
				None
			} else {
				Some(c.data.get_value(0))
			}
		})
		.expect("subscription_id column");
	match value {
		Value::Uint8(n) => SubscriptionId(n),
		other => panic!("unexpected subscription_id value: {:?}", other),
	}
}

fn engine_lease_service(db: &TestDb) -> (StandardEngine, VersionLeaseGuard, SubscriptionServiceRef) {
	let engine = db.engine().clone();
	let (_, lease) = engine.acquire_current_snapshot_lease().expect("acquire lease");
	let sub_service = engine.services().ioc.resolve::<SubscriptionServiceRef>().expect("resolve service");
	(engine, lease, sub_service)
}

fn seed_id_qty(db: &TestDb, table: &str, rows: usize) {
	let mut insert_stmt = format!("INSERT {} [", table);
	for i in 0..rows {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}, qty: {}}}", i, i * 2));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);
}

fn create_and_setup(
	db: &TestDb,
	query: &str,
) -> (StandardEngine, SubscriptionId, VersionLeaseGuard, SubscriptionServiceRef) {
	let stmt = format!("CREATE SUBSCRIPTION AS {{ {} }}", query);
	let frames = db.admin(&stmt);
	let sub_id = extract_sub_id(&frames);
	let (engine, lease, sub_service) = engine_lease_service(db);
	thread::sleep(Duration::from_milliseconds(50).unwrap().to_std());
	(engine, sub_id, lease, sub_service)
}

#[test]
fn hydrate_returns_existing_rows_at_pinned_version() {
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::orders { id: int4, qty: int4 }");

	db.command("INSERT app::orders [{id: 1, qty: 10}, {id: 2, qty: 20}, {id: 3, qty: 30}]");

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::orders");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 1024).expect("hydrate succeeds");

	let total_rows: usize = outcome.batches.iter().map(|(_, c)| c.row_count()).sum();
	assert_eq!(total_rows, 3, "snapshot should contain 3 seeded rows");
}

#[test]
fn hydrate_500_rows_stages_scan_frame_batches_not_one_per_row() {
	// Batch count must track scan frames, never row count: a Change per row re-pays the fixed process cost per row.
	const ROWS: usize = 500;

	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::wide { id: int4 }");

	let mut insert_stmt = String::from("INSERT app::wide [");
	for i in 0..ROWS {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}}}", i));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::wide");

	let outcome =
		sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, ROWS as u64).expect("hydrate succeeds");

	let total_rows: usize = outcome.batches.iter().map(|(_, c)| c.row_count()).sum();
	assert_eq!(total_rows, ROWS, "batching must not drop or duplicate snapshot rows");

	assert!(
		outcome.batches.len() <= 8,
		"snapshot of {} rows staged {} batches; batch count must follow scan frames, not row count",
		ROWS,
		outcome.batches.len()
	);
}

#[test]
fn hydrate_delivers_every_row_of_a_snapshot_larger_than_the_delivery_ring() {
	// Take stages one batch per admitted row, so 2000 rows overrun the 1024-batch ring and evict the oldest before
	// drain.
	const ROWS: usize = 2000;

	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::deep { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::deep", ROWS);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::deep | take 2000");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 5000).expect("hydrate succeeds");

	let total_rows: usize = outcome.batches.iter().map(|(_, c)| c.row_count()).sum();
	assert_eq!(total_rows, ROWS, "snapshot must carry every row the query returned, not the ring's last 1024");
}

fn seed_backdated(db: &TestDb, table: &str, rows: &[(i32, u64)]) {
	// Row numbers ascend with insertion order while created_at does not, which is what a backfill produces.
	for (id, at_millis) in rows {
		db.mock_clock().set_millis(*at_millis);
		db.command(&format!("INSERT {} [{{id: {}, qty: {}}}]", table, id, id * 2));
	}
}

fn announced_ids(batches: &[StagedBatch]) -> Vec<i32> {
	let mut out = Vec::new();
	for (_, batch) in batches {
		let id_col = batch.iter().find(|c| c.name().text() == "id").expect("id column");
		for row_idx in 0..batch.row_count() {
			match id_col.data().get_value(row_idx) {
				Value::Int4(v) => out.push(v),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	out
}

#[test]
fn hydrate_take_selects_the_newest_by_created_at_not_by_row_number() {
	// A pushed take cuts by row number while the operator keeps the newest created_at, so a backfill disagrees.
	let db = TestDb::builder().mock_time(DateTime::from_millis(1_000)).memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::backfill { id: int4, qty: int4 }");

	seed_backdated(&db, "app::backfill", &[(1, 5_000), (2, 4_000), (3, 3_000), (4, 2_000), (5, 1_000)]);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::backfill | take 2");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 50).expect("hydrate succeeds");

	let mut got = announced_ids(&outcome.batches);
	got.sort();
	assert_eq!(got, vec![1, 2], "take 2 must announce the two newest rows by created_at, which are ids 1 and 2");
}

#[test]
fn hydrate_take_breaks_created_at_ties_by_row_number() {
	// One bulk insert stamps every row with the same created_at, so only the row-number tiebreak picks the window.
	let db = TestDb::builder().mock_time(DateTime::from_millis(1_000)).memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::tied { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::tied", 20);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::tied | take 3");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 50).expect("hydrate succeeds");

	let mut got = announced_ids(&outcome.batches);
	got.sort();
	assert_eq!(got, vec![17, 18, 19], "tied created_at must fall back to the highest row numbers, ids 17..19");
}

#[test]
fn hydrate_snapshot_announces_inserts_only() {
	// A hydration starts from an empty subscriber, so any update or remove retracts a row the same snapshot just
	// announced.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::churn { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::churn", 50);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::churn | take 5");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 50).expect("hydrate succeeds");

	for (op, _) in &outcome.batches {
		assert_eq!(*op, DiffType::Insert, "hydration snapshot must announce inserts only, saw op={:?}", op);
	}
}

#[test]
fn hydrate_never_announces_a_remove_for_a_row_it_did_not_announce() {
	// A retraction for a row the subscriber never received is a phantom it cannot apply, which desyncs the client
	// forever.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::phantom { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::phantom", 50);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::phantom | take 5");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 50).expect("hydrate succeeds");

	let mut announced: HashSet<u64> = HashSet::new();
	let mut seen = 0usize;
	for (op, batch) in &outcome.batches {
		let row_numbers = batch.row_numbers();
		assert_eq!(
			row_numbers.len(),
			batch.row_count(),
			"row numbers must cover the batch or this guard cannot identify the rows it checks"
		);
		for row_idx in 0..batch.row_count() {
			let row = row_numbers[row_idx].value();
			seen += 1;
			match op {
				DiffType::Insert => {
					announced.insert(row);
				}
				DiffType::Update | DiffType::Remove => assert!(
					announced.contains(&row),
					"an op targeting row {} arrived before any batch announced it",
					row
				),
			}
		}
	}

	assert!(seen > 0, "the fixture must deliver rows or this guard passes vacuously");
}

#[test]
fn hydrate_fails_when_row_cap_exceeded() {
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::big { id: int4 }");

	let mut insert_stmt = String::from("INSERT app::big [");
	for i in 0..50 {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}}}", i));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::big");

	let err = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect_err("expected RowCapExceeded");

	match err {
		HydrateError::RowCapExceeded {
			cap,
			bound,
		} => {
			assert_eq!(cap, 10);
			// The query carries no bound at all, so telling the user to add one is the right advice.
			assert_eq!(bound, HydrationBound::Absent);
		}
		other => panic!("unexpected error: {:?}", other),
	}
}

#[test]
fn hydrate_pushes_take_into_source_query() {
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::big { id: int4 }");

	let mut insert_stmt = String::from("INSERT app::big [");
	for i in 0..50 {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}}}", i));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::big | take 5");

	sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect("hydrate succeeds: take 5 should be pushed into source so cap=10 holds");
}

#[test]
fn hydrate_pushes_filter_into_source_query() {
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::events { id: int4, kind: utf8 }");

	let mut insert_stmt = String::from("INSERT app::events [");
	let mut first = true;
	for kind in ["a", "b", "c"] {
		for i in 0..100 {
			if !first {
				insert_stmt.push(',');
			}
			first = false;
			insert_stmt.push_str(&format!("{{id: {}, kind: '{}'}}", i, kind));
		}
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);

	let (engine, sub_id, lease, sub_service) =
		create_and_setup(&db, "from app::events | filter { kind == 'b' } | take 5");

	// The filter must reach the source query, or the 5-row take selects 5 'a' rows that the in-flow filter
	// then discards, leaving the snapshot empty. The cap is the take limit, so both variants fit under it
	// and the assertion rests on the rows matching the filter, not on a cap-exceeded error.
	let outcome = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("hydrate succeeds at cap=5 (matches TAKE 5)");

	let total_rows: usize = outcome.batches.iter().map(|(_, c)| c.row_count()).sum();
	assert!(total_rows > 0, "snapshot must deliver at least one filtered row");

	for (_, cols) in &outcome.batches {
		let kind_col = cols.iter().find(|c| c.name() == "kind").expect("kind column present");
		for i in 0..cols.row_count() {
			match kind_col.data().get_value(i) {
				Value::Utf8(s) => assert_eq!(s, "b", "filter must restrict to kind == 'b'"),
				other => panic!("unexpected kind value: {:?}", other),
			}
		}
	}
}

#[test]
fn hydrate_keeps_the_take_earned_before_an_unrenderable_filter() {
	// Mul cannot render, but the take sits above it and was already earned; dropping it pulls all 50 rows.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::big { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::big", 50);

	let (engine, sub_id, lease, sub_service) =
		create_and_setup(&db, "from app::big | take 5 | filter { qty * 2 > 0 }");

	sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("hydrate succeeds: take 5 survives the unrenderable filter below it");
}

#[test]
fn hydrate_pushes_take_through_map() {
	// Map is one row in, one row out, so the take below it selects the same rows at the source.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::mapped { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::mapped", 50);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::mapped | map { id, qty } | take 5");

	sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("hydrate succeeds: take 5 should be pushed through map so cap=5 holds");
}

#[test]
fn hydrate_pushes_take_through_extend() {
	// Extend adds a column without changing cardinality or order, so a source take is exact.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::extended { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::extended", 50);

	let (engine, sub_id, lease, sub_service) =
		create_and_setup(&db, "from app::extended | extend { qty_x2: qty * 2 } | take 5");

	sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("hydrate succeeds: take 5 should be pushed through extend so cap=5 holds");
}

#[test]
fn hydrate_does_not_push_take_below_distinct() {
	// Distinct changes cardinality, so this bound is genuinely unpushable and the cap must still fire.
	let db = TestDb::memory();

	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::keyed { id: int4, qty: int4 }");
	seed_id_qty(&db, "app::keyed", 50);

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::keyed | distinct {id} | take 5");

	let err = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect_err("expected RowCapExceeded: take must not be pushed below distinct");

	match err {
		HydrateError::RowCapExceeded {
			cap,
			bound,
		} => {
			assert_eq!(cap, 5);
			// The user already wrote a take, so advice to add one is wrong; the error must name the
			// blocker.
			assert_eq!(
				bound,
				HydrationBound::Blocked {
					operator: "Distinct".to_string(),
				}
			);
		}
		other => panic!("unexpected error: {:?}", other),
	}
}

fn subscribe_with_params(
	db: &TestDb,
	identity: IdentityId,
	query: &str,
	params: Params,
) -> (StandardEngine, SubscriptionId, VersionLeaseGuard, SubscriptionServiceRef) {
	let stmt = format!("CREATE SUBSCRIPTION AS {{ {} }}", query);
	let result = db.engine().subscribe_as(identity, &stmt, params);
	assert!(result.error.is_none(), "subscribe failed: {:?}", result.error);
	let sub_id = extract_sub_id(&result.frames);
	let (engine, lease, sub_service) = engine_lease_service(db);
	thread::sleep(Duration::from_milliseconds(50).unwrap().to_std());
	(engine, sub_id, lease, sub_service)
}

fn monitor_param(monitor: &str) -> Params {
	Params::from(HashMap::from([("monitor".to_string(), Value::Utf8(monitor.to_string()))]))
}

fn seed_two_monitors(db: &TestDb, rows: usize) {
	// Even ids belong to monitor a and odd ids to b, so every other row is one the filter must drop.
	let mut insert_stmt = String::from("INSERT app::checks [");
	for i in 0..rows {
		if i > 0 {
			insert_stmt.push(',');
		}
		let monitor = if i % 2 == 0 {
			"a"
		} else {
			"b"
		};
		insert_stmt.push_str(&format!("{{id: {}, monitor: '{}'}}", i, monitor));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);
}

fn two_monitor_db() -> TestDb {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::checks { id: int4, monitor: utf8 }");
	seed_two_monitors(&db, 60);
	db
}

fn lookup_identity(db: &TestDb, name: &str) -> IdentityId {
	let frames = db.query(&format!("from system::identities filter {{ name == '{name}' }}"));
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
	match col.data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value: {other:?}"),
	}
}

fn drain_live(db: &TestDb, sub_id: SubscriptionId) -> Vec<StagedBatch> {
	let target = db.watermarks().tx().current().expect("current version");
	let caught_up = db.watermarks().cdc().wait_for_consumer(target, Duration::from_seconds(10).unwrap());
	assert!(caught_up, "cdc consumer did not reach {:?}", target);
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	subsystem.store().drain(&sub_id, usize::MAX)
}

fn ids_with_op(batches: &[StagedBatch], want: DiffType) -> Vec<i32> {
	let only: Vec<StagedBatch> = batches.iter().filter(|(op, _)| *op == want).cloned().collect();
	announced_ids(&only)
}

#[test]
fn hydrate_bounds_a_param_filtered_take_by_its_limit_not_by_the_matching_rows() {
	// A $name filter that stops the take reaching the source makes take 5 fail a cap of 5 it satisfies.
	let db = two_monitor_db();

	let (engine, sub_id, lease, sub_service) = subscribe_with_params(
		&db,
		IdentityId::root(),
		"from app::checks | filter { monitor == $monitor } | take 5",
		monitor_param("a"),
	);

	let outcome = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("take 5 bounds the snapshot, so a cap of 5 must hold however many rows match");

	assert_eq!(
		announced_ids(&outcome.batches),
		vec![58, 56, 54, 52, 50],
		"the snapshot must be the five newest matching rows, newest first, as the live window holds them"
	);
}

#[test]
fn hydrate_bounds_a_policy_scoped_param_filtered_take_by_its_limit() {
	// The injected $identity.id policy filter sits first, so it must not block the take reaching the source.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::checks { id: int4, monitor: utf8, owner: identity_id }");
	db.admin("create user alice");
	db.admin("create user bob");
	db.admin("create session policy allow_subscribe { subscription: { filter { true } } }");
	db.admin("create table policy checks_owner on app::checks { from: { filter { owner == $identity.id } } }");
	let alice = lookup_identity(&db, "alice");
	let bob = lookup_identity(&db, "bob");

	let mut insert_stmt = String::from("INSERT app::checks [");
	for i in 0..90 {
		if i > 0 {
			insert_stmt.push(',');
		}
		let owner = if i % 3 == 0 {
			bob
		} else {
			alice
		};
		let monitor = if i % 2 == 0 {
			"a"
		} else {
			"b"
		};
		insert_stmt
			.push_str(&format!("{{id: {i}, monitor: '{monitor}', owner: cast('{owner}', identity_id)}}"));
	}
	insert_stmt.push(']');
	db.command(&insert_stmt);

	let (engine, sub_id, lease, sub_service) = subscribe_with_params(
		&db,
		alice,
		"from app::checks filter { monitor == $monitor } map { id, monitor } take 5",
		monitor_param("a"),
	);

	let outcome = sub_service
		.hydrate(sub_id, &engine, alice, lease, 5)
		.expect("take 5 bounds the snapshot, so a cap of 5 must hold however many rows alice owns");

	assert_eq!(
		announced_ids(&outcome.batches),
		vec![88, 86, 82, 80, 76],
		"the snapshot must be alice's five newest monitor a rows; bob's 84 and 78 must never appear"
	);
}

#[test]
fn hydrate_refuses_an_identity_other_than_the_subscriber() {
	// a hydrate run as another identity must be refused, never seed or return a snapshot of the subscriber's flow.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::checks { id: int4, owner: identity_id }");
	db.admin("create user alice");
	db.admin("create user bob");
	db.admin("create session policy allow_subscribe { subscription: { filter { true } } }");
	db.admin("create table policy checks_owner on app::checks { from: { filter { owner == $identity.id } } }");
	let alice = lookup_identity(&db, "alice");
	let bob = lookup_identity(&db, "bob");
	assert_ne!(alice, bob, "the test needs two distinct identities");
	db.command(&format!(
		"INSERT app::checks [{{id: 1, owner: cast('{alice}', identity_id)}}, {{id: 2, owner: cast('{bob}', identity_id)}}, \
		 {{id: 3, owner: cast('{alice}', identity_id)}}, {{id: 4, owner: cast('{bob}', identity_id)}}]"
	));

	let (engine, sub_id, lease, sub_service) =
		subscribe_with_params(&db, alice, "from app::checks map { id }", Params::None);

	match sub_service.hydrate(sub_id, &engine, bob, lease, 50) {
		Err(HydrateError::SubscriptionNotFound) => {}
		Ok(outcome) => panic!(
			"bob hydrated alice's subscription and received ids {:?}",
			announced_ids(&outcome.batches)
		),
		Err(other) => panic!("unexpected error: {other:?}"),
	}

	let (_, lease, _) = engine_lease_service(&db);
	let outcome =
		sub_service.hydrate(sub_id, &engine, alice, lease, 50).expect("alice hydrates her own subscription");
	let mut ids = announced_ids(&outcome.batches);
	ids.sort_unstable();
	assert_eq!(ids, vec![1, 3], "alice's snapshot must hold exactly her rows");
}

#[test]
fn hydrate_still_refuses_a_param_filtered_take_larger_than_the_cap() {
	// take 20 admits twenty matching rows, so a cap of 10 must refuse the snapshot rather than truncate it.
	let db = two_monitor_db();

	let (engine, sub_id, lease, sub_service) = subscribe_with_params(
		&db,
		IdentityId::root(),
		"from app::checks | filter { monitor == $monitor } | take 20",
		monitor_param("a"),
	);

	let err = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect_err("expected RowCapExceeded: take 20 exceeds a cap of 10");

	match err {
		HydrateError::RowCapExceeded {
			cap,
			bound,
		} => {
			assert_eq!(cap, 10);
			assert_eq!(
				bound,
				HydrationBound::Pushed,
				"the take reached the source, so only raising the cap helps"
			);
		}
		other => panic!("unexpected error: {:?}", other),
	}
}

#[test]
fn hydrate_still_refuses_a_param_filter_with_no_take_over_the_cap() {
	// Pushing the filter must not pass for a bound: thirty matching rows with no take still exceed a cap of 10.
	let db = two_monitor_db();

	let (engine, sub_id, lease, sub_service) = subscribe_with_params(
		&db,
		IdentityId::root(),
		"from app::checks | filter { monitor == $monitor }",
		monitor_param("a"),
	);

	let err = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect_err("expected RowCapExceeded: nothing bounds thirty matching rows");

	match err {
		HydrateError::RowCapExceeded {
			cap,
			bound,
		} => {
			assert_eq!(cap, 10);
			assert_eq!(
				bound,
				HydrationBound::Absent,
				"the query has no take, so the advice must be to add one"
			);
		}
		other => panic!("unexpected error: {:?}", other),
	}
}

#[test]
fn a_live_match_after_a_param_filtered_hydration_evicts_the_oldest_hydrated_row() {
	// A window seeded from the pushed snapshot must evict its oldest row on the next match and ignore a non-match.
	let db = two_monitor_db();

	let (engine, sub_id, lease, sub_service) = subscribe_with_params(
		&db,
		IdentityId::root(),
		"from app::checks | filter { monitor == $monitor } | take 5",
		monitor_param("a"),
	);
	sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 5).expect("hydrate succeeds");
	drain_live(&db, sub_id);

	db.command("INSERT app::checks [{id: 60, monitor: 'a'}]");
	db.command("INSERT app::checks [{id: 61, monitor: 'b'}]");
	let live = drain_live(&db, sub_id);

	assert_eq!(ids_with_op(&live, DiffType::Insert), vec![60], "only the new monitor a row may enter the window");
	assert_eq!(ids_with_op(&live, DiffType::Remove), vec![50], "the oldest hydrated row must be the one evicted");
}

#[test]
fn hydrate_returns_subscription_not_found_for_unknown_id() {
	let db = TestDb::memory();

	let (engine, lease, sub_service) = engine_lease_service(&db);

	let err = sub_service
		.hydrate(SubscriptionId(99_999), &engine, IdentityId::root(), lease, 1024)
		.expect_err("expected SubscriptionNotFound");

	match err {
		HydrateError::SubscriptionNotFound => {}
		other => panic!("unexpected error: {:?}", other),
	}
}

fn first_value(frames: &[Frame], name: &str) -> Option<Value> {
	let frame = frames.first()?;
	let col = frame.columns.iter().find(|c| c.name == name)?;
	if col.data.is_empty() {
		return None;
	}
	Some(col.data.get_value(0))
}

#[test]
fn create_subscription_default_returns_hydration_enabled_true_with_no_max_rows() {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::orders { id: int4, qty: int4 }");

	let frames = db.admin("CREATE SUBSCRIPTION AS { FROM app::orders }");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(b, "default hydration should be enabled"),
		other => panic!("hydration_enabled column missing or wrong type: {:?}", other),
	}
	match first_value(&frames, "hydration_max_rows") {
		Some(Value::None {
			..
		})
		| None => {}
		other => panic!("hydration_max_rows should be None when not specified, got: {:?}", other),
	}
}

#[test]
fn create_subscription_with_disabled_returns_hydration_enabled_false() {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::orders { id: int4, qty: int4 }");

	let frames = db.admin("CREATE SUBSCRIPTION WITH { hydration: { enabled: false } } AS { FROM app::orders }");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(!b, "explicit enabled=false should produce false"),
		other => panic!("hydration_enabled column missing or wrong type: {:?}", other),
	}
}

#[test]
fn create_subscription_with_max_rows_returns_max_rows_uint8() {
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::orders { id: int4, qty: int4 }");

	let frames = db.admin("CREATE SUBSCRIPTION WITH { hydration: { max_rows: 250 } } AS { FROM app::orders }");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(b, "max_rows-only should default enabled to true"),
		other => panic!("hydration_enabled wrong: {:?}", other),
	}
	match first_value(&frames, "hydration_max_rows") {
		Some(Value::Uint8(n)) => assert_eq!(n, 250, "max_rows should round-trip to 250"),
		other => panic!("hydration_max_rows wrong: {:?}", other),
	}
}
