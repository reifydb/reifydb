// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{Barrier, Mutex},
	thread,
	thread::sleep,
	time::Duration,
};

use reifydb::{
	ConfigKey, Frame, IdentityId, Params, Value, embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_test_harness::engine::AsEngine;

const ORDERED: &str = r#"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH {
	fifo: { partitions: 1, ordered_by: tenant },
	retry: { attempts: 5, backoff: 50ms }
}"#;

fn built_db() -> TestDb {
	// the reap interval must be set before boot: the lifecycle actor reads it once to schedule its timer.
	TestDb::from(
		embedded::memory()
			.with_config(ConfigKey::QueueLeaseReapInterval, Value::duration_milliseconds(50))
			.build()
			.unwrap(),
	)
}

fn seeded_db(attempts: u32) -> TestDb {
	let db = built_db();
	db.admin("CREATE NAMESPACE test");
	db.admin(&format!(r#"CREATE QUEUE test::jobs {{ id: int4 }} WITH {{
			fifo: {{ partitions: 1 }},
			retry: {{ attempts: {attempts}, backoff: 50ms }}
		}}"#));
	db.command("INSERT test::jobs [{ id: 1 }]");
	db
}

fn seeded_ordered_db() -> TestDb {
	let db = built_db();
	db.admin("CREATE NAMESPACE test");
	db.admin(ORDERED);
	db.command(r#"INSERT test::jobs [{ id: 1, tenant: "acme" }, { id: 2, tenant: "acme" }]"#);
	db
}

fn column(frames: &[Frame], name: &str) -> Value {
	frames.first()
		.expect("no frame")
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("no `{name}` column"))
		.data
		.get_value(0)
}

fn utf8(frames: &[Frame], name: &str) -> String {
	match column(frames, name) {
		Value::Utf8(s) => s,
		other => panic!("{name} must be Utf8, got {other:?}"),
	}
}

fn uint4(frames: &[Frame], name: &str) -> u32 {
	match column(frames, name) {
		Value::Uint4(v) => v,
		other => panic!("{name} must be Uint4, got {other:?}"),
	}
}

fn uint8(frames: &[Frame], name: &str) -> u64 {
	match column(frames, name) {
		Value::Uint8(v) => v,
		other => panic!("{name} must be Uint8, got {other:?}"),
	}
}

fn int4(frames: &[Frame], name: &str) -> i32 {
	match column(frames, name) {
		Value::Int4(v) => v,
		other => panic!("{name} must be Int4, got {other:?}"),
	}
}

fn try_claim(db: &TestDb, worker: &str, lease_ms: u64) -> Option<Vec<Frame>> {
	let frames =
		db.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", 1, duration::millis({lease_ms}))"#));
	if frames.first().map(|f| f.row_count()).unwrap_or(0) == 0 {
		None
	} else {
		Some(frames)
	}
}

fn ack(db: &TestDb, token: &str) -> String {
	utf8(&db.command(&format!(r#"CALL queue::ack("{token}")"#)), "status")
}

fn extend(db: &TestDb, token: &str, ttl_ms: u64) {
	db.command(&format!(r#"CALL queue::extend("{token}", duration::millis({ttl_ms}))"#));
}

fn depth_and_in_flight(db: &TestDb) -> (u64, u64) {
	let frames = db.query(r#"FROM system::queues FILTER { name == "jobs" } MAP { depth, in_flight }"#);
	(uint8(&frames, "depth"), uint8(&frames, "in_flight"))
}

#[test]
fn a_late_ack_after_lease_expiry_and_reassignment_is_rejected_as_stale() {
	let db = seeded_db(5);

	// worker-a claims the only item, then never acks - it must never be able to finish work again.
	let first = try_claim(&db, "worker-a", 100).expect("must claim the only item");
	let stale_token = utf8(&first, "token");
	assert_eq!(uint4(&first, "attempt"), 1);

	// the reaper (sped up above) must return the expired lease to the ready set on its own.
	let second = poll_until(|| try_claim(&db, "worker-b", 30_000), Duration::from_secs(5))
		.expect("the reaper must return the expired lease within the timeout");
	assert_eq!(uint4(&second, "attempt"), 2, "reassignment must be recorded as a fresh attempt");

	// worker-a's ack finally arrives, addressed to the attempt it no longer holds.
	assert_eq!(
		ack(&db, &stale_token),
		"stale",
		"a late ack naming a superseded attempt must be rejected, not applied"
	);

	// the current holder must be unaffected by the superseded worker's stale ack.
	let live_token = utf8(&second, "token");
	assert_eq!(ack(&db, &live_token), "ok", "the current holder's ack must still succeed");

	// exactly one completion, never a resurrected or double-processed item.
	assert_eq!(depth_and_in_flight(&db), (0, 0));
}

#[test]
fn repeated_crashes_exhaust_the_retry_budget_and_an_operator_replays_the_dead_item() {
	let db = seeded_db(2);

	// attempt 1 crashes; the reaper must redeliver it as attempt 2, spending one unit of budget.
	let first = try_claim(&db, "doomed-1", 100).expect("must claim the only item");
	let item = uint8(&first, "item");
	assert_eq!(uint4(&first, "attempt"), 1);
	let second = poll_until(|| try_claim(&db, "doomed-2", 100), Duration::from_secs(5))
		.expect("the first crash must be reaped and redelivered");
	assert_eq!(uint4(&second, "attempt"), 2);

	// attempt 2 crashes too - the budget of 2 is spent, so the reaper must bury the item, not retry again.
	assert!(
		poll_until(|| (depth_and_in_flight(&db) == (0, 0)).then_some(()), Duration::from_secs(5)).is_some(),
		"the second crash must spend the last of the retry budget"
	);
	assert!(try_claim(&db, "prober", 30_000).is_none(), "a dead item must never be redelivered on its own");

	// an operator replays the dead item by its row number, restoring a fresh retry budget.
	let replayed = db.command(&format!(r#"CALL queue::replay("test::jobs", {item})"#));
	assert_eq!(utf8(&replayed, "state"), "ready", "a replayed item must return to the ready set");

	// a third worker now completes it normally, proving the replay is a real second chance.
	let third = poll_until(|| try_claim(&db, "worker-final", 30_000), Duration::from_secs(5))
		.expect("the replayed item must be claimable again");
	assert_eq!(ack(&db, &utf8(&third, "token")), "ok");
	assert_eq!(depth_and_in_flight(&db), (0, 0));
}

#[test]
fn a_worker_that_extends_before_the_deadline_survives_the_reaper_sweep() {
	let db = seeded_db(5);

	// worker-a is alive but slow: it renews its lease well before the original deadline arrives.
	let claimed = try_claim(&db, "worker-a", 150).expect("must claim the only item");
	let token = utf8(&claimed, "token");
	extend(&db, &token, 5_000);

	// several reap sweeps must cross the ORIGINAL deadline without touching the extended lease.
	sleep(Duration::from_millis(400));
	assert!(try_claim(&db, "worker-b", 30_000).is_none(), "an extended lease must never be reclaimed early");

	// worker-a finishes normally, proving the lease and its attempt count survived untouched.
	assert_eq!(ack(&db, &token), "ok", "the original attempt must still be live after the extension");
	assert_eq!(depth_and_in_flight(&db), (0, 0));
}

#[test]
fn several_workers_racing_a_reclaimed_lease_receive_it_exactly_once() {
	const RACERS: usize = 8;
	let db = seeded_db(5);

	// worker-a claims the only item and crashes; the race starts once the reaper puts it back.
	let first = try_claim(&db, "worker-a", 100).expect("must claim the only item");
	assert_eq!(uint4(&first, "attempt"), 1);

	let winners: Mutex<Vec<u32>> = Mutex::new(Vec::new());
	let barrier = Barrier::new(RACERS);

	// TestDb is not Sync (it owns boxed subsystems); the racers share the underlying engine instead.
	let engine = db.engine();
	thread::scope(|scope| {
		for racer in 0..RACERS {
			let (winners, barrier) = (&winners, &barrier);
			scope.spawn(move || {
				barrier.wait();
				let won = poll_until(
					|| {
						let r = engine.command_as(
							IdentityId::system(),
							&format!(
								r#"CALL queue::claim("racer-{racer}", "test::jobs", 1, duration::millis(30000))"#
							),
							Params::None,
						);
						if let Some(e) = r.error {
							panic!("command failed: {e:?}");
						}
						if r.frames.first().map(|f| f.row_count()).unwrap_or(0) == 0 {
							None
						} else {
							Some(r.frames)
						}
					},
					Duration::from_secs(2),
				);
				if let Some(frames) = won {
					winners.lock().unwrap().push(uint4(&frames, "attempt"));
				}
			});
		}
	});

	let winners = winners.into_inner().unwrap();
	assert_eq!(winners.len(), 1, "the reclaimed lease must go to exactly one racer, never zero and never many");
	assert_eq!(winners[0], 2, "the winning claim must be recorded as a fresh attempt");
}

#[test]
fn a_crashed_worker_holding_the_head_of_a_key_keeps_its_sibling_parked_until_reaped() {
	let db = seeded_ordered_db();

	// worker-a claims the head of tenant "acme"; its sibling must stay parked behind a live lease.
	let head = try_claim(&db, "worker-a", 100).expect("must claim the head of the key");
	assert_eq!(uint4(&head, "attempt"), 1);
	assert_eq!(int4(&head, "id"), 1);
	assert!(try_claim(&db, "worker-b", 30_000).is_none(), "a parked sibling must never bypass a live head lease");

	// the reaper reclaims the crashed head - redelivery, not promotion, since the head never finished.
	let redelivered = poll_until(|| try_claim(&db, "worker-c", 30_000), Duration::from_secs(5))
		.expect("the reaper must return the head's expired lease");
	assert_eq!(uint4(&redelivered, "attempt"), 2, "redelivery of the head must be recorded as a fresh attempt");
	assert_eq!(int4(&redelivered, "id"), 1, "the redelivered item must be the original head, never its sibling");

	// only a terminal transition on the head may promote the parked sibling.
	assert_eq!(ack(&db, &utf8(&redelivered, "token")), "ok");
	let sibling = poll_until(|| try_claim(&db, "worker-d", 30_000), Duration::from_secs(5))
		.expect("the sibling must be promoted once the head is done");
	assert_eq!(int4(&sibling, "id"), 2, "the promoted item must be the previously parked sibling");
}
