// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The lease reaper is the only clock that invalidates a lease. Everything here is about what a
//! worker that never comes back costs: the item must come back, exactly one loss must be recorded
//! for it, that loss must count against the retry budget, and a lease that is still alive - or was
//! extended, or was already acked - must survive the sweep untouched.

use std::sync::Arc;

use reifydb_codec::row::bytes::RowBuilder;
use reifydb_core::{
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::QueueId,
			queue::{
				AttemptOutcome, QueueAttemptRecord, QueueItemState, QueueItemStatus,
				QueuePartitionCounters, decode_queue_attempt, decode_queue_item_state,
				decode_queue_partition_counters, encode_queue_attempt,
			},
		},
		store::{SingleVersionGet, SingleVersionRange},
	},
	key::{
		EncodableKey,
		queue_attempt::QueueAttemptKey,
		queue_schedule::{QueueItemStateKey, QueuePartitionKey},
	},
	lifecycle::{metrics::RetentionMetrics, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_lifecycle::{plane::RetentionPlane, queue::reap::QueueLeaseReapTask};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{Value, frame::frame::Frame, identity::IdentityId, row_number::RowNumber};

const ONE_PARTITION: &str =
	"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 2 } }";

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn reaper(t: &TestEngine) -> QueueLeaseReapTask {
	let engine: &StandardEngine = t.inner();
	QueueLeaseReapTask::new(
		engine.clone(),
		RetentionPlane::for_engine(engine, RetentionMetrics::new()),
		engine.clock().clone(),
		Arc::new(engine.catalog()) as Arc<dyn GetConfig>,
	)
}

fn queue_id(t: &TestEngine, name: &str) -> QueueId {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap().unwrap().id
}

fn states(t: &TestEngine, queue: QueueId) -> Vec<QueueItemState> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueItemStateKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| decode_queue_item_state(&item.bytes).unwrap())
		.collect()
}

fn state_of(t: &TestEngine, queue: QueueId) -> QueueItemState {
	states(t, queue).into_iter().next().expect("the queue must hold exactly one item")
}

fn counters(t: &TestEngine, queue: QueueId) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue, 0))
		.unwrap()
		.map(|stored| decode_queue_partition_counters(&stored.bytes))
		.unwrap_or_default()
}

fn attempts(t: &TestEngine, queue: QueueId) -> Vec<(QueueAttemptKey, QueueAttemptRecord)> {
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let mut stream = txn.range(QueueAttemptKey::queue_scan(queue), RangeScope::All, 1024).unwrap();

	let mut out = Vec::new();
	while let Some(item) = stream.next() {
		let item = item.unwrap();
		out.push((QueueAttemptKey::decode(&item.key).unwrap(), decode_queue_attempt(&item.bytes).unwrap()));
	}
	out
}

fn token_of(frames: &[Frame]) -> String {
	let frame = frames.first().expect("claim must return a frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one claimed item");
	match frame.columns.iter().find(|c| c.name == "token").unwrap().data.get_value(0) {
		Value::Utf8(t) => t,
		other => panic!("token must be Utf8, got {other:?}"),
	}
}

fn claim_one(t: &TestEngine, worker: &str, lease_seconds: u64) -> String {
	token_of(&t.command(&format!(
		r#"CALL queue::claim("{worker}", "test::jobs", 1, duration::seconds({lease_seconds}))"#
	)))
}

fn claimable(t: &TestEngine) -> usize {
	TestEngine::row_count(&t.command(r#"CALL queue::claim("probe", "test::jobs", 10, duration::seconds(30))"#))
}

fn plant_attempt(t: &TestEngine, queue: QueueId, row: u64, attempt: u32, record: QueueAttemptRecord) {
	let mut txn = t.inner().begin_command(IdentityId::system()).unwrap();
	txn.set(&QueueAttemptKey::encoded(queue, RowNumber(row), attempt), encode_queue_attempt(&record).freeze_bytes())
		.unwrap();
	txn.commit().unwrap();
}

#[test]
fn test_an_expired_lease_comes_back_with_exactly_one_lost_attempt() {
	// A worker that dies mid-task is the failure the whole reaper exists for. The item must return
	// to the ready set, and the loss must be on the record: without it the next reap could not tell
	// a first loss from a repeat, and the retry budget would never be charged for the dead worker.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "doomed", 30);

	t.mock_clock().set_millis(31_000);
	assert_eq!(reaper(&t).run_slice(), Progress::Exhausted);

	let state = state_of(&t, queue);
	assert_eq!(state.status, QueueItemStatus::Ready, "an expired lease must return the item");
	assert_eq!(state.lease_deadline, None, "the dead worker's lease must be cleared");
	assert_eq!(state.attempt, 1, "the attempt counter is history and must not rewind");

	let recorded = attempts(&t, queue);
	assert_eq!(recorded.len(), 1, "exactly one loss for one expired lease");
	assert_eq!(recorded[0].1.lost, true);
	assert_eq!(recorded[0].0.attempt, 1);

	assert_eq!(counters(&t, queue).in_flight, 0);
	assert_eq!(counters(&t, queue).depth, 1);
}

#[test]
fn test_a_reaped_item_waits_out_its_backoff_before_redelivery() {
	// Redelivering instantly would let a task that kills its worker take the next worker down at
	// full speed. The reaper's requeue goes through the same backoff curve an err-ack does.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "doomed", 30);

	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();

	assert_eq!(
		state_of(&t, queue).backoff_until.map(|b| b.to_nanos()),
		Some(41_000_000_000),
		"the default 10s backoff runs from the reap, not from the original claim"
	);
	assert_eq!(claimable(&t), 0, "the item is ready but not yet due");

	t.mock_clock().set_millis(41_000);
	assert_eq!(claimable(&t), 1, "once the backoff elapses the item is deliverable again");
}

#[test]
fn test_lost_attempts_spend_the_retry_budget() {
	// R5: a black-holing endpoint that kills every worker must eventually stop being retried. If
	// lost attempts did not count, an item that never produces an ack would cycle forever and the
	// dead state would be unreachable for exactly the failure mode that needs it most.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");

	t.mock_clock().set_millis(0);
	claim_one(&t, "doomed-1", 30);
	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Ready, "attempt 1 of 2 is recoverable");

	t.mock_clock().set_millis(41_000);
	claim_one(&t, "doomed-2", 30);
	t.mock_clock().set_millis(72_000);
	reaper(&t).run_slice();

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead, "two lost attempts spend a budget of 2");
	assert_eq!(attempts(&t, queue).len(), 2);
	assert_eq!(claimable(&t), 0);
	assert_eq!(counters(&t, queue).in_flight, 0);
}

#[test]
fn test_a_live_lease_is_left_alone() {
	// The reaper scans every item state, not just expired ones. Reaping on the wrong side of the
	// deadline comparison would hand a working worker's item to a second worker.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "alive", 30);

	t.mock_clock().set_millis(29_999);
	assert_eq!(reaper(&t).run_slice(), Progress::Exhausted);

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Leased, "a lease is live right up to its deadline");
	assert_eq!(attempts(&t, queue).len(), 0, "no loss may be recorded for a lease that never expired");
	assert_eq!(counters(&t, queue).in_flight, 1);
}

#[test]
fn test_an_extended_lease_survives_a_concurrent_sweep() {
	// The reaper's scan is advisory and unlocked, so a worker can extend between the scan and the
	// compare-and-set. Guarding only on (leased, attempt) would let the reaper steal an item from a
	// worker that is demonstrably alive - the exact race queue::extend exists to prevent.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	let token = claim_one(&t, "slow", 30);

	t.mock_clock().set_millis(31_000);
	t.command(&format!(r#"CALL queue::extend("{token}", duration::seconds(60))"#));
	assert_eq!(reaper(&t).run_slice(), Progress::Exhausted);

	let state = state_of(&t, queue);
	assert_eq!(state.status, QueueItemStatus::Leased, "the extended lease must survive the sweep");
	assert_eq!(state.lease_deadline.map(|d| d.to_nanos()), Some(91_000_000_000));
	assert_eq!(attempts(&t, queue).len(), 0, "an extended lease is not a loss");
}

#[test]
fn test_an_already_acked_item_is_healed_rather_than_marked_lost() {
	// The crash window between a durable ack and its state transition. The outcome the worker
	// reported is on disk, so the reaper must finish that ack's job. Recording a loss instead would
	// re-run work the worker already completed - the one thing an at-least-once queue must not do
	// when it has the evidence in hand.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "w1", 30);

	plant_attempt(
		&t,
		queue,
		1,
		1,
		QueueAttemptRecord {
			worker: "w1".to_string(),
			outcome: AttemptOutcome::Ok,
			response: None,
			finished_at: t.mock_clock().now(),
			lost: false,
			anomaly: None,
		},
	);

	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Done, "the recorded ok outcome decides the transition");
	let recorded = attempts(&t, queue);
	assert_eq!(recorded.len(), 1, "the reaper must not add a loss on top of a real outcome");
	assert_eq!(recorded[0].1.lost, false);
	assert_eq!(claimable(&t), 0);
}

#[test]
fn test_a_recorded_err_outcome_is_completed_by_the_reaper() {
	// Same crash window, failing outcome. The item must follow the err path (retry or dead), not be
	// double-charged with a second lost record for the same attempt.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "w1", 30);

	plant_attempt(
		&t,
		queue,
		1,
		1,
		QueueAttemptRecord {
			worker: "w1".to_string(),
			outcome: AttemptOutcome::Err,
			response: None,
			finished_at: t.mock_clock().now(),
			lost: false,
			anomaly: None,
		},
	);

	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Ready, "attempt 1 of 2 still has budget");
	assert_eq!(attempts(&t, queue).len(), 1, "the existing outcome is authoritative; no loss is added");
}

#[test]
fn test_a_reap_that_crashed_after_recording_finishes_on_the_next_slice() {
	// Ordering is fixed: the lost record is written before the compare-and-set, so a crash in
	// between leaves a lost record on a still-leased item. The next slice must complete the
	// transition without writing a second record, or every crash would inflate the attempt history
	// and spend the budget twice as fast.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "doomed", 30);

	plant_attempt(
		&t,
		queue,
		1,
		1,
		QueueAttemptRecord {
			worker: String::new(),
			outcome: AttemptOutcome::Err,
			response: None,
			finished_at: t.mock_clock().now(),
			lost: true,
			anomaly: None,
		},
	);

	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Ready);
	assert_eq!(attempts(&t, queue).len(), 1, "the planted loss must not be duplicated");
}

#[test]
fn test_a_second_slice_over_the_same_state_changes_nothing() {
	// run_slice is called on a timer forever. A sweep that transitioned an item it already
	// transitioned would decrement in_flight twice and corrupt the counters permanently.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	claim_one(&t, "doomed", 30);

	t.mock_clock().set_millis(31_000);
	reaper(&t).run_slice();
	let after_first = (state_of(&t, queue), counters(&t, queue), attempts(&t, queue).len());

	reaper(&t).run_slice();

	assert_eq!(state_of(&t, queue), after_first.0);
	assert_eq!(counters(&t, queue), after_first.1);
	assert_eq!(attempts(&t, queue).len(), after_first.2);
}

#[test]
fn test_a_backlog_deeper_than_the_budget_drains_across_slices() {
	// The reaper shares one actor lane with every other lifecycle task, and its scan covers ready
	// items too. Without the cursor a budget smaller than the backlog would rescan the same head
	// every slice and never reach the tail, so a queue could hold expired leases indefinitely while
	// the task reported progress.
	let t = engine_with_queue(ONE_PARTITION);
	for id in 1..=6 {
		t.command(&format!("INSERT test::jobs [{{ id: {id} }}]"));
	}
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	for worker in 1..=6 {
		claim_one(&t, &format!("doomed-{worker}"), 30);
	}
	t.set_config(ConfigKey::QueueLeaseReapBatchSize, Value::Uint8(2));

	t.mock_clock().set_millis(31_000);
	let mut task = reaper(&t);
	assert_eq!(task.run_slice(), Progress::Yielded, "a budget of 2 cannot cover 6 records in one slice");

	let mut slices = 1;
	while task.run_slice() == Progress::Yielded {
		slices += 1;
		assert!(slices < 20, "the cursor must advance; it is looping over the same head");
	}

	assert_eq!(
		states(&t, queue).iter().filter(|s| s.status == QueueItemStatus::Ready).count(),
		6,
		"every expired lease is reaped once the slices drain"
	);
	assert_eq!(attempts(&t, queue).len(), 6, "one loss per item, no duplicates across slices");
	assert_eq!(counters(&t, queue).in_flight, 0);
}
