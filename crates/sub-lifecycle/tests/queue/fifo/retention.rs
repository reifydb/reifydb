// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Retention is the only destructive queue task, so most of what matters here is what it must NOT
//! delete: anything unfinished, anything inside its declared window, and anything it cannot date.
//!
//! Deduplication records follow their OWN declared ttl and are not coupled to item retention: a
//! swept item keeps suppressing its key until that ttl elapses. Deleting the item is a storage
//! decision; the deduplication promise made to the caller is a separate one with its own clock.

use std::sync::Arc;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::QueueId,
			queue::{QueueItemState, QueueItemStatus, decode_queue_item_state},
		},
		store::SingleVersionRange,
	},
	key::queue::{QueueAttemptKey, QueueDeduplicationKey, QueueItemStateKey},
	lifecycle::{
		gate::{Gated, RetentionStartupGate},
		metrics::RetentionMetrics,
		progress::Progress,
		task::LifecycleTask,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_lifecycle::{plane::RetentionPlane, queue::retention::QueueRetentionTask};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{Value, duration::Duration, frame::frame::Frame};

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn sweeper(t: &TestEngine) -> QueueRetentionTask {
	let engine: &StandardEngine = t.inner();
	QueueRetentionTask::new(
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
		.map(|item| decode_queue_item_state(EncodedPodRow::view(&item.bytes)).unwrap())
		.collect()
}

fn attempt_count(t: &TestEngine, queue: QueueId) -> usize {
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let mut stream = txn.range(QueueAttemptKey::queue_scan(queue), RangeScope::All, 1024).unwrap();
	let mut count = 0;
	while let Some(item) = stream.next() {
		item.unwrap();
		count += 1;
	}
	count
}

fn deduplication_count(t: &TestEngine, queue: QueueId) -> usize {
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let mut stream = txn.range(QueueDeduplicationKey::full_scan(queue), RangeScope::All, 1024).unwrap();
	let mut count = 0;
	while let Some(item) = stream.next() {
		item.unwrap();
		count += 1;
	}
	count
}

fn items(t: &TestEngine) -> usize {
	t.query("FROM test::jobs").first().map(|frame: &Frame| frame.row_count()).unwrap_or(0)
}

fn token_of(frames: &[Frame]) -> String {
	let frame = frames.first().expect("claim must return a frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one claimed item");
	match frame.columns.iter().find(|c| c.name == "token").unwrap().data.get_value(0) {
		Value::Utf8(t) => t,
		other => panic!("token must be Utf8, got {other:?}"),
	}
}

fn claim_one(t: &TestEngine) -> String {
	token_of(&t.command(r#"CALL queue::claim("w1", "test::jobs", 1, duration::seconds(30))"#))
}

fn finish(t: &TestEngine) {
	let token = claim_one(t);
	t.command(&format!(r#"CALL queue::ack("{token}")"#));
}

fn kill(t: &TestEngine) {
	let token = claim_one(t);
	t.command(&format!(r#"CALL queue::kill("{token}", none)"#));
}

#[test]
fn test_a_finished_item_is_deleted_once_it_outlives_the_declared_window() {
	// The whole point of retention.done: a completed item is history, and history is bounded. The
	// item row, its attempt records and its scheduling state must all go, or the queue leaks one of
	// the three forever.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	t.mock_clock().set_millis(3_600_001);
	assert_eq!(sweeper(&t).run_slice(), Progress::Exhausted);

	assert_eq!(items(&t), 0, "the item row must be gone");
	assert_eq!(attempt_count(&t, queue), 0, "the attempt history must go with the item");
	assert_eq!(states(&t, queue).len(), 0, "a state record without an item is a leak");
}

#[test]
fn test_a_dead_item_is_swept_too_which_bounds_replayability() {
	// Documented contract: retention bounds how long a dead item can be replayed. Keeping dead
	// items forever would make retention.done a lie for exactly the items most likely to pile up.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	kill(&t);

	t.mock_clock().set_millis(3_600_001);
	sweeper(&t).run_slice();

	assert_eq!(items(&t), 0);
	assert_eq!(states(&t, queue).len(), 0);
}

#[test]
fn test_sweeping_a_dead_item_closes_its_replay_window_for_good() {
	// The other half of the contract above, from the operator's side: retention.done is also the
	// deadline for recovering a dead item. Once swept, replay must say so plainly instead of
	// reporting success against scheduling state that no longer exists - an operator who is told a
	// replay worked will stop looking for the lost work.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	kill(&t);
	t.command(r#"CALL queue::replay("test::jobs", 1)"#);
	assert_eq!(states(&t, queue)[0].status, QueueItemStatus::Ready, "before the sweep it is still recoverable");
	kill(&t);

	t.mock_clock().set_millis(3_600_001);
	sweeper(&t).run_slice();

	let err = t.command_err(r#"CALL queue::replay("test::jobs", 1)"#);
	assert!(err.contains("QUEUE_004"), "{err}");
	assert_eq!(states(&t, queue).len(), 0);
}

#[test]
fn test_a_finished_item_inside_the_window_survives() {
	// The cutoff is the contract. Sweeping one second early destroys audit data the operator was
	// promised, and there is no way to get it back.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	t.mock_clock().set_millis(3_599_999);
	sweeper(&t).run_slice();

	assert_eq!(items(&t), 1, "one millisecond inside the window is still inside it");
	assert_eq!(attempt_count(&t, queue), 1);
	assert_eq!(states(&t, queue).len(), 1);
}

#[test]
fn test_an_unfinished_item_is_never_swept_however_old_it_is() {
	// Age alone must never delete an unfinished promise. A ready item that outlived the window is
	// backlog, not garbage; deleting it silently drops work the caller was told was accepted.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.mock_clock().set_millis(0);
	t.command("INSERT test::jobs [{ id: 1 }]");
	t.command("INSERT test::jobs [{ id: 2 }]");
	let queue = queue_id(&t, "jobs");
	claim_one(&t);

	t.mock_clock().set_millis(86_400_000);
	sweeper(&t).run_slice();

	assert_eq!(items(&t), 2, "a leased item and a ready item both outlive any retention window");
	let statuses: Vec<QueueItemStatus> = states(&t, queue).iter().map(|s| s.status).collect();
	assert_eq!(statuses.len(), 2);
	assert!(statuses.contains(&QueueItemStatus::Leased));
	assert!(statuses.contains(&QueueItemStatus::Ready));
}

#[test]
fn test_a_queue_without_a_declared_window_keeps_everything() {
	// retention.done is opt-in. A queue that never declared one must accumulate, not silently adopt
	// a default that deletes the operator's data.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	t.mock_clock().set_millis(86_400_000_000);
	assert_eq!(sweeper(&t).run_slice(), Progress::Exhausted);

	assert_eq!(items(&t), 1);
	assert_eq!(states(&t, queue).len(), 1);
	assert_eq!(attempt_count(&t, queue), 1);
}

#[test]
fn test_a_terminal_item_with_no_attempt_record_is_left_alone() {
	// The terminal attempt's finished_at is the only clock retention has. With no record and the
	// item row still present, the item cannot be dated - and an undateable item must be preserved
	// and reported, never deleted on the assumption that it is old enough.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	let mut txn = t.inner().begin_command(TestEngine::identity()).unwrap();
	txn.remove(&QueueAttemptKey::encoded(queue, reifydb_value::value::row_number::RowNumber(1), 1)).unwrap();
	txn.commit().unwrap();

	t.mock_clock().set_millis(3_600_001);
	sweeper(&t).run_slice();

	assert_eq!(items(&t), 1, "an item that cannot be dated must survive");
	assert_eq!(states(&t, queue).len(), 1);
}

#[test]
fn test_an_orphan_state_record_from_a_crashed_sweep_is_collected() {
	// Deletion order is fixed: MVCC rows first, single-lane state second. A crash in between leaves
	// a state record pointing at nothing. Without this cleanup every crashed sweep would leak one
	// scheduling record per item, permanently.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	let mut txn = t.inner().begin_command(TestEngine::identity()).unwrap();
	txn.remove(&reifydb_core::key::row::RowKey::encoded(queue, reifydb_value::value::row_number::RowNumber(1)))
		.unwrap();
	txn.remove(&QueueAttemptKey::encoded(queue, reifydb_value::value::row_number::RowNumber(1), 1)).unwrap();
	txn.commit().unwrap();
	assert_eq!(states(&t, queue).len(), 1, "the orphan is planted");

	t.mock_clock().set_millis(3_600_001);
	sweeper(&t).run_slice();

	assert_eq!(states(&t, queue).len(), 0, "the orphaned state record must be collected");
}

#[test]
fn test_a_swept_item_keeps_suppressing_its_deduplication_key() {
	// The deduplication window is the caller's contract and runs on its own ttl. Sweeping the item
	// is a storage decision that must not silently re-open a key the caller was told was taken - a
	// retention window shorter than the dedup ttl would otherwise let the same work run twice.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h }, deduplicate: { by: {id}, ttl: 30d } }"#,
	);
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	t.command("INSERT test::jobs [{ id: 1 }]");
	finish(&t);

	t.mock_clock().set_millis(3_600_001);
	sweeper(&t).run_slice();
	assert_eq!(items(&t), 0, "the item itself is past its retention window");
	assert_eq!(deduplication_count(&t, queue), 1, "the deduplication record outlives the item it named");

	t.command("INSERT test::jobs [{ id: 1 }]");
	assert_eq!(items(&t), 0, "the key is still inside its 30d window, so the re-insert is deduplicated");
}

#[test]
fn test_a_deduplication_record_is_swept_once_its_own_ttl_elapses() {
	// Nothing else ever deletes these records: a key that is never reused is simply overwritten on
	// reuse, so without this sweep a queue with high-cardinality keys grows without bound.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h }, deduplicate: { by: {id}, ttl: 1d } }"#,
	);
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	t.command("INSERT test::jobs [{ id: 1 }]");
	finish(&t);

	t.mock_clock().set_millis(86_400_001);
	sweeper(&t).run_slice();

	assert_eq!(deduplication_count(&t, queue), 0, "the record is past its own ttl");

	t.command("INSERT test::jobs [{ id: 1 }]");
	assert_eq!(items(&t), 1, "once the deduplication window closes the key is reusable");
}

#[test]
fn test_a_forever_deduplication_record_is_never_swept() {
	// `ttl: forever` is an explicit promise that the key can never be reused. A sweep that expired
	// it would let a caller's exactly-once key run a second time, which is the one thing the
	// declaration rules out.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h }, deduplicate: { by: {id}, ttl: forever } }"#,
	);
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	t.command("INSERT test::jobs [{ id: 1 }]");
	finish(&t);

	t.mock_clock().set_millis(86_400_000_000);
	sweeper(&t).run_slice();

	assert_eq!(deduplication_count(&t, queue), 1);
	t.command("INSERT test::jobs [{ id: 1 }]");
	assert_eq!(items(&t), 0, "a forever key stays taken");
}

#[test]
fn test_retention_does_no_work_inside_the_startup_grace_window() {
	// Retention is the destructive task, so it is gated: right after boot the floors it trusts are
	// not yet warm, and deleting on a cold floor is unrecoverable. The gate must suppress the sweep
	// entirely, not merely narrow it.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	finish(&t);

	t.mock_clock().set_millis(3_600_001);
	let gate = RetentionStartupGate::arm(t.inner().clock().clone(), Duration::from_hours_const(1));
	let mut gated = Gated::new(sweeper(&t), gate);
	gated.run_slice();

	assert_eq!(items(&t), 1, "the gate must suppress a destructive sweep during the grace window");
	assert_eq!(states(&t, queue).len(), 1);
}

#[test]
fn test_a_backlog_deeper_than_the_budget_drains_across_slices() {
	// The sweeper shares one lane with every other lifecycle task and scans unfinished items too.
	// Without a cursor a small budget would rescan the same head forever and the tail would never
	// be swept, while the task reported progress every slice.
	let t = engine_with_queue(
		r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retention: { done: 1h } }"#,
	);
	t.mock_clock().set_millis(0);
	for id in 1..=6 {
		t.command(&format!("INSERT test::jobs [{{ id: {id} }}]"));
		finish(&t);
	}
	let queue = queue_id(&t, "jobs");
	t.set_config(ConfigKey::QueueRetentionBatchSize, Value::Uint8(2));

	t.mock_clock().set_millis(3_600_001);
	let mut task = sweeper(&t);
	assert_eq!(task.run_slice(), Progress::Yielded, "a budget of 2 cannot cover 6 records in one slice");

	let mut slices = 1;
	while task.run_slice() == Progress::Yielded {
		slices += 1;
		assert!(slices < 20, "the cursor must advance; it is looping over the same head");
	}

	assert_eq!(items(&t), 0, "every finished item is swept once the slices drain");
	assert_eq!(states(&t, queue).len(), 0);
	assert_eq!(attempt_count(&t, queue), 0);
}
