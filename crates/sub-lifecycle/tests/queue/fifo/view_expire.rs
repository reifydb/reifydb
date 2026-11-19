// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, mpsc};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::RowBuilder};
use reifydb_core::{
	actors::flow::FlowActorMessage,
	delta::{Delta, RemoveVisibility},
	interface::{
		catalog::{
			config::GetConfig,
			flow::FlowId,
			id::{QueueId, ViewId},
			queue::Queue,
			view::QueueView,
		},
		store::SingleVersionRange,
	},
	key::{
		any::TaggedKey,
		queue::{QueueAttemptKey, QueueItemStateKey},
		row::RowKey,
	},
	lifecycle::{class::LifecycleClass, metrics::LifecycleMetrics, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::{
	engine::StandardEngine,
	transaction::operation::queue::{QueueInsertRow, QueueOperations},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::transaction::TransactionContext,
};
use reifydb_sub_lifecycle::{plane::LifecyclePlane, queue::expire::QueueExpireTask};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{
	Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition,
	row_number::RowNumber,
};

struct HandoffRecorder {
	received: mpsc::Sender<(Vec<EncodedKey>, RemoveVisibility)>,
}

impl Actor for HandoffRecorder {
	type State = ();
	type Message = FlowActorMessage;

	fn init(&self, _ctx: &Context<Self::Message>) {}

	fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		if let FlowActorMessage::RemoveExpired {
			keys,
			visibility,
		} = msg
		{
			self.received.send((keys, visibility)).expect("the test still listens for hand-offs");
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

fn handoff_recorder(
	engine: &StandardEngine,
	received: mpsc::Sender<(Vec<EncodedKey>, RemoveVisibility)>,
) -> ActorRef<FlowActorMessage> {
	engine.spawner()
		.spawn_ephemeral(
			"handoff-recorder",
			HandoffRecorder {
				received,
			},
		)
		.actor_ref()
		.clone()
}

fn sweeper(t: &TestEngine) -> QueueExpireTask {
	let engine: &StandardEngine = t.inner();
	QueueExpireTask::new(
		engine.clone(),
		LifecyclePlane::for_engine(engine, LifecycleMetrics::new()),
		engine.clock().clone(),
		Arc::new(engine.catalog()) as Arc<dyn GetConfig>,
	)
}

fn queue_of_view(t: &TestEngine) -> Queue {
	let catalog = t.inner().catalog();
	let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), "q").unwrap().unwrap()
}

fn enqueue(t: &TestEngine, queue: &Queue) {
	let catalog = t.inner().catalog();
	let mut admin = t.inner().begin_admin(TestEngine::identity()).unwrap();
	let shape = catalog.get_or_create_queue_shape(&mut Transaction::Admin(&mut admin), queue).unwrap();
	let mut encoded = shape.allocate_queue();
	shape.set_value(&mut encoded, 0, &Value::Int4(1));
	shape.set_value(&mut encoded, 1, &Value::Utf8(QueueView::CHANGE_INSERT.to_string()));
	encoded.set_timestamps(DateTime::from_nanos(1), DateTime::from_nanos(1));
	admin.insert_queue(
		queue,
		&[QueueInsertRow {
			row_number: RowNumber(1),
			partition: Partition(0),
			key_hash: None,
			not_before: None,
			encoded: encoded.freeze_bytes(),
		}],
	)
	.unwrap();
	admin.commit().unwrap();
}

#[test]
fn a_finished_view_item_with_no_running_flow_stays_in_the_view() {
	// Without a running flow expiry must leave the item, or it becomes a second writer on a view queue.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4 } WITH { time: processing }");
	t.admin(
		"CREATE DEFERRED QUEUE VIEW test::q { id: int4 } WITH { fifo: {}, retention: { done: 1h } } AS { from test::src }",
	);
	let queue = queue_of_view(&t);
	enqueue(&t, &queue);
	assert_eq!(t.query("from test::q")[0].row_count(), 1, "precondition: the view shows the item");

	t.mock_clock().set_millis(0);
	let claimed = t.command(r#"CALL queue::claim("w1", "test::q", 1, duration::seconds(30))"#);
	let Value::Utf8(token) = claimed[0].column("token").unwrap().unwrap().get_value(0) else {
		panic!("the token must be utf8");
	};
	t.command(&format!(r#"CALL queue::ack("{token}")"#));

	t.mock_clock().set_millis(3_599_999);
	sweeper(&t).run_slice();
	assert_eq!(t.query("from test::q")[0].row_count(), 1, "the declared window must hold until it ends");

	t.mock_clock().set_millis(3_600_001);
	assert_eq!(sweeper(&t).run_slice(), Progress::Exhausted);
	assert_eq!(
		t.query("from test::q")[0].row_count(),
		1,
		"with no running flow the finished item must stay in the view"
	);
}

fn finished_view_item(t: &TestEngine) -> Queue {
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4 } WITH { time: processing }");
	t.admin(
		"CREATE DEFERRED QUEUE VIEW test::q { id: int4 } WITH { fifo: {}, retention: { done: 1h } } AS { from test::src }",
	);
	let queue = queue_of_view(t);
	enqueue(t, &queue);
	t.mock_clock().set_millis(0);
	let claimed = t.command(r#"CALL queue::claim("w1", "test::q", 1, duration::seconds(30))"#);
	let Value::Utf8(token) = claimed[0].column("token").unwrap().unwrap().get_value(0) else {
		panic!("the token must be utf8");
	};
	t.command(&format!(r#"CALL queue::ack("{token}")"#));
	t.mock_clock().set_millis(3_600_001);
	queue
}

fn view_of_queue(t: &TestEngine) -> ViewId {
	let catalog = t.inner().catalog();
	let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_view_by_name(&mut txn, namespace.id(), "q").unwrap().unwrap().id()
}

fn expire(t: &TestEngine) -> (QueueExpireTask, LifecyclePlane) {
	let engine: &StandardEngine = t.inner();
	let plane = LifecyclePlane::for_engine(engine, LifecycleMetrics::new());
	let task = QueueExpireTask::new(
		engine.clone(),
		plane.clone(),
		engine.clock().clone(),
		Arc::new(engine.catalog()) as Arc<dyn GetConfig>,
	);
	(task, plane)
}

fn row_stored(t: &TestEngine, queue: QueueId) -> bool {
	let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
	query.get(&[RowKey::new(queue, RowNumber(1)).into()]).unwrap().pop().flatten().is_some()
}

fn attempts(t: &TestEngine, queue: QueueId) -> usize {
	let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query);
	txn.range(QueueAttemptKey::queue_scan(queue), RangeScope::All, 1024).unwrap().map(|item| item.unwrap()).count()
}

fn item_states(t: &TestEngine, queue: QueueId) -> usize {
	let store = t.inner().single().read_store();
	let ctx = TransactionContext::system(t.inner().runtime_context().clone());
	SingleVersionRange::range_batch(&store, &ctx, QueueItemStateKey::queue_scan(queue).encode(), 1024)
		.unwrap()
		.items
		.len()
}

fn deleted(plane: &LifecyclePlane) -> u64 {
	plane.snapshot(LifecycleClass::QueueDoneTtl).work_done
}

fn record_handoffs(t: &TestEngine) -> mpsc::Receiver<(Vec<EncodedKey>, RemoveVisibility)> {
	let (sender, received) = mpsc::channel();
	t.inner().flow_writers().register(view_of_queue(t), FlowId(1), handoff_recorder(t.inner(), sender));
	received
}

fn removed_keys_after(t: &TestEngine, before: u64) -> Vec<Vec<TaggedKey>> {
	t.commit_entries()
		.iter()
		.filter(|entry| entry.version.commit.0 > before)
		.map(|entry| {
			entry.deltas
				.iter()
				.filter_map(|delta| match delta {
					Delta::Remove {
						key,
						..
					} => Some(key.clone()),
					Delta::Set {
						..
					} => None,
				})
				.collect()
		})
		.collect()
}

#[test]
fn a_view_queue_with_no_running_flow_keeps_its_item_and_its_attempts() {
	// A view queue with no flow must be skipped as backlog, never swept by a second writer.
	let t = TestEngine::new();
	let queue = finished_view_item(&t);
	let (mut task, plane) = expire(&t);

	assert_eq!(task.run_slice(), Progress::Exhausted, "a skipped view queue must never make expiry yield");

	assert!(row_stored(&t, queue.id), "with no flow the item row must stay");
	assert_eq!(attempts(&t, queue.id), 1, "with no flow the attempt keys must stay");
	assert_eq!(item_states(&t, queue.id), 1, "with no flow the item state must stay");
	assert_eq!(
		plane.snapshot(LifecycleClass::QueueDoneTtl).backlog_hint,
		1,
		"a skipped view queue must be reported as backlog"
	);
}

#[test]
fn a_handed_view_item_keeps_its_attempts_until_the_row_is_gone() {
	// Attempt keys removed at send would leave a row that expiry can never date again.
	let t = TestEngine::new();
	let queue = finished_view_item(&t);
	let received = record_handoffs(&t);
	let (mut task, plane) = expire(&t);
	let before = t.current_version().unwrap();

	for slice in 1..=2 {
		task.run_slice();
		let (keys, visibility) = received
			.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
			.expect("expiry must hand the row to the flow");
		assert_eq!(
			keys,
			vec![TaggedKey::from(RowKey::new(queue.id, RowNumber(1))).encode()],
			"slice {slice}: the hand-off must carry exactly the item's row key"
		);
		assert_eq!(visibility, RemoveVisibility::Announced, "slice {slice}: a queue row remove is announced");
		assert_eq!(attempts(&t, queue.id), 1, "slice {slice}: the attempt keys must wait for the row to go");
		assert_eq!(item_states(&t, queue.id), 1, "slice {slice}: the item state must wait for the row to go");
	}
	assert_eq!(t.current_version().unwrap(), before, "expiry must not commit a view queue's row itself");
	assert_eq!(deleted(&plane), 0, "an item must not count as deleted at send");
}

#[test]
fn a_view_item_whose_row_is_gone_loses_only_its_attempt_keys_and_then_its_state() {
	// The row must go first, then the attempt keys, then the state, and the item must count exactly once.
	let t = TestEngine::new();
	let queue = finished_view_item(&t);
	let received = record_handoffs(&t);
	let (mut task, plane) = expire(&t);

	task.run_slice();
	received.recv_timeout(Duration::from_seconds(10).unwrap().to_std()).expect("precondition: the row is sent");
	let mut txn = t.inner().begin_command(IdentityId::system()).unwrap();
	txn.remove(vec![(RowKey::new(queue.id, RowNumber(1)).into(), None)]).unwrap();
	txn.commit().unwrap();
	assert!(!row_stored(&t, queue.id), "precondition: the row is gone, as the flow would leave it");
	let before = t.current_version().unwrap().0;

	task.run_slice();
	assert_eq!(attempts(&t, queue.id), 0, "with the row gone the attempt keys must go");
	assert_eq!(item_states(&t, queue.id), 1, "the item state must wait for the next slice");
	let commits = removed_keys_after(&t, before);
	assert_eq!(commits.len(), 1, "the attempt keys must go in exactly one commit");
	assert!(
		!commits[0].is_empty() && commits[0].iter().all(|key| matches!(key, TaggedKey::QueueAttempt(_))),
		"expiry's own commit must hold attempt keys only: {:?}",
		commits[0]
	);
	assert!(
		received.recv_timeout(Duration::from_milliseconds(200).unwrap().to_std()).is_err(),
		"a row that is gone must never be handed off again"
	);
	assert_eq!(deleted(&plane), 1, "the item must count once, when its attempt keys go");

	task.run_slice();
	assert_eq!(item_states(&t, queue.id), 0, "the item state must go once the attempt record is gone");
	assert_eq!(deleted(&plane), 1, "removing the state must not count the item again");
}

#[test]
fn a_plain_queue_item_is_still_swept_in_one_commit() {
	// The view queue branch must never leak into a plain queue, which expiry still owns alone.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, retention: { done: 1h } }");
	t.mock_clock().set_millis(0);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let token = {
		let claimed = t.command(r#"CALL queue::claim("w1", "test::jobs", 1, duration::seconds(30))"#);
		let Value::Utf8(token) = claimed[0].column("token").unwrap().unwrap().get_value(0) else {
			panic!("the token must be utf8");
		};
		token
	};
	t.command(&format!(r#"CALL queue::ack("{token}")"#));
	let queue = {
		let catalog = t.inner().catalog();
		let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
		let mut txn = Transaction::Query(&mut query);
		let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
		catalog.find_queue_by_name(&mut txn, namespace.id(), "jobs").unwrap().unwrap()
	};
	t.mock_clock().set_millis(3_600_001);
	let before = t.current_version().unwrap().0;

	let (mut task, _plane) = expire(&t);
	assert_eq!(task.run_slice(), Progress::Exhausted);

	assert!(!row_stored(&t, queue.id), "the plain queue row must be gone");
	assert_eq!(attempts(&t, queue.id), 0, "the attempt keys must be gone");
	assert_eq!(item_states(&t, queue.id), 0, "the item state must be gone");
	let commits = removed_keys_after(&t, before);
	assert_eq!(commits.len(), 1, "a plain queue item must be swept in exactly one commit");
	assert!(
		commits[0].iter().any(|key| matches!(key, TaggedKey::QueueAttempt(_)))
			&& commits[0].iter().any(|key| matches!(key, TaggedKey::Row(_))),
		"the one commit must hold both the attempt keys and the row: {:?}",
		commits[0]
	);
}
