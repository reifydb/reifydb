// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::bytes::RowBuilder;
use reifydb_core::{
	interface::catalog::{config::GetConfig, queue::Queue, view::QueueView},
	lifecycle::{metrics::RetentionMetrics, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::{
	engine::StandardEngine,
	transaction::operation::queue::{QueueInsertRow, QueueOperations},
};
use reifydb_sub_lifecycle::{plane::RetentionPlane, queue::retention::QueueRetentionTask};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber};

fn sweeper(t: &TestEngine) -> QueueRetentionTask {
	let engine: &StandardEngine = t.inner();
	QueueRetentionTask::new(
		engine.clone(),
		RetentionPlane::for_engine(engine, RetentionMetrics::new()),
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
fn a_finished_view_item_is_swept_and_disappears_from_the_view() {
	// The sweep deletes under the queue's row keys, so the view must read the same keys or show ghost items.
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
	assert_eq!(t.query("from test::q").first().map_or(0, |frame| frame.row_count()), 0);
}
