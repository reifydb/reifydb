// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	queue::EncodedQueueRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column::Column,
		id::QueueId,
		queue::Queue,
		storage::StorageId,
		view::{QueueView, View, ViewStorageKind},
	},
	key::row::RowKey,
	partition::{ordered_by_index, partition_by_indices, placement_of},
};
use reifydb_engine::{
	queue::hydrate::hydrate_queues,
	transaction::operation::queue::{QueueInsertRow, QueueOperations},
};
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	queue::scheduling::{QueueAdmission, admit_ready_items, queue_partition_counters},
	transaction::Transaction,
};
use reifydb_value::value::{
	Value, datetime::DateTime, duration::Duration, partition::Partition, row_number::RowNumber,
};

fn system_ctx() -> Arc<TransactionContext> {
	Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
}

fn find(t: &TestEngine, name: &str) -> (Option<View>, Option<Queue>) {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	let view = catalog.find_view_by_name(&mut txn, namespace.id(), name).unwrap();
	let queue = catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap();
	(view, queue)
}

fn prepared() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4, kind: utf8 } WITH { time: processing }");
	t
}

const FULL: &str = r#"CREATE DEFERRED QUEUE VIEW test::q { id: int4, kind: utf8 } WITH {
		partition: { by: { kind } },
		fifo: { ordered_by: id },
		retention: { done: 7d },
		retry: { attempts: 9, backoff: 30s }
	} AS { from test::src }"#;

#[test]
fn every_option_reaches_the_queue_the_claim_path_reads() {
	// A dropped or mis-scaled option silently changes when items retry, block, expire or collapse.
	let t = prepared();
	t.admin(FULL);
	let (view, queue) = find(&t, "q");
	let queue = queue.expect("a queue view must own a queue");
	assert_eq!(queue.partition_by, vec!["kind".to_string()]);
	assert_eq!(queue.ordered_by(), Some("id"));
	assert_eq!(queue.retention.done, Some(Duration::from_days(7).unwrap()));
	assert_eq!(queue.retry.attempts, 9);
	assert_eq!(queue.retry.backoff, Duration::from_seconds_const(30));
	assert_eq!(queue.deduplicate, None, "a queue view never suppresses a change");

	let view = view.expect("the view must exist");
	assert_eq!(view.storage_kind(), ViewStorageKind::Queue);
	assert_eq!(view.storage_id(), StorageId::Queue(queue.id), "the view must read the rows the queue schedules");
	assert_eq!(view.partition_by(), &["kind".to_string()]);
}

#[test]
fn the_reserved_change_column_is_the_last_column_of_both_the_view_and_the_queue() {
	// The two must agree on the row shape, otherwise claim decodes items a scan cannot.
	let t = prepared();
	t.admin(FULL);
	let (view, queue) = find(&t, "q");
	let names = |columns: &[Column]| -> Vec<String> { columns.iter().map(|column| column.name.clone()).collect() };
	let expected = vec!["id".to_string(), "kind".to_string(), QueueView::CHANGE_COLUMN.to_string()];
	assert_eq!(names(view.unwrap().columns()), expected);
	assert_eq!(names(&queue.unwrap().columns), expected);
}

#[test]
fn a_declared_change_column_fails_the_create_and_leaves_no_queue_behind() {
	// A half created queue would keep the name taken with no view able to feed it.
	let t = prepared();
	let error = t.admin_err(
		"CREATE DEFERRED QUEUE VIEW test::q { change: utf8 } WITH { fifo: {} } AS { from test::src }",
	);
	assert!(error.contains("CA_132"), "{error}");
	assert!(error.contains("column change is reserved"), "{error}");
	assert_eq!(find(&t, "q"), (None, None));
}

#[test]
fn dropping_the_view_removes_the_queue_and_its_scheduling_state() {
	// Scheduling state outliving the queue would keep counting items no worker can claim.
	let t = prepared();
	t.admin(FULL);
	let queue: QueueId = find(&t, "q").1.unwrap().id;
	let single = t.inner().single().clone();
	admit_ready_items(
		&system_ctx(),
		&single,
		queue,
		Partition(5),
		&[QueueAdmission {
			row: RowNumber(1),
			key_hash: None,
			not_before: None,
		}],
	)
	.unwrap();
	assert_eq!(
		queue_partition_counters(&system_ctx(), &single, queue).unwrap().len(),
		1,
		"precondition: the item is scheduled"
	);

	t.admin("DROP VIEW test::q");

	assert_eq!(find(&t, "q"), (None, None));
	assert!(queue_partition_counters(&system_ctx(), &single, queue).unwrap().is_empty());
}

#[test]
fn dropping_the_namespace_removes_the_view_and_its_queue_once() {
	// The namespace drop walks views and queues separately, so a view drop that also dropped the queue would fail.
	let t = prepared();
	t.admin(FULL);
	t.admin("DROP VIEW test::q");
	t.admin(FULL);
	t.admin("DROP NAMESPACE test CASCADE");
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	assert!(catalog.list_queues(&mut txn).unwrap().is_empty());
	assert!(catalog.list_views_all(&mut txn).unwrap().is_empty());
}

#[test]
fn the_queue_behind_a_view_refuses_direct_inserts_and_drops_but_a_plain_queue_does_not() {
	// A direct insert would create an item with no change, and a direct drop would strand the flow's sink.
	let t = prepared();
	t.admin(FULL);
	let insert = t.command_err(r#"INSERT test::q [{ id: 1, kind: "a", change: "insert" }]"#);
	assert!(insert.contains("CA_136") && insert.contains("insert into the source"), "{insert}");
	let drop = t.admin_err("DROP QUEUE test::q");
	assert!(drop.contains("CA_136") && drop.contains("use DROP VIEW"), "{drop}");
	assert!(find(&t, "q").1.is_some());

	t.admin("CREATE QUEUE test::plain { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::plain [{ id: 1 }]");
	t.admin("DROP QUEUE test::plain");
	assert!(find(&t, "plain").1.is_none());
}

#[test]
fn a_flow_refuses_to_read_a_queue_view() {
	// Queue rows are not view changes to a flow, so a downstream view would silently stay empty.
	let t = prepared();
	t.admin(FULL);
	let error = t.admin_err("CREATE DEFERRED VIEW test::down { id: int4 } AS { from test::q }");
	assert!(error.contains("FLOW_040"), "{error}");
}

#[test]
fn deduplicate_is_refused_because_every_change_must_become_an_item() {
	// A suppressed change would break the outbox contract and need state nothing sweeps.
	let t = prepared();
	let error = t.admin_err(
		"CREATE DEFERRED QUEUE VIEW test::q { id: int4 } WITH { fifo: {}, deduplicate: { by: { id } } } AS { from test::src }",
	);
	assert!(error.contains("does not support `deduplicate`"), "{error}");
	assert_eq!(find(&t, "q"), (None, None));
}

#[test]
fn partition_and_ordered_by_cannot_name_the_reserved_change_column() {
	// Keying on change would split one source row's changes across FIFOs and reorder them.
	let t = prepared();
	for with in ["partition: { by: { change } }, fifo: {}", "fifo: { ordered_by: change }"] {
		let error = t.admin_err(&format!(
			"CREATE DEFERRED QUEUE VIEW test::q {{ id: int4 }} WITH {{ {with} }} AS {{ from test::src }}"
		));
		assert!(error.contains("change"), "{with}: {error}");
		assert!(!error.contains("reserved"), "{with}: the column is simply not declared: {error}");
		assert_eq!(find(&t, "q"), (None, None), "{with}");
	}
}

fn write_item(t: &TestEngine, queue: &Queue, row: u64, track: bool) -> EncodedBytes {
	let catalog = t.inner().catalog();
	let mut admin = t.inner().begin_admin(TestEngine::identity()).unwrap();
	let shape = catalog.get_or_create_queue_shape(&mut Transaction::Admin(&mut admin), queue).unwrap();
	let mut encoded = shape.allocate_queue();
	shape.set_value(&mut encoded, 0, &Value::Int4(row as i32));
	shape.set_value(&mut encoded, 1, &Value::Utf8("a".to_string()));
	shape.set_value(&mut encoded, 2, &Value::Utf8(QueueView::CHANGE_INSERT.to_string()));
	encoded.set_timestamps(DateTime::from_nanos(1), DateTime::from_nanos(1));
	let bytes = encoded.freeze_bytes();
	if track {
		let placement = placement_of(
			&shape,
			EncodedQueueRow::view(&bytes),
			&partition_by_indices(queue),
			ordered_by_index(queue).unwrap(),
		);
		admin.insert_queue(
			queue,
			&[QueueInsertRow {
				row_number: RowNumber(row),
				partition: placement.partition,
				key_hash: placement.key_hash,
				not_before: None,
				encoded: bytes.clone(),
			}],
		)
		.unwrap();
	} else {
		admin.set(vec![(RowKey::new(queue.id, RowNumber(row)).into(), bytes.clone())]).unwrap();
	}
	assert!(admin.commit().unwrap() > CommitVersion(0));
	bytes
}

#[test]
fn a_slice_admitted_while_the_drop_is_still_open_is_swept_by_the_drop_commit() {
	// The drop wipes scheduling before it commits, so an admission landing in that gap must not survive it.
	let t = prepared();
	t.admin(FULL);
	let queue = find(&t, "q").1.unwrap();
	let view = find(&t, "q").0.unwrap();
	let single = t.inner().single().clone();
	let catalog = t.inner().catalog();

	let mut admin = t.inner().begin_admin(TestEngine::identity()).unwrap();
	catalog.drop_view(&mut admin, view).unwrap();
	admit_ready_items(
		&system_ctx(),
		&single,
		queue.id,
		Partition(5),
		&[QueueAdmission {
			row: RowNumber(1),
			key_hash: None,
			not_before: None,
		}],
	)
	.unwrap();
	assert_eq!(
		queue_partition_counters(&system_ctx(), &single, queue.id).unwrap().len(),
		1,
		"precondition: the race happened"
	);
	admin.commit().unwrap();

	assert!(queue_partition_counters(&system_ctx(), &single, queue.id).unwrap().is_empty());
}

#[test]
fn hydration_admits_view_items_where_the_sink_would_have_placed_them() {
	// A restart that lost scheduling state must find view items by the queue row keys and the stored row.
	let t = prepared();
	t.admin(FULL);
	let queue = find(&t, "q").1.unwrap();
	let bytes = write_item(&t, &queue, 1, false);
	let single = t.inner().single().clone();
	assert!(
		queue_partition_counters(&system_ctx(), &single, queue.id).unwrap().is_empty(),
		"precondition: not admitted"
	);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 1);

	let catalog = t.inner().catalog();
	let mut query = t.inner().begin_query(TestEngine::identity()).unwrap();
	let shape = catalog
		.get_or_load_row_shape(EncodedQueueRow::view(&bytes).fingerprint(), &mut Transaction::Query(&mut query))
		.unwrap()
		.unwrap();
	let placement = placement_of(
		&shape,
		EncodedQueueRow::view(&bytes),
		&partition_by_indices(&queue),
		ordered_by_index(&queue).unwrap(),
	);
	let counters = queue_partition_counters(&system_ctx(), &single, queue.id).unwrap();
	assert_eq!(counters.len(), 1);
	assert_eq!(counters[0].0, placement.partition);
	assert_eq!(counters[0].1.depth, 1);
}
