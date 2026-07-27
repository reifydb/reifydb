// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::queue::Queue;
use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::duration::Duration;

fn find_queue(t: &TestEngine, namespace: &str, name: &str) -> Option<Queue> {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, namespace).unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap()
}

/// An omitted WITH block must land the declared defaults in the catalog, not
/// zeroes: a queue created with 0 partitions or 0 retry attempts could never
/// deliver work once the scheduling lane arrives.
#[test]
fn test_create_queue_without_with_applies_defaults() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { order_id: uuid7, kind: utf8 }");

	let queue = find_queue(&t, "test", "jobs").expect("queue should exist");

	assert_eq!(queue.partitions, Queue::DEFAULT_PARTITIONS);
	assert_eq!(queue.ordered_by, None);
	assert_eq!(queue.retention.done, None);
	assert_eq!(queue.retry.attempts, Queue::DEFAULT_RETRY_ATTEMPTS);
	assert_eq!(queue.retry.backoff, Queue::DEFAULT_RETRY_BACKOFF);
	assert_eq!(queue.columns.len(), 2);
	assert!(!queue.underlying);
}

/// Every declared option must reach the catalog unchanged, including the two
/// Duration fields - a dropped or mis-scaled duration silently changes when
/// items expire and how fast they retry.
#[test]
fn test_create_queue_with_all_options_round_trips() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(r#"CREATE QUEUE test::jobs { order_id: uuid7, kind: utf8 } WITH {
			partitions: 32,
			ordered_by: order_id,
			retention: { done: "7d" },
			retry: { attempts: 9, backoff: "30s" }
		}"#);

	let queue = find_queue(&t, "test", "jobs").expect("queue should exist");

	assert_eq!(queue.partitions, 32);
	assert_eq!(queue.ordered_by, Some("order_id".to_string()));
	assert_eq!(queue.retention.done, Some(Duration::from_days(7).unwrap()));
	assert_eq!(queue.retry.attempts, 9);
	assert_eq!(queue.retry.backoff, Duration::from_seconds_const(30));
}

/// Partition count is the concurrency knob for the future claim lane; 0 would
/// make the queue undeliverable and an unbounded count would explode the
/// scheduling keyspace, so both ends of the range must be rejected at compile.
#[test]
fn test_create_queue_rejects_out_of_range_partitions() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let too_few = t.admin_err("CREATE QUEUE test::a { id: int4 } WITH { partitions: 0 }");
	assert!(too_few.contains("partitions"), "error should name the option, got: {too_few}");

	let too_many = t.admin_err("CREATE QUEUE test::b { id: int4 } WITH { partitions: 1025 }");
	assert!(too_many.contains("partitions"), "error should name the option, got: {too_many}");

	assert!(find_queue(&t, "test", "a").is_none());
	assert!(find_queue(&t, "test", "b").is_none());
}

#[test]
fn test_create_queue_accepts_range_boundaries() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::low { id: int4 } WITH { partitions: 1 }");
	t.admin("CREATE QUEUE test::high { id: int4 } WITH { partitions: 1024 }");

	assert_eq!(find_queue(&t, "test", "low").unwrap().partitions, 1);
	assert_eq!(find_queue(&t, "test", "high").unwrap().partitions, 1024);
}

/// ordered_by names the column whose values get per-key FIFO ordering. Pointing
/// it at a column that does not exist must fail loudly at DDL time rather than
/// producing a queue whose ordering key can never be read.
#[test]
fn test_create_queue_rejects_unknown_ordered_by_column() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { ordered_by: missing }");

	assert!(err.contains("missing"), "error should carry the column fragment, got: {err}");
	assert!(find_queue(&t, "test", "jobs").is_none());
}

/// A retry budget below 1 means an item dies before its first attempt, and a
/// zero backoff would spin the reaper; both are rejected at compile.
#[test]
fn test_create_queue_rejects_degenerate_retry() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let attempts = t.admin_err("CREATE QUEUE test::a { id: int4 } WITH { retry: { attempts: 0 } }");
	assert!(attempts.contains("attempts"), "error should name the option, got: {attempts}");

	let backoff = t.admin_err(r#"CREATE QUEUE test::b { id: int4 } WITH { retry: { backoff: "0s" } }"#);
	assert!(backoff.contains("positive"), "error should explain the positivity rule, got: {backoff}");
}

/// DDL is admin-gated across the board; a queue must not be creatable from an
/// ordinary command transaction.
#[test]
fn test_create_queue_requires_admin() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	t.command_err("CREATE QUEUE test::jobs { id: int4 }");
	assert!(find_queue(&t, "test", "jobs").is_none());

	t.admin("CREATE QUEUE test::jobs { id: int4 }");
	assert!(find_queue(&t, "test", "jobs").is_some());
}

#[test]
fn test_create_queue_duplicate_name_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 }");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 }");

	assert!(err.contains("CA_095"), "duplicate queue should use the queue-specific code, got: {err}");
}

/// system::queues is the operator-facing view of the definitions; ordered_by
/// must render as none when undeclared rather than as an empty column name.
#[test]
fn test_system_queues_lists_definitions() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::plain { id: int4 }");
	t.admin("CREATE QUEUE test::keyed { id: int4 } WITH { partitions: 4, ordered_by: id }");

	let frames = t.query("FROM system::queues SORT { name: asc }");
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 2);

	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "keyed");
	assert_eq!(rows[0].get::<u64>("partitions").unwrap().unwrap(), 4);
	assert_eq!(rows[0].get::<String>("ordered_by").unwrap().unwrap(), "id");

	assert_eq!(rows[1].get::<String>("name").unwrap().unwrap(), "plain");
	assert_eq!(rows[1].get::<u64>("partitions").unwrap().unwrap(), Queue::DEFAULT_PARTITIONS as u64);
	assert_eq!(rows[1].get::<String>("ordered_by").unwrap(), None);
}

#[test]
fn test_drop_queue_removes_the_definition() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 }");

	let frames = t.admin("DROP QUEUE test::jobs");
	assert_eq!(frames[0].rows().next().unwrap().get::<bool>("dropped").unwrap().unwrap(), true);

	assert!(find_queue(&t, "test", "jobs").is_none());
	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 0);
}

/// A dropped name must be reusable: if the namespace link row survived, the
/// name would stay permanently taken.
#[test]
fn test_dropped_queue_name_can_be_recreated() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 }");
	let first = find_queue(&t, "test", "jobs").unwrap().id;

	t.admin("DROP QUEUE test::jobs");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { partitions: 8 }");

	let second = find_queue(&t, "test", "jobs").unwrap();
	assert_ne!(second.id, first);
	assert_eq!(second.partitions, 8);
}

/// IF EXISTS is the difference between a guarded teardown and a hard failure;
/// both branches must behave as declared.
#[test]
fn test_drop_missing_queue_honours_if_exists() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let frames = t.admin("DROP QUEUE IF EXISTS test::nope");
	assert_eq!(frames[0].rows().next().unwrap().get::<bool>("dropped").unwrap().unwrap(), false);

	let err = t.admin_err("DROP QUEUE test::nope");
	assert!(err.contains("CA_096"), "missing queue should report queue_not_found, got: {err}");
}

/// Dropping a namespace must take its queues with it; a surviving definition
/// would be unreachable by name yet still occupy its id and columns.
#[test]
fn test_drop_namespace_cascades_to_queues() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 }");
	t.admin("CREATE QUEUE test::other { id: int4 }");

	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 2);

	t.admin("DROP NAMESPACE test");

	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 0);
}

/// Step 1 declares the shell only: until the item lane lands, a queue must not
/// be addressable as a source, and it must not silently resolve to something
/// else either.
#[test]
fn test_queue_is_not_yet_a_source() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 }");

	let err = t.query_err("FROM test::jobs");
	assert!(err.contains("jobs"), "error should name the unresolved source, got: {err}");

	let insert = t.command_err("INSERT test::jobs [{ id: 1 }]");
	assert!(insert.contains("jobs"), "error should name the unresolved target, got: {insert}");
}

/// Definitions must survive a value that only exists in the WITH block: this is
/// the codec check for the optional Duration field specifically.
#[test]
fn test_retention_only_queue_keeps_its_duration() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(r#"CREATE QUEUE test::jobs { id: int4 } WITH { retention: { done: "1h" } }"#);

	let queue = find_queue(&t, "test", "jobs").unwrap();

	assert_eq!(queue.retention.done, Some(Duration::from_hours_const(1)));
	assert_eq!(queue.retry.attempts, Queue::DEFAULT_RETRY_ATTEMPTS);
	assert_eq!(queue.retry.backoff, Queue::DEFAULT_RETRY_BACKOFF);
}
