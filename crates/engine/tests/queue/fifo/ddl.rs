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

#[test]
fn test_create_queue_with_bare_dispatch_applies_defaults() {
	// A queue landing 0 partitions or 0 retry attempts could never deliver work.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { order_id: uuid7, kind: utf8 } WITH { fifo: {} }");

	let queue = find_queue(&t, "test", "jobs").expect("queue should exist");

	assert_eq!(queue.partitions(), Queue::DEFAULT_PARTITIONS);
	assert_eq!(queue.ordered_by(), None);
	assert_eq!(queue.retention.done, None);
	assert_eq!(queue.retry.attempts, Queue::DEFAULT_RETRY_ATTEMPTS);
	assert_eq!(queue.retry.backoff, Queue::DEFAULT_RETRY_BACKOFF);
	assert_eq!(queue.columns.len(), 2);
	assert!(!queue.underlying);
}

#[test]
fn test_create_queue_with_all_options_round_trips() {
	// A dropped or mis-scaled Duration silently changes when items expire and how fast they retry.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(r#"CREATE QUEUE test::jobs { order_id: uuid7, kind: utf8 } WITH {
			fifo: { partitions: 32, ordered_by: order_id },
			retention: { done: "7d" },
			retry: { attempts: 9, backoff: "30s" }
		}"#);

	let queue = find_queue(&t, "test", "jobs").expect("queue should exist");

	assert_eq!(queue.partitions(), 32);
	assert_eq!(queue.ordered_by(), Some("order_id"));
	assert_eq!(queue.retention.done, Some(Duration::from_days(7).unwrap()));
	assert_eq!(queue.retry.attempts, 9);
	assert_eq!(queue.retry.backoff, Duration::from_seconds_const(30));
}

#[test]
fn test_create_queue_rejects_out_of_range_partitions() {
	// 0 partitions makes the queue undeliverable; an unbounded count explodes the scheduling keyspace.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let too_few = t.admin_err("CREATE QUEUE test::a { id: int4 } WITH { fifo: { partitions: 0 } }");
	assert!(too_few.contains("partitions"), "error should name the option, got: {too_few}");

	let too_many = t.admin_err("CREATE QUEUE test::b { id: int4 } WITH { fifo: { partitions: 1025 } }");
	assert!(too_many.contains("partitions"), "error should name the option, got: {too_many}");

	assert!(find_queue(&t, "test", "a").is_none());
	assert!(find_queue(&t, "test", "b").is_none());
}

#[test]
fn test_create_queue_accepts_range_boundaries() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::low { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.admin("CREATE QUEUE test::high { id: int4 } WITH { fifo: { partitions: 1024 } }");

	assert_eq!(find_queue(&t, "test", "low").unwrap().partitions(), 1);
	assert_eq!(find_queue(&t, "test", "high").unwrap().partitions(), 1024);
}

#[test]
fn test_create_queue_rejects_unknown_ordered_by_column() {
	// Otherwise the queue exists with an ordering key that can never be read.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { ordered_by: missing } }");

	assert!(err.contains("missing"), "error should carry the column fragment, got: {err}");
	assert!(find_queue(&t, "test", "jobs").is_none());
}

#[test]
fn test_create_queue_rejects_degenerate_retry() {
	// A budget below 1 kills an item before its first attempt; a zero backoff spins the reaper.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let attempts = t.admin_err("CREATE QUEUE test::a { id: int4 } WITH { fifo: {}, retry: { attempts: 0 } }");
	assert!(attempts.contains("attempts"), "error should name the option, got: {attempts}");

	let backoff = t.admin_err(r#"CREATE QUEUE test::b { id: int4 } WITH { fifo: {}, retry: { backoff: "0s" } }"#);
	assert!(backoff.contains("positive"), "error should explain the positivity rule, got: {backoff}");
}

#[test]
fn test_create_queue_requires_admin() {
	// DDL is admin-gated across the board, so an ordinary command transaction must not create one.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	t.command_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	assert!(find_queue(&t, "test", "jobs").is_none());

	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	assert!(find_queue(&t, "test", "jobs").is_some());
}

#[test]
fn test_create_queue_duplicate_name_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	assert!(err.contains("CA_095"), "duplicate queue should use the queue-specific code, got: {err}");
}

#[test]
fn test_system_queues_lists_definitions() {
	// An undeclared ordered_by must render as none, not as an empty column name.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::plain { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE QUEUE test::keyed { id: int4 } WITH { fifo: { partitions: 4, ordered_by: id } }");

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
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.admin("DROP QUEUE test::jobs");
	assert_eq!(frames[0].rows().next().unwrap().get::<bool>("dropped").unwrap().unwrap(), true);

	assert!(find_queue(&t, "test", "jobs").is_none());
	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 0);
}

#[test]
fn test_dropped_queue_name_can_be_recreated() {
	// A surviving namespace link row would leave the name permanently taken.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	let first = find_queue(&t, "test", "jobs").unwrap().id;

	t.admin("DROP QUEUE test::jobs");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 8 } }");

	let second = find_queue(&t, "test", "jobs").unwrap();
	assert_ne!(second.id, first);
	assert_eq!(second.partitions(), 8);
}

#[test]
fn test_drop_missing_queue_honours_if_exists() {
	// IF EXISTS is the difference between a guarded teardown and a hard failure.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let frames = t.admin("DROP QUEUE IF EXISTS test::nope");
	assert_eq!(frames[0].rows().next().unwrap().get::<bool>("dropped").unwrap().unwrap(), false);

	let err = t.admin_err("DROP QUEUE test::nope");
	assert!(err.contains("CA_096"), "missing queue should report queue_not_found, got: {err}");
}

#[test]
fn test_drop_namespace_cascades_to_queues() {
	// A surviving definition is unreachable by name yet still occupies its id and columns.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE QUEUE test::other { id: int4 } WITH { fifo: {} }");

	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 2);

	t.admin("DROP NAMESPACE test");

	assert_eq!(TestEngine::row_count(&t.query("FROM system::queues")), 0);
}

#[test]
fn test_queue_resolves_as_a_queue_not_another_primitive() {
	// The queue probe sits last in the resolver chain, so a wrong hit would scan another
	// object's rows under the queue's name and report them as items.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::decoy { other: utf8 }");
	t.admin("CREATE QUEUE test::jobs { id: int4, payload: utf8 } WITH { fifo: {} }");

	let frames = t.query("FROM test::jobs");
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();

	assert_eq!(names, vec!["id", "payload"], "FROM must expose the queue's own declared columns");
	assert_eq!(frames[0].rows().count(), 0, "a queue with no items scans empty, it does not error");
}

#[test]
fn test_retention_only_queue_keeps_its_duration() {
	// Covers the codec path for an optional Duration that only the WITH block supplies.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, retention: { done: "1h" } }"#);

	let queue = find_queue(&t, "test", "jobs").unwrap();

	assert_eq!(queue.retention.done, Some(Duration::from_hours_const(1)));
	assert_eq!(queue.retry.attempts, Queue::DEFAULT_RETRY_ATTEMPTS);
	assert_eq!(queue.retry.backoff, Queue::DEFAULT_RETRY_BACKOFF);
}

#[test]
fn test_create_queue_with_deduplicate_round_trips() {
	// A lost column list silently widens what counts as a duplicate; a lost ttl silently
	// shortens how long the guarantee holds.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		r#"CREATE QUEUE test::jobs { order_id: int4, kind: utf8, payload: utf8 } WITH { fifo: {}, deduplicate: { by: {order_id, kind}, ttl: "30d" } }"#,
	);

	let queue = find_queue(&t, "test", "jobs").unwrap();
	let deduplicate = queue.deduplicate.expect("the declaration must persist");

	assert_eq!(deduplicate.by, vec!["order_id".to_string(), "kind".to_string()]);
	assert_eq!(deduplicate.ttl, Duration::from_days(30).unwrap());
	assert!(!deduplicate.is_forever());
}

#[test]
fn test_deduplicate_without_ttl_defaults_to_forever() {
	// Any finite default puts an expiry date on a guarantee the user never bounded.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {id} } }");

	let deduplicate = find_queue(&t, "test", "jobs").unwrap().deduplicate.unwrap();

	assert!(deduplicate.is_forever(), "an unbounded declaration must stay unbounded");
	assert_eq!(deduplicate.ttl, Duration::MAX);
}

#[test]
fn test_deduplicate_ttl_forever_is_accepted_explicitly() {
	// The documented way to ask for a permanent guarantee must not differ from the default one.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {id}, ttl: forever } }");

	assert!(find_queue(&t, "test", "jobs").unwrap().deduplicate.unwrap().is_forever());
}

#[test]
fn test_a_queue_without_deduplicate_declares_none() {
	// An implicit rule would start dropping items a producer expects to be distinct.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	assert!(find_queue(&t, "test", "jobs").unwrap().deduplicate.is_none());
}

#[test]
fn test_deduplicate_rejects_an_unknown_column() {
	// A typo must fault at CREATE, where the author can still see it, not at the first enqueue.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {nope} } }");
	assert!(err.contains("nope"), "the error must name the unknown column, got: {err}");

	assert!(find_queue(&t, "test", "jobs").is_none(), "the rejected queue must not exist");
}

#[test]
fn test_deduplicate_rejects_an_empty_column_list() {
	// Deduplicating on nothing collapses the whole queue onto a single key.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {} } }");
	assert!(!err.is_empty(), "an empty by list must fault");
	assert!(find_queue(&t, "test", "jobs").is_none());
}

#[test]
fn test_deduplicate_requires_by() {
	// A block carrying only a ttl is incomplete, not a queue that deduplicates on everything.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err(r#"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { ttl: "1d" } }"#);
	assert!(!err.is_empty(), "deduplicate without by must fault");
	assert!(find_queue(&t, "test", "jobs").is_none());
}

#[test]
fn test_deduplicate_rejects_a_repeated_column() {
	// A repeat contributes nothing and signals the author meant two different fields.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {id, id} } }");
	assert!(!err.is_empty(), "a repeated by column must fault");
}

#[test]
fn test_deduplicate_rejects_a_bare_identifier_ttl() {
	// A bare identifier is a misspelling of a duration literal or `forever`, not another unit.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.admin_err(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {}, deduplicate: { by: {id}, ttl: never } }",
	);
	assert!(!err.is_empty(), "an unrecognised ttl word must fault");
}

#[test]
fn test_system_queues_exposes_the_deduplicate_declaration() {
	// An operator must see the declaration without reading the DDL back, and `forever`
	// must read as `forever` rather than as a max-duration number.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		r#"CREATE QUEUE test::bounded { a: int4, b: int4 } WITH { fifo: {}, deduplicate: { by: {a, b}, ttl: "1d" } }"#,
	);
	t.admin("CREATE QUEUE test::unbounded { a: int4 } WITH { fifo: {}, deduplicate: { by: {a} } }");
	t.admin("CREATE QUEUE test::plain { a: int4 } WITH { fifo: {} }");

	let frames = t.query("FROM system::queues");
	let rows: Vec<_> = frames[0].rows().collect();

	let by = |name: &str| -> Option<String> {
		rows.iter()
			.find(|r| r.get::<String>("name").unwrap().unwrap() == name)
			.and_then(|r| r.get::<String>("deduplicate_by").unwrap())
	};
	let ttl = |name: &str| -> Option<String> {
		rows.iter()
			.find(|r| r.get::<String>("name").unwrap().unwrap() == name)
			.and_then(|r| r.get::<String>("deduplicate_ttl").unwrap())
	};

	assert_eq!(by("bounded"), Some("a,b".to_string()));
	assert_eq!(ttl("unbounded"), Some("forever".to_string()), "forever must not render as a number");
	assert_eq!(by("plain"), None, "a queue without the declaration reports none");
}
