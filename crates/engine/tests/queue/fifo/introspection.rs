// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, datetime::DateTime, frame::frame::Frame, value_type::ValueType};

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn column(frames: &[Frame], name: &str) -> Vec<Value> {
	let frame = frames.first().expect("a query must always return a frame");
	let column = frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| {
		panic!(
			"result has no column {name}; got {:?}",
			frame.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
		)
	});
	(0..frame.row_count()).map(|i| column.data.get_value(i)).collect()
}

fn uint8s(frames: &[Frame], name: &str) -> Vec<u64> {
	column(frames, name)
		.into_iter()
		.map(|v| match v {
			Value::Uint8(n) => n,
			other => panic!("column {name} must be Uint8, got {other:?}"),
		})
		.collect()
}

fn only_uint8(frames: &[Frame], name: &str) -> u64 {
	let values = uint8s(frames, name);
	assert_eq!(values.len(), 1, "expected exactly one row");
	values[0]
}

fn queue_row(t: &TestEngine, name: &str) -> Vec<Frame> {
	t.query(&format!(r#"FROM system::queues filter {{ name == "{name}" }}"#))
}

fn partition_rows(t: &TestEngine, queue_id: u64) -> Vec<Frame> {
	t.query(&format!("FROM system::queue_partitions filter {{ queue_id == {queue_id} }}"))
}

fn queue_id(t: &TestEngine, name: &str) -> u64 {
	only_uint8(&queue_row(t, name), "id")
}

fn claim(t: &TestEngine, worker: &str, max_n: u32) -> Vec<Frame> {
	t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds(30))"#))
}

fn tokens(frames: &[Frame]) -> Vec<String> {
	column(frames, "token")
		.into_iter()
		.map(|v| match v {
			Value::Utf8(t) => t,
			other => panic!("token must be Utf8, got {other:?}"),
		})
		.collect()
}

#[test]
fn test_depth_and_in_flight_follow_every_transition() {
	// depth and in_flight are not derived, they are incrementally maintained by three separate
	// writers: the enqueue interceptor, queue::claim, and the ack interceptor. A transition that
	// forgets its delta corrupts them silently forever, because nothing recomputes them outside
	// startup hydration. Walking insert -> claim -> ack is the only way to catch a writer that
	// moves an item without moving the counter with it.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let row = queue_row(&t, "jobs");
	assert_eq!(only_uint8(&row, "depth"), 3, "every enqueued item must be counted as waiting");
	assert_eq!(only_uint8(&row, "in_flight"), 0, "nothing is claimed yet");

	let claimed = claim(&t, "w1", 2);
	let tokens = tokens(&claimed);
	assert_eq!(tokens.len(), 2);

	let row = queue_row(&t, "jobs");
	assert_eq!(only_uint8(&row, "depth"), 1, "a claim must move items out of the waiting count");
	assert_eq!(only_uint8(&row, "in_flight"), 2, "a claim must move them into the in-flight count");

	t.command(&format!(r#"CALL queue::ack("{}")"#, tokens[0]));

	let row = queue_row(&t, "jobs");
	assert_eq!(only_uint8(&row, "depth"), 1, "an ack must not touch the waiting count");
	assert_eq!(only_uint8(&row, "in_flight"), 1, "a completed item must leave the in-flight count");
}

#[test]
fn test_blocked_keys_counts_parked_siblings_only_on_ordered_queues() {
	// blocked_keys is the operator's only view of head-of-line blocking: a queue with depth 100
	// and blocked_keys 99 is one slow key, not a throughput problem. An unkeyed queue can never
	// park anything, so a nonzero count there means the parked-sibling accounting leaked into a
	// path where per-key exclusivity does not apply.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant } }",
	);
	t.command(
		r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }, { id: 3, tenant: "a" }, { id: 4, tenant: "b" }]"#,
	);

	let row = queue_row(&t, "jobs");
	assert_eq!(only_uint8(&row, "depth"), 4, "parked siblings are still waiting work");
	assert_eq!(only_uint8(&row, "blocked_keys"), 1, "tenant a is blocked, tenant b is not");

	let t = engine_with_queue("CREATE QUEUE test::plain { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::plain [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let row = queue_row(&t, "plain");
	assert_eq!(only_uint8(&row, "depth"), 3);
	assert_eq!(only_uint8(&row, "blocked_keys"), 0, "an unkeyed queue can never block a key");
}

#[test]
fn test_partition_rows_sum_to_the_queue_row_and_cover_every_partition() {
	// The queue row is an aggregate over the partition rows, and a partition that never took work
	// has no counter record at all. If an absent record were skipped instead of read as zero, the
	// partition list would be ragged and an operator could not tell an idle partition from one
	// that does not exist. This also catches an aggregation that double-counts or drops a
	// partition.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 8 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]");
	claim(&t, "w1", 2);

	let id = queue_id(&t, "jobs");
	let rows = partition_rows(&t, id);

	let partitions: BTreeSet<u64> = column(&rows, "partition")
		.into_iter()
		.map(|v| match v {
			Value::Uint2(n) => u64::from(n),
			other => panic!("partition must be Uint2, got {other:?}"),
		})
		.collect();
	assert_eq!(partitions, (0..8).collect::<BTreeSet<_>>(), "every declared partition must appear exactly once");

	let queue = queue_row(&t, "jobs");
	assert_eq!(uint8s(&rows, "depth").iter().sum::<u64>(), only_uint8(&queue, "depth"));
	assert_eq!(uint8s(&rows, "in_flight").iter().sum::<u64>(), only_uint8(&queue, "in_flight"));
	assert_eq!(uint8s(&rows, "blocked_keys").iter().sum::<u64>(), only_uint8(&queue, "blocked_keys"));
	assert_eq!(only_uint8(&queue, "in_flight"), 2, "the claim must be visible in the aggregate");
}

#[test]
fn test_oldest_due_at_reports_the_earliest_instant_not_the_latest() {
	// The due index is stored bitwise-inverted, so a forward scan returns the LATEST entry first.
	// A peek that forgets to scan in reverse reports the furthest-future item as "oldest", which
	// makes the column read exactly backwards and would tell an operator a starving queue is
	// healthy. Claiming the currently-due item must then advance the column to the next instant.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.mock_clock().set_millis(1_000);
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(50000) }"#);
	t.command(r#"INSERT test::jobs [{ id: 2 }] WITH { not_before: datetime::from_epoch_millis(20000) }"#);
	t.command(r#"INSERT test::jobs [{ id: 3 }] WITH { not_before: datetime::from_epoch_millis(90000) }"#);

	let expected_nanos = |millis: u64| Value::DateTime(DateTime::from_nanos(millis * 1_000_000));

	assert_eq!(
		column(&queue_row(&t, "jobs"), "oldest_due_at")[0],
		expected_nanos(20_000),
		"oldest_due_at must be the minimum due instant, not the maximum"
	);

	t.mock_clock().set_millis(20_000);
	assert_eq!(TestEngine::row_count(&claim(&t, "w1", 10)), 1, "only the 20s item is due");

	assert_eq!(
		column(&queue_row(&t, "jobs"), "oldest_due_at")[0],
		expected_nanos(50_000),
		"claiming the head must advance oldest_due_at to the next pending instant"
	);
}

#[test]
fn test_an_empty_queue_reports_zero_counters_and_no_oldest_due_at() {
	// An untouched partition has no counter record and no due entry. The counters must still read
	// as 0 rather than none, because operators filter and sum on them; only oldest_due_at is
	// genuinely absent when there is nothing to be due. Getting this backwards would make
	// `filter { depth > 0 }` silently drop every idle queue from an operator's dashboards.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 4 } }");

	let row = queue_row(&t, "jobs");
	assert_eq!(only_uint8(&row, "depth"), 0);
	assert_eq!(only_uint8(&row, "in_flight"), 0);
	assert_eq!(only_uint8(&row, "blocked_keys"), 0);
	assert_eq!(
		column(&row, "oldest_due_at")[0],
		Value::none_of(ValueType::DateTime),
		"an empty queue has no oldest due instant"
	);

	let rows = partition_rows(&t, queue_id(&t, "jobs"));
	assert_eq!(uint8s(&rows, "depth").len(), 4, "an untouched partition still gets a row");
	assert!(uint8s(&rows, "depth").iter().all(|d| *d == 0));
}

#[test]
fn test_a_dropped_queue_leaves_no_partition_rows_behind() {
	// Queue ids are reused after a drop. A partition row that outlived its queue would be
	// re-attributed to whatever queue takes the id next and report phantom depth.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 2 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let id = queue_id(&t, "jobs");
	assert_eq!(uint8s(&partition_rows(&t, id), "depth").len(), 2);

	t.admin("DROP QUEUE test::jobs");

	assert!(uint8s(&partition_rows(&t, id), "depth").is_empty(), "partition rows must not outlive the queue");
}
