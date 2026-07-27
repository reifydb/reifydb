// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, Mutex};

use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::{
	change::RowChange,
	interceptor::{interceptors::Interceptors, transaction::post_commit},
};
use reifydb_value::value::Value;

/// Captures every `RowChange` the committed transactions produced, in commit order.
/// The scheduling lane of the next step is built entirely from these records, so
/// tests assert on them directly rather than on any observable side effect.
#[derive(Clone, Default)]
struct RecordedChanges(Arc<Mutex<Vec<RowChange>>>);

impl RecordedChanges {
	fn install(&self, t: &TestEngine) {
		let sink = self.0.clone();
		t.add_interceptor_factory(Arc::new(move |interceptors: &mut Interceptors| {
			let sink = sink.clone();
			interceptors.post_commit.add(Arc::new(post_commit(move |ctx| {
				sink.lock().unwrap().extend(ctx.row_changes.iter().cloned());
				Ok(())
			})));
		}));
	}

	fn queue_inserts(&self) -> Vec<reifydb_transaction::change::QueueRowInsertion> {
		self.0.lock()
			.unwrap()
			.iter()
			.filter_map(|change| match change {
				RowChange::QueueInsert(insertion) => Some(insertion.clone()),
				_ => None,
			})
			.collect()
	}
}

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

/// Items must be real, queryable rows in enqueue order. The row shape carries a
/// trailing system field for not_before that FROM must never surface: if it
/// leaked, every consumer would see a phantom column it never declared.
#[test]
fn test_insert_then_scan_roundtrip() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ id: 1, payload: "a" }, { id: 2, payload: "b" }, { id: 3, payload: "c" }]"#);

	let frames = t.query("FROM test::jobs");
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["id", "payload"], "the trailing not_before field must not surface");

	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[1].get::<i32>("id").unwrap().unwrap(), 2);
	assert_eq!(rows[2].get::<i32>("id").unwrap().unwrap(), 3);
	assert_eq!(rows[0].get::<String>("payload").unwrap().unwrap(), "a");
	assert_eq!(rows[2].get::<String>("payload").unwrap().unwrap(), "c");
}

/// The statement result is the only enqueue signal a caller gets without
/// re-querying. `duplicates` is reported from this step on so that the column
/// set does not change under callers once deduplication lands.
#[test]
fn test_insert_result_reports_inserted_and_duplicates() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<String>("namespace").unwrap().unwrap(), "test");
	assert_eq!(row.get::<String>("queue").unwrap().unwrap(), "jobs");
	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

/// Every enqueued item must leave exactly one record: a missing one silently
/// drops work from the scheduler, a duplicated one schedules the same item
/// twice. The partition must land inside the declared bucket count, because the
/// consumer side indexes by it.
#[test]
fn test_row_changes_carry_one_queue_insert_per_item() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 8 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");

	let inserts = recorded.queue_inserts();
	assert_eq!(inserts.len(), 2, "two items must produce exactly two records");

	let row_numbers: Vec<u64> = inserts.iter().map(|i| i.row_number.0).collect();
	assert_eq!(row_numbers[1], row_numbers[0] + 1, "row numbers must be consecutive in enqueue order");

	for insert in &inserts {
		assert!(insert.partition < 8, "partition {} must fall inside the declared 8 buckets", insert.partition);
		assert_eq!(insert.not_before, None, "an insert without WITH is immediately due");
		assert!(!insert.encoded.is_empty(), "the record must carry the encoded item row");
	}
}

/// Partition affinity is the contract per-key FIFO ordering will rely on: the
/// same ordered_by value must always land in the same bucket, across separate
/// statements, or two items of one key could be consumed out of order.
#[test]
fn test_partition_assignment_is_deterministic_per_ordered_by_value() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin(
		r#"CREATE QUEUE test::jobs { tenant: utf8, id: int4 } WITH { fifo: { partitions: 16, ordered_by: tenant } }"#,
	);

	t.command(r#"INSERT test::jobs [{ tenant: "acme", id: 1 }]"#);
	t.command(r#"INSERT test::jobs [{ tenant: "acme", id: 2 }]"#);

	let inserts = recorded.queue_inserts();
	assert_eq!(inserts.len(), 2);
	assert_eq!(
		inserts[0].partition, inserts[1].partition,
		"the same ordered_by value must map to the same partition across statements"
	);
}

/// Without affinity every item would land in one bucket and the queue could not
/// be consumed in parallel. Many distinct keys must spread across buckets.
#[test]
fn test_distinct_ordered_by_values_reach_more_than_one_partition() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin(r#"CREATE QUEUE test::jobs { tenant: utf8 } WITH { fifo: { partitions: 16, ordered_by: tenant } }"#);

	let rows: Vec<String> = (0..32).map(|i| format!(r#"{{ tenant: "tenant-{i}" }}"#)).collect();
	t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));

	let partitions: Vec<u16> = recorded.queue_inserts().iter().map(|i| i.partition).collect();
	assert_eq!(partitions.len(), 32);

	let mut distinct = partitions.clone();
	distinct.sort_unstable();
	distinct.dedup();
	assert!(distinct.len() > 1, "32 distinct keys collapsed into one partition: {partitions:?}");
}

/// A queue without ordered_by has no key to hash, so it falls back to the row
/// number. Items must still spread rather than pile into bucket 0, which is
/// what a forgotten fallback would produce.
#[test]
fn test_a_queue_without_ordered_by_still_spreads_across_partitions() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 16 } }");

	let rows: Vec<String> = (0..32).map(|i| format!("{{ id: {i} }}")).collect();
	t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));

	let mut partitions: Vec<u16> = recorded.queue_inserts().iter().map(|i| i.partition).collect();
	assert_eq!(partitions.len(), 32);
	partitions.sort_unstable();
	partitions.dedup();
	assert!(partitions.len() > 1, "row-number fallback collapsed every item into one partition");
}

/// A single-partition queue is legal and must not divide by zero or overflow the
/// u16 bucket: every item belongs to bucket 0.
#[test]
fn test_a_single_partition_queue_places_every_item_in_bucket_zero() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let partitions: Vec<u16> = recorded.queue_inserts().iter().map(|i| i.partition).collect();
	assert_eq!(partitions, vec![0, 0, 0]);
}

/// Column values must go through the same coercion and constraint path as a
/// table insert. A value that violates the declared type has to fault at enqueue
/// rather than reach a consumer as a malformed item.
#[test]
fn test_insert_rejects_a_value_the_column_constraint_forbids() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: "not-a-number" }]"#);
	assert!(!err.is_empty(), "a type violation must fault rather than enqueue a malformed item");

	let frames = t.query("FROM test::jobs");
	assert_eq!(frames[0].rows().count(), 0, "the rejected statement must not have enqueued anything");
}

/// An omitted column is none, not a zero value: a consumer distinguishes "no
/// payload supplied" from "empty payload", and the encoder must preserve that.
#[test]
fn test_an_omitted_column_enqueues_as_none() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");

	let frames = t.query("FROM test::jobs");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(row.get::<String>("payload").unwrap(), None);
}

/// RETURNING must project the item's declared columns. The trailing not_before
/// field sits in the same encoded row, so a projection that forgot to drop it
/// would expose it here first.
#[test]
fn test_returning_projects_the_declared_columns_only() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 7, payload: "x" }] RETURNING { id, payload }"#);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["id", "payload"]);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap().unwrap(), 7);
	assert_eq!(row.get::<String>("payload").unwrap().unwrap(), "x");
}

/// Enqueue order is the queue's only intrinsic ordering, and it must survive
/// separate statements rather than restarting each time.
#[test]
fn test_row_numbers_continue_across_statements() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");
	t.command("INSERT test::jobs [{ id: 2 }]");
	t.command("INSERT test::jobs [{ id: 3 }]");

	let frames = t.query("FROM test::jobs");
	let ids: Vec<i32> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	assert_eq!(ids, vec![1, 2, 3], "later statements must enqueue after earlier ones");
}

/// Two queues are independent lanes. Sharing a row-number generator or a key
/// prefix would make one queue's scan return the other's items.
#[test]
fn test_two_queues_do_not_share_items() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::a { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE QUEUE test::b { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::a [{ id: 1 }, { id: 2 }]");
	t.command("INSERT test::b [{ id: 9 }]");

	let a: Vec<i32> = t.query("FROM test::a")[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	let b: Vec<i32> = t.query("FROM test::b")[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();

	assert_eq!(a, vec![1, 2]);
	assert_eq!(b, vec![9]);
}

/// A queue insert must not be mistaken for a table insert downstream: the
/// scheduling lane consumes only QueueInsert records, and a TableInsert emitted
/// for a queue would be scheduled by nothing and replicated as the wrong kind.
#[test]
fn test_a_queue_insert_does_not_emit_a_table_row_change() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let table_inserts =
		recorded.0.lock().unwrap().iter().filter(|change| matches!(change, RowChange::TableInsert(_))).count();
	assert_eq!(table_inserts, 0, "a queue insert must not report itself as a table insert");
}

/// Inserting into a queue that does not exist must name the queue rather than
/// falling through to a table-not-found path, which would send a caller looking
/// for the wrong object.
#[test]
fn test_insert_into_a_missing_queue_reports_the_queue() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.command_err("INSERT test::missing [{ id: 1 }]");
	assert!(err.contains("missing"), "the error must name the unresolved target, got: {err}");
}

/// An insert whose input produces no rows is a no-op, not a fault, and must not
/// burn row numbers or emit records for items that do not exist.
#[test]
fn test_an_empty_insert_enqueues_nothing() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE TABLE test::empty { id: int4 }");

	let frames = t.command("INSERT test::jobs FROM test::empty");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 0);

	assert!(recorded.queue_inserts().is_empty(), "an empty insert must emit no records");
	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 0);
}

/// The enqueued values must survive the shape round trip exactly. A trailing
/// hidden field makes off-by-one field indexing easy, and it would show up as a
/// value landing in the neighbouring column.
#[test]
fn test_every_declared_column_round_trips_through_the_shape() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { a: int4, b: utf8, c: bool } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ a: 42, b: "text", c: true }]"#);

	let frames = t.query("FROM test::jobs");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("a").unwrap().unwrap(), 42);
	assert_eq!(row.get::<String>("b").unwrap().unwrap(), "text");
	assert_eq!(row.get::<bool>("c").unwrap().unwrap(), true);
}

/// The declared partition count bounds the bucket, not the hash. A queue at the
/// documented maximum must still produce an in-range u16 rather than wrapping.
#[test]
fn test_the_maximum_partition_count_still_yields_an_in_range_bucket() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1024 } }");

	let rows: Vec<String> = (0..64).map(|i| format!("{{ id: {i} }}")).collect();
	t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));

	for insert in recorded.queue_inserts() {
		assert!(insert.partition < 1024, "partition {} escaped the declared range", insert.partition);
	}
}

/// Values inserted through a pipeline rather than an inline list take the same
/// encode path. If only the inline path were wired, this would enqueue nothing.
#[test]
fn test_insert_from_a_query_source_enqueues_its_rows() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4 }");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::src [{ id: 5 }, { id: 6 }]");
	let frames = t.command("INSERT test::jobs FROM test::src");
	assert_eq!(frames[0].rows().next().unwrap().get::<u64>("inserted").unwrap().unwrap(), 2);

	let mut ids: Vec<i32> =
		t.query("FROM test::jobs")[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort_unstable();
	assert_eq!(ids, vec![5, 6], "every source row must reach the queue");
}

/// The item lane is MVCC: an aborted statement must leave no item and no record
/// behind, or a rolled-back enqueue would still be scheduled.
#[test]
fn test_a_failed_statement_leaves_no_partial_items() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: 1 }, { id: "bad" }]"#);
	assert!(!err.is_empty());

	assert_eq!(
		t.query("FROM test::jobs")[0].rows().count(),
		0,
		"the valid row of a failed statement must not survive"
	);
}

/// Enqueue must be usable from a plain command transaction, not only from admin:
/// producers are ordinary clients, and requiring admin would make the primitive
/// unusable for its purpose.
#[test]
fn test_insert_does_not_require_admin() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

/// The result columns are a contract for callers scripting enqueues. Pinning the
/// exact set catches a silent rename or reordering.
#[test]
fn test_the_insert_result_column_set_is_pinned() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command("INSERT test::jobs [{ id: 1 }]");
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["namespace", "queue", "inserted", "duplicates"]);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<Value>("inserted").unwrap().unwrap(), Value::Uint8(1));
}

/// An deduplication key must make a repeated enqueue a no-op. Without it, a
/// producer that retries after an ambiguous failure enqueues the work twice,
/// which is the single failure mode the key exists to prevent.
#[test]
fn test_deduplication_duplicate_is_a_noop() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let first = t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: "job-1" }"#);
	assert_eq!(first[0].rows().next().unwrap().get::<u64>("inserted").unwrap().unwrap(), 1);

	let second = t.command(r#"INSERT test::jobs [{ id: 2 }] WITH { deduplication_key: "job-1" }"#);
	let row = second[0].rows().next().unwrap();
	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 0, "the repeat must enqueue nothing");
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 1, "the repeat must be counted");

	let ids: Vec<i32> =
		t.query("FROM test::jobs")[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	assert_eq!(ids, vec![1], "only the first item may exist");
}

/// Dedup must not depend on commit boundaries: two rows of one statement that
/// share a key are still one item, or a batch retry would smuggle duplicates in.
#[test]
fn test_deduplication_dedups_inside_a_single_statement() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 1 }, { id: 2 }] WITH { deduplication_key: "same" }"#);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 1);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 1);
	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

/// The key is evaluated per row with the row's own columns in scope, so distinct
/// rows produce distinct keys. If it were evaluated once per statement, a batch
/// of genuinely different items would collapse into one.
#[test]
fn test_the_deduplication_key_is_evaluated_per_row() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, tag: utf8 } WITH { fifo: {} }");

	let frames = t.command(
		r#"INSERT test::jobs [{ id: 1, tag: "a" }, { id: 2, tag: "b" }] WITH { deduplication_key: tag }"#,
	);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2, "distinct keys must both enqueue");
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

/// A duplicate must hand back the item that already exists, not the one that was
/// rejected: a retrying producer uses RETURNING to learn the identity of the work
/// it enqueued the first time.
#[test]
fn test_returning_on_a_duplicate_yields_the_existing_item() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: "job-1" }"#);

	let frames =
		t.command(r#"INSERT test::jobs [{ id: 99 }] WITH { deduplication_key: "job-1" } RETURNING { id }"#);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(
		row.get::<i32>("id").unwrap().unwrap(),
		1,
		"RETURNING must describe the surviving item, not the rejected one"
	);
}

/// Keys are scoped per queue. If the record were global, enqueueing a key on one
/// queue would silently suppress the same key on every other queue.
#[test]
fn test_the_same_deduplication_key_is_independent_per_queue() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::a { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE QUEUE test::b { id: int4 } WITH { fifo: {} }");

	t.command(r#"INSERT test::a [{ id: 1 }] WITH { deduplication_key: "shared" }"#);
	let frames = t.command(r#"INSERT test::b [{ id: 1 }] WITH { deduplication_key: "shared" }"#);

	assert_eq!(frames[0].rows().next().unwrap().get::<u64>("inserted").unwrap().unwrap(), 1);
	assert_eq!(t.query("FROM test::b")[0].rows().count(), 1);
}

/// A none key means "do not deduplicate this item". Treating none as a key would
/// make every un-keyed item after the first a duplicate.
#[test]
fn test_a_none_deduplication_key_does_not_deduplicate() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, tag: Option(utf8) } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 1 }, { id: 2 }] WITH { deduplication_key: tag }"#);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

/// not_before must be durable inside the item row, not held in memory: the
/// scheduling lane is rebuilt from stored rows after a crash, and a delay that
/// lived only in the interceptor would come back as immediately due.
#[test]
fn test_not_before_travels_on_the_row_change() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(1700000000000) }"#);

	let inserts = recorded.queue_inserts();
	assert_eq!(inserts.len(), 1);
	assert!(inserts[0].not_before.is_some(), "a delayed item must carry its due instant");
}

/// A delayed item is still a queryable item. If not_before gated the scan, an
/// operator could not inspect scheduled work before it becomes due.
#[test]
fn test_a_delayed_item_is_still_visible_to_a_scan() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(1800000000000) }"#);

	let frames = t.query("FROM test::jobs");
	assert_eq!(frames[0].rows().count(), 1);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["id"], "the hidden not_before field must still not surface");
}

/// Both options together must not interfere: the desugar appends two hidden
/// columns, and an index mistake would swap the key for the instant.
#[test]
fn test_both_with_options_can_be_used_together() {
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command(
		r#"INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: "k", not_before: datetime::from_epoch_millis(1700000000000) }"#,
	);

	let inserts = recorded.queue_inserts();
	assert_eq!(inserts.len(), 1);
	assert!(inserts[0].not_before.is_some());

	let repeat = t.command(r#"INSERT test::jobs [{ id: 2 }] WITH { deduplication_key: "k" }"#);
	assert_eq!(repeat[0].rows().next().unwrap().get::<u64>("duplicates").unwrap().unwrap(), 1);
}

/// WITH is queue-only. On a table it must fault rather than being ignored, or a
/// producer would believe its items were deduplicated when they were not.
#[test]
fn test_with_on_a_table_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");

	let err = t.command_err(r#"INSERT test::t [{ id: 1 }] WITH { deduplication_key: "k" }"#);
	assert!(err.contains("INSERT_005"), "expected the queue-only diagnostic, got: {err}");
}

/// An unknown option is a typo, and silently dropping it would silently drop the
/// guarantee the caller asked for.
#[test]
fn test_an_unknown_with_option_is_rejected() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: 1 }] WITH { idempotncy_key: "k" }"#);
	assert!(err.contains("INSERT_006"), "expected the unknown-option diagnostic, got: {err}");
}

/// A repeated option is ambiguous. Taking the last one silently would apply a
/// guarantee the caller did not intend.
#[test]
fn test_a_duplicate_with_option_is_rejected() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(
		r#"INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: "a", deduplication_key: "b" }"#,
	);
	assert!(err.contains("INSERT_007"), "expected the duplicate-option diagnostic, got: {err}");
}

/// The typed temporal contract: not_before is an instant. Accepting an integer
/// would reintroduce exactly the unit ambiguity typed temporals exist to prevent.
#[test]
fn test_a_non_datetime_not_before_is_rejected() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err("INSERT test::jobs [{ id: 1 }] WITH { not_before: 12345 }");
	assert!(err.contains("CA_019"), "expected the not_before type diagnostic, got: {err}");
}

/// The key is exact-match text. A non-text key would either hash differently
/// across runs or compare unequal to the same logical key written elsewhere.
#[test]
fn test_a_non_utf8_deduplication_key_is_rejected() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err("INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: 42 }");
	assert!(err.contains("CA_018"), "expected the deduplication_key type diagnostic, got: {err}");
}

/// A queue that declares a column named like a reserved hidden field would
/// produce two columns of the same name after the desugar, silently shadowing
/// one. It must fault at plan time instead.
#[test]
fn test_a_column_colliding_with_a_reserved_field_is_rejected() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { __queue_not_before: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ __queue_not_before: 1 }] WITH { deduplication_key: "k" }"#);
	assert!(err.contains("CA_017"), "expected the reserved-column diagnostic, got: {err}");
}

/// A queue without WITH must not pay for the desugar: the reserved-name check
/// only applies when the hidden columns are actually appended, so a queue with
/// such a column is still usable for plain inserts.
#[test]
fn test_a_reserved_column_name_is_allowed_without_with() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { __queue_not_before: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ __queue_not_before: 7 }]");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

/// Items are immutable: they leave a queue by acknowledgement or retention, never
/// by being edited. Falling through to a table-not-found error would send a
/// caller looking for an object that does exist.
#[test]
fn test_update_on_a_queue_is_rejected_as_immutable() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err("UPDATE test::jobs { id: 2 } FILTER { id == 1 }");
	assert!(err.contains("QUEUE_001"), "expected the immutability diagnostic, got: {err}");
}

/// The same contract for DELETE: retention owns removal, not the caller.
#[test]
fn test_delete_on_a_queue_is_rejected_as_immutable() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err("DELETE test::jobs FILTER { id == 1 }");
	assert!(err.contains("QUEUE_001"), "expected the immutability diagnostic, got: {err}");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1, "the item must survive the rejected DELETE");
}
