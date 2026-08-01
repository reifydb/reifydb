// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, Mutex};

use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::{
	change::RowChange,
	interceptor::{interceptors::Interceptors, transaction::post_commit},
};
use reifydb_value::value::Value;

/// Every `RowChange` the committed transactions produced, in commit order.
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

#[test]
fn test_insert_then_scan_roundtrip() {
	// A leaked not_before field would show every consumer a column it never declared.
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

#[test]
fn test_insert_result_reports_inserted_and_duplicates() {
	// The statement result is the only enqueue signal a caller gets without re-querying.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<String>("namespace").unwrap().unwrap(), "test");
	assert_eq!(row.get::<String>("queue").unwrap().unwrap(), "jobs");
	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

#[test]
fn test_row_changes_carry_one_queue_insert_per_item() {
	// A missing record drops work from the scheduler; a duplicated one schedules it twice.
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

#[test]
fn test_partition_assignment_is_deterministic_per_ordered_by_value() {
	// Two items of one key in different buckets could be consumed out of order.
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

#[test]
fn test_distinct_ordered_by_values_reach_more_than_one_partition() {
	// Every key in one bucket leaves the queue unconsumable in parallel.
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

#[test]
fn test_a_queue_without_ordered_by_still_spreads_across_partitions() {
	// With no key to hash the row number is the fallback; forgetting it piles everything into bucket 0.
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

#[test]
fn test_a_single_partition_queue_places_every_item_in_bucket_zero() {
	// A single partition is legal and must not divide by zero or overflow the u16 bucket.
	let t = TestEngine::new();
	let recorded = RecordedChanges::default();
	recorded.install(&t);

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let partitions: Vec<u16> = recorded.queue_inserts().iter().map(|i| i.partition).collect();
	assert_eq!(partitions, vec![0, 0, 0]);
}

#[test]
fn test_insert_rejects_a_value_the_column_constraint_forbids() {
	// A type violation must fault at enqueue rather than reach a consumer as a malformed item.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: "not-a-number" }]"#);
	assert!(!err.is_empty(), "a type violation must fault rather than enqueue a malformed item");

	let frames = t.query("FROM test::jobs");
	assert_eq!(frames[0].rows().count(), 0, "the rejected statement must not have enqueued anything");
}

#[test]
fn test_an_omitted_column_enqueues_as_none() {
	// A consumer distinguishes "no payload supplied" from "empty payload".
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");

	let frames = t.query("FROM test::jobs");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(row.get::<String>("payload").unwrap(), None);
}

#[test]
fn test_returning_projects_the_declared_columns_only() {
	// The hidden not_before field shares the encoded row, so a projection that keeps it shows up here.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 7, payload: "x" }] RETURNING { id, payload }"#);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["id", "payload"]);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap().unwrap(), 7);
	assert_eq!(row.get::<String>("payload").unwrap().unwrap(), "x");
}

#[test]
fn test_row_numbers_continue_across_statements() {
	// Enqueue order is the queue's only intrinsic ordering; it must not restart per statement.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");
	t.command("INSERT test::jobs [{ id: 2 }]");
	t.command("INSERT test::jobs [{ id: 3 }]");

	let frames = t.query("FROM test::jobs");
	let ids: Vec<i32> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	assert_eq!(ids, vec![1, 2, 3], "later statements must enqueue after earlier ones");
}

#[test]
fn test_two_queues_do_not_share_items() {
	// A shared row-number generator or key prefix would make one queue's scan return the other's items.
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

#[test]
fn test_a_queue_insert_does_not_emit_a_table_row_change() {
	// The scheduling lane consumes only QueueInsert; a TableInsert is scheduled by nothing
	// and replicated as the wrong kind.
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

#[test]
fn test_insert_into_a_missing_queue_reports_the_queue() {
	// A table-not-found path would send the caller looking for the wrong object.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let err = t.command_err("INSERT test::missing [{ id: 1 }]");
	assert!(err.contains("missing"), "the error must name the unresolved target, got: {err}");
}

#[test]
fn test_an_empty_insert_enqueues_nothing() {
	// An empty input is a no-op, not a fault, and must not burn row numbers.
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

#[test]
fn test_every_declared_column_round_trips_through_the_shape() {
	// A trailing hidden field makes off-by-one field indexing easy; it lands a value
	// in the neighbouring column.
	let t = engine_with_queue("CREATE QUEUE test::jobs { a: int4, b: utf8, c: bool } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ a: 42, b: "text", c: true }]"#);

	let frames = t.query("FROM test::jobs");
	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("a").unwrap().unwrap(), 42);
	assert_eq!(row.get::<String>("b").unwrap().unwrap(), "text");
	assert_eq!(row.get::<bool>("c").unwrap().unwrap(), true);
}

#[test]
fn test_the_maximum_partition_count_still_yields_an_in_range_bucket() {
	// The declared partition count bounds the bucket, not the hash; the maximum must not wrap the u16.
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

#[test]
fn test_insert_from_a_query_source_enqueues_its_rows() {
	// If only the inline path were wired, a pipeline source would enqueue nothing.
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

#[test]
fn test_a_failed_statement_leaves_no_partial_items() {
	// A rolled-back enqueue that survived would still be scheduled.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: 1 }, { id: "bad" }]"#);
	assert!(!err.is_empty());

	assert_eq!(
		t.query("FROM test::jobs")[0].rows().count(),
		0,
		"the valid row of a failed statement must not survive"
	);
}

#[test]
fn test_insert_does_not_require_admin() {
	// Producers are ordinary clients; requiring admin would make the primitive unusable.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ id: 1 }]");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

#[test]
fn test_the_insert_result_column_set_is_pinned() {
	// The result columns are a contract for callers scripting enqueues; pinning them
	// catches a silent rename or reordering.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command("INSERT test::jobs [{ id: 1 }]");
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["namespace", "queue", "inserted", "duplicates"]);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<Value>("inserted").unwrap().unwrap(), Value::Uint8(1));
}

#[test]
fn test_deduplication_duplicate_is_a_noop() {
	// Without the key a producer retrying after an ambiguous failure enqueues the work twice.
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

#[test]
fn test_deduplication_dedups_inside_a_single_statement() {
	// If dedup depended on commit boundaries, a batch retry would smuggle duplicates in.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 1 }, { id: 2 }] WITH { deduplication_key: "same" }"#);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 1);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 1);
	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

#[test]
fn test_the_deduplication_key_is_evaluated_per_row() {
	// Evaluated once per statement, a batch of genuinely different items would collapse into one.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, tag: utf8 } WITH { fifo: {} }");

	let frames = t.command(
		r#"INSERT test::jobs [{ id: 1, tag: "a" }, { id: 2, tag: "b" }] WITH { deduplication_key: tag }"#,
	);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2, "distinct keys must both enqueue");
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

#[test]
fn test_returning_on_a_duplicate_yields_the_existing_item() {
	// A retrying producer uses RETURNING to learn the identity of the work it enqueued first.
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

#[test]
fn test_the_same_deduplication_key_is_independent_per_queue() {
	// A global record would let a key used on one queue silently suppress it on every other.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::a { id: int4 } WITH { fifo: {} }");
	t.admin("CREATE QUEUE test::b { id: int4 } WITH { fifo: {} }");

	t.command(r#"INSERT test::a [{ id: 1 }] WITH { deduplication_key: "shared" }"#);
	let frames = t.command(r#"INSERT test::b [{ id: 1 }] WITH { deduplication_key: "shared" }"#);

	assert_eq!(frames[0].rows().next().unwrap().get::<u64>("inserted").unwrap().unwrap(), 1);
	assert_eq!(t.query("FROM test::b")[0].rows().count(), 1);
}

#[test]
fn test_a_none_deduplication_key_does_not_deduplicate() {
	// Treating none as a key would make every un-keyed item after the first a duplicate.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4, tag: Option(utf8) } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::jobs [{ id: 1 }, { id: 2 }] WITH { deduplication_key: tag }"#);
	let row = frames[0].rows().next().unwrap();

	assert_eq!(row.get::<u64>("inserted").unwrap().unwrap(), 2);
	assert_eq!(row.get::<u64>("duplicates").unwrap().unwrap(), 0);
}

#[test]
fn test_not_before_travels_on_the_row_change() {
	// The scheduling lane is rebuilt from stored rows after a crash, so a delay held only
	// in memory would come back as immediately due.
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

#[test]
fn test_a_delayed_item_is_still_visible_to_a_scan() {
	// If not_before gated the scan, an operator could not inspect scheduled work before it is due.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(1800000000000) }"#);

	let frames = t.query("FROM test::jobs");
	assert_eq!(frames[0].rows().count(), 1);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();
	assert_eq!(names, vec!["id"], "the hidden not_before field must still not surface");
}

#[test]
fn test_both_with_options_can_be_used_together() {
	// Two hidden columns are appended together; an index mistake would swap the key for the instant.
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

#[test]
fn test_with_on_a_table_is_rejected() {
	// Silently ignoring WITH would let a producer believe its items were deduplicated.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");

	let err = t.command_err(r#"INSERT test::t [{ id: 1 }] WITH { deduplication_key: "k" }"#);
	assert!(err.contains("INSERT_005"), "expected the queue-only diagnostic, got: {err}");
}

#[test]
fn test_an_unknown_with_option_is_rejected() {
	// Silently dropping a typo drops the guarantee the caller asked for.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: 1 }] WITH { idempotncy_key: "k" }"#);
	assert!(err.contains("INSERT_006"), "expected the unknown-option diagnostic, got: {err}");
}

#[test]
fn test_a_duplicate_with_option_is_rejected() {
	// Taking the last of a repeated option would apply a guarantee the caller did not intend.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err(
		r#"INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: "a", deduplication_key: "b" }"#,
	);
	assert!(err.contains("INSERT_007"), "expected the duplicate-option diagnostic, got: {err}");
}

#[test]
fn test_a_non_datetime_not_before_is_rejected() {
	// Accepting an integer instant would reintroduce the unit ambiguity typed temporals prevent.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err("INSERT test::jobs [{ id: 1 }] WITH { not_before: 12345 }");
	assert!(err.contains("CA_019"), "expected the not_before type diagnostic, got: {err}");
}

#[test]
fn test_a_non_utf8_deduplication_key_is_rejected() {
	// A non-text key could hash differently across runs or compare unequal to the same
	// logical key written elsewhere.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let err = t.command_err("INSERT test::jobs [{ id: 1 }] WITH { deduplication_key: 42 }");
	assert!(err.contains("CA_018"), "expected the deduplication_key type diagnostic, got: {err}");
}

#[test]
fn test_a_column_colliding_with_a_reserved_field_is_rejected() {
	// Two columns of the same name would silently shadow one; it has to fault at plan time.
	let t = engine_with_queue("CREATE QUEUE test::jobs { __queue_not_before: int4 } WITH { fifo: {} }");

	let err = t.command_err(r#"INSERT test::jobs [{ __queue_not_before: 1 }] WITH { deduplication_key: "k" }"#);
	assert!(err.contains("CA_017"), "expected the reserved-column diagnostic, got: {err}");
}

#[test]
fn test_a_reserved_column_name_is_allowed_without_with() {
	// The reserved-name check only bites when the hidden columns are appended, so a queue
	// carrying such a column stays usable for plain inserts.
	let t = engine_with_queue("CREATE QUEUE test::jobs { __queue_not_before: int4 } WITH { fifo: {} }");

	t.command("INSERT test::jobs [{ __queue_not_before: 7 }]");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1);
}

#[test]
fn test_update_on_a_queue_is_rejected_as_immutable() {
	// Items leave a queue by acknowledgement or retention, never by being edited.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err("UPDATE test::jobs { id: 2 } FILTER { id == 1 }");
	assert!(err.contains("QUEUE_001"), "expected the immutability diagnostic, got: {err}");
}

#[test]
fn test_delete_on_a_queue_is_rejected_as_immutable() {
	// Retention owns removal, not the caller.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err("DELETE test::jobs FILTER { id == 1 }");
	assert!(err.contains("QUEUE_001"), "expected the immutability diagnostic, got: {err}");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 1, "the item must survive the rejected DELETE");
}
