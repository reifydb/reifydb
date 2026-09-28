// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId, row_number::RowNumber},
};

fn seeded() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, kind: utf8 }");
	t.command("INSERT test::t [{ id: 1, kind: 'a' }, { id: 2, kind: 'b' }]");
	t
}

fn stamped() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::s { id: int4, kind: utf8 } WITH { partition: { by: { kind } }, time: processing }");
	t.command("INSERT test::s [{ id: 1, kind: 'a' }, { id: 2, kind: 'a' }]");
	t
}

fn query_internal(t: &TestEngine, rql: &str) -> Vec<Frame> {
	let result = t.begin_query(IdentityId::system()).unwrap().rql(rql, Params::None);
	if let Some(e) = result.error {
		panic!("internal query failed: {e:?}\nrql: {rql}")
	}
	result.frames
}

fn only_frame(frames: &[Frame]) -> &Frame {
	assert_eq!(frames.len(), 1, "one statement answers with exactly one frame");
	&frames[0]
}

fn ids(frame: &Frame) -> Vec<i32> {
	frame.rows().map(|row| row.get::<i32>("id").unwrap().unwrap()).collect()
}

#[track_caller]
fn assert_rownum_without_other_unnamed_system_columns(t: &TestEngine, rql: &str) {
	let internal = query_internal(t, rql);
	let full = only_frame(&internal);
	assert!(
		!full.created_at().is_empty(),
		"`{rql}` must build #created_at inside, or its absence below proves nothing"
	);
	assert!(
		!full.updated_at().is_empty(),
		"`{rql}` must build #updated_at inside, or its absence below proves nothing"
	);
	assert!(!full.time().is_empty(), "`{rql}` must build #time inside, or its absence below proves nothing");
	assert!(
		!full.system.partitions().is_empty(),
		"`{rql}` must build #partition inside, or its absence below proves nothing"
	);
	assert!(
		!full.system.commit_versions().is_empty(),
		"`{rql}` must build #commit_version inside, or its absence below proves nothing"
	);

	let frames = t.query(rql);
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![2, 1], "the edge must keep the scan's rows in the scan's order, newest first");
	assert_eq!(frame.row_numbers(), &[RowNumber(2), RowNumber(1)], "`{rql}` must carry #rownum, named or not");
	assert!(frame.to_string().contains("#rownum"), "`{rql}` must print its #rownum column:\n{frame}");
	assert!(frame.created_at().is_empty(), "`{rql}` never names #created_at, so it must not carry it");
	assert!(frame.updated_at().is_empty(), "`{rql}` never names #updated_at, so it must not carry it");
	assert!(frame.time().is_empty(), "`{rql}` never names #time, so it must not carry it");
	assert!(frame.system.partitions().is_empty(), "`{rql}` never names #partition, so it must not carry it");
	assert!(
		frame.system.commit_versions().is_empty(),
		"`{rql}` never names #commit_version, so it must not carry it"
	);
}

#[test]
fn a_plain_query_carries_rownum_but_no_other_unnamed_system_column() {
	// #rownum is always sent; every other # column the scan builds must stop at the edge unless named.
	let t = stamped();
	assert_rownum_without_other_unnamed_system_columns(&t, "FROM test::s");
}

#[test]
fn a_map_carries_rownum_but_no_other_unnamed_system_column() {
	// Map passes its input's system columns through, so the edge is the only place the unnamed ones stop.
	let t = stamped();
	assert_rownum_without_other_unnamed_system_columns(&t, "FROM test::s | map { id }");
}

#[test]
fn returning_a_plain_column_carries_the_written_rows_rownum() {
	// Passthrough returning must carry the written rows' #rownum, never other unnamed # columns.
	let t = seeded();
	let rql = "INSERT test::t [{ id: 3, kind: 'c' }] RETURNING { id }";
	let frames = t.command(rql);
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![3], "returning must still answer with the written row");
	assert_eq!(frame.row_numbers(), &[RowNumber(3)], "`{rql}` must carry the written row's #rownum");
	assert!(frame.to_string().contains("#rownum"), "`{rql}` must print its #rownum column:\n{frame}");
	assert!(frame.created_at().is_empty(), "`{rql}` never names #created_at, so it must not carry it");
	assert!(frame.updated_at().is_empty(), "`{rql}` never names #updated_at, so it must not carry it");
	assert!(frame.time().is_empty(), "`{rql}` never names #time, so it must not carry it");
}

#[test]
fn returning_an_expression_carries_the_updated_rows_rownum() {
	// Evaluating returning must carry the updated rows' #rownum, never other unnamed # columns.
	let t = seeded();
	let rql = "UPDATE test::t { kind: 'z' } FILTER { id == 2 } RETURNING { id, twice: id * 2 }";
	let frames = t.command(rql);
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![2], "returning must still answer with the updated row");
	assert_eq!(frame.row_numbers(), &[RowNumber(2)], "`{rql}` must carry the updated row's #rownum");
	assert!(frame.to_string().contains("#rownum"), "`{rql}` must print its #rownum column:\n{frame}");
	assert!(frame.created_at().is_empty(), "`{rql}` never names #created_at, so it must not carry it");
	assert!(frame.updated_at().is_empty(), "`{rql}` never names #updated_at, so it must not carry it");
	assert!(frame.time().is_empty(), "`{rql}` never names #time, so it must not carry it");
}

#[test]
fn a_query_that_names_rownum_carries_it() {
	// A filter on #rownum must still see the scan's row numbers, and the result keeps them.
	let t = seeded();
	let frames = t.query("FROM test::t | filter { #rownum > 1 }");
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![2], "the filter must read the real row numbers");
	assert!(frame.has_row_numbers(), "a query that names #rownum must carry it");
	assert_eq!(frame.row_numbers(), &[RowNumber(2)], "the carried row number must be the row's own");
}

#[test]
fn a_query_that_sorts_by_rownum_carries_it() {
	// A sort key is a reference too; without the mark the sort reads no row numbers.
	let t = seeded();
	let frames = t.query("FROM test::t | sort { #rownum:DESC }");
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![2, 1], "the sort must order by the real row numbers");
	assert_eq!(frame.row_numbers(), &[RowNumber(2), RowNumber(1)], "the sorted rows keep their own numbers");
}

#[test]
fn commit_version_from_a_ringbuffer_fails_at_planning() {
	// An empty ringbuffer never evaluates the filter, so only a planning-time check reports the missing column.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::r { id: int4 } WITH { capacity: 10 }");
	let err = t.query_err("FROM test::r | filter { #commit_version > 0 }");
	assert!(err.contains("QUERY_001"), "a system column the source lacks is a column not found, got {err}");
	assert!(err.contains("ringbuffers have no #commit_version"), "the note must say why, got {err}");
}

#[test]
fn commit_version_from_a_filled_ringbuffer_carries_the_note() {
	// With rows the evaluator would also fail, but without the note naming why.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::r { id: int4 } WITH { capacity: 10 }");
	t.command("INSERT test::r [{ id: 1 }]");
	let err = t.query_err("FROM test::r | map { v: #commit_version }");
	assert!(err.contains("QUERY_001"), "a system column the source lacks is a column not found, got {err}");
	assert!(err.contains("ringbuffers have no #commit_version"), "the note must say why, got {err}");
}

#[test]
fn update_and_delete_still_address_their_rows() {
	// Update and delete address rows by #rownum; a scan that drops it unnamed fails them with a missing row number.
	let t = seeded();
	t.command("UPDATE test::t { kind: 'z' } FILTER { id == 1 }");
	t.command("DELETE test::t FILTER { id == 2 }");
	let frames = t.query("FROM test::t");
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![1], "delete must remove exactly the filtered row");
	let kinds: Vec<String> = frame.rows().map(|row| row.get::<String>("kind").unwrap().unwrap()).collect();
	assert_eq!(kinds, vec!["z".to_string()], "update must change exactly the filtered row");
}

#[test]
fn update_and_delete_on_a_partitioned_table_still_address_their_rows() {
	// A partitioned row key also needs #partition; without it update and delete report a missing partition address.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::p { id: int4, region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	t.command("INSERT test::p [{ id: 1, region: 'eu', n: 1 }, { id: 2, region: 'us', n: 2 }]");
	t.command("INSERT test::p [{ id: 3, region: 'eu', n: 3 }]");
	t.command("UPDATE test::p { n: 10 } FILTER { id == 1 }");
	t.command("DELETE test::p FILTER { id == 3 }");
	let frames = t.query("FROM test::p | sort { id:ASC }");
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![1, 2], "delete must remove exactly the filtered row");
	let ns: Vec<i32> = frame.rows().map(|row| row.get::<i32>("n").unwrap().unwrap()).collect();
	assert_eq!(ns, vec![10, 2], "update must change exactly the filtered row");
}

#[test]
fn update_and_delete_on_a_ringbuffer_still_address_their_rows() {
	// A ringbuffer addresses rows by #rownum like a table does, through its own write path.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::r { id: int4, n: int4 } WITH { capacity: 10 }");
	t.command("INSERT test::r [{ id: 1, n: 1 }, { id: 2, n: 2 }, { id: 3, n: 3 }]");
	t.command("UPDATE test::r { n: 10 } FILTER { id == 1 }");
	t.command("DELETE test::r FILTER { id == 3 }");
	let frames = t.query("FROM test::r | sort { id:ASC }");
	let frame = only_frame(&frames);
	assert_eq!(ids(frame), vec![1, 2], "delete must remove exactly the filtered row");
	let ns: Vec<i32> = frame.rows().map(|row| row.get::<i32>("n").unwrap().unwrap()).collect();
	assert_eq!(ns, vec![10, 2], "update must change exactly the filtered row");
}

#[test]
fn queue_claim_keeps_the_row_numbers_of_the_claimed_items() {
	// Claimed items are stored rows; dropping their row numbers leaves ack and kill nothing to address.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");
	let frames = t.command(r#"CALL queue::claim("w1", "test::jobs", 2, duration::seconds(30))"#);
	let frame = only_frame(&frames);
	assert_eq!(frame.row_count(), 2, "both items must be claimed");
	assert!(frame.has_row_numbers(), "a claim result must carry the items' row numbers");
	let mut claimed: Vec<u64> = frame.row_numbers().iter().map(|row| row.value()).collect();
	claimed.sort();
	assert_eq!(claimed, vec![1, 2], "the carried row numbers must be the items' own");
}
