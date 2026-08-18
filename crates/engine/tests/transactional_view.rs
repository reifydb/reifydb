// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

fn source_and_view(view_body: &str) -> TestEngine {
	// Deliberately NOT TestEngine::new(): that turns CDC on, and CDC is the transport the
	// deferred engine rides. With no CDC there is no asynchronous path at all, so anything these
	// tests observe in the view can only have been written inline by the committing transaction.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	t.admin("CREATE TABLE tv::src { id: int4, v: int4 }");
	t.admin(&format!("CREATE TRANSACTIONAL VIEW tv::v {view_body}"));
	t
}

fn values(frames: &[Frame], column: &str) -> Vec<String> {
	let mut out: Vec<String> = frames
		.first()
		.and_then(|f| f.columns.iter().find(|c| c.name == column))
		.map(|c| (0..c.data.len()).map(|i| c.data.get_value(i).to_string()).collect())
		.unwrap_or_default();
	out.sort();
	out
}

#[test]
fn insert_is_visible_in_the_view_the_moment_the_command_returns() {
	// The whole point of a transactional view is that materialization happens inside the
	// committing transaction, not after it. So the read below deliberately has no drain, no
	// await_cdc, no sleep: if the rows only appeared via the deferred path this reads zero.
	// Falsified by routing transactional views through CDC - the assert would see 0, not 2.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");

	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");

	assert_eq!(values(&t.query("FROM tv::v"), "v"), vec!["10", "20"], "both rows must be in the view already");
}

#[test]
fn the_view_body_is_executed_not_bypassed_into_a_raw_copy() {
	// A sink that just mirrors the source table would pass the test above. Putting a filter and
	// an arithmetic map in the body forces the operator chain to actually run: the excluded row
	// must be absent and the surviving value must be transformed, not copied.
	let t = source_and_view("{ id: int4, doubled: int4 } AS { FROM tv::src FILTER { v > 15 } MAP { id, doubled: v * 2 } }");

	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]");

	let frames = t.query("FROM tv::v");
	assert_eq!(values(&frames, "id"), vec!["2", "3"], "the filter must drop the row below the threshold");
	assert_eq!(values(&frames, "doubled"), vec!["40", "60"], "the map must be applied, not the raw column");
}

#[test]
fn an_update_rewrites_the_existing_view_row_instead_of_appending_a_second_one() {
	// Row identity in the view hangs on the encoded row key. Get that encoding wrong and the
	// update lands under a different key: the count silently grows and both the old and the new
	// value are readable. Asserting the count alongside the value is what catches that.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");
	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");

	t.command("UPDATE tv::src { v: 99 } FILTER { id == 1 }");

	let frames = t.query("FROM tv::v");
	assert_eq!(TestEngine::row_count(&frames), 2, "an update must not add a row");
	assert_eq!(values(&frames, "v"), vec!["20", "99"], "the updated value must replace the old one");
}

#[test]
fn a_delete_removes_the_row_from_the_view() {
	// Sinks that only implement the insert half of the diff leave deleted rows readable forever,
	// which is worse than stale: the view reports records the source says no longer exist.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");
	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");

	t.command("DELETE tv::src FILTER { id == 1 }");

	let frames = t.query("FROM tv::v");
	assert_eq!(TestEngine::row_count(&frames), 1, "the deleted row must be gone from the view");
	assert_eq!(values(&frames, "v"), vec!["20"], "the surviving row must be the one that was not deleted");
}

#[test]
fn several_commands_accumulate_rather_than_the_last_one_winning() {
	// Each commit runs the flow against only its own change set. If the sink ever rebuilds from
	// the current batch instead of applying a diff onto stored state, the second insert wipes the
	// first and this reads 2 instead of 4.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");

	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	t.command("INSERT tv::src [{ id: 3, v: 30 }, { id: 4, v: 40 }]");

	assert_eq!(
		values(&t.query("FROM tv::v"), "v"),
		vec!["10", "20", "30", "40"],
		"every committed row must still be in the view"
	);
}
