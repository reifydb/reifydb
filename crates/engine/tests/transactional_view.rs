// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	event::{EventBus, EventListener, transaction::PostCommitEvent},
	interface::change::ChangeOrigin,
};
use reifydb_engine::vm::Command;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::frame::frame::Frame};

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

fn read_values(frames: &[Frame], column: &str) -> Vec<String> {
	// The read-your-own-writes cases send several statements in one request, so the view read is
	// the LAST frame, not the first. Using `values` here would assert against the INSERT receipt.
	let mut out: Vec<String> = frames
		.last()
		.and_then(|f| f.columns.iter().find(|c| c.name == column))
		.map(|c| (0..c.data.len()).map(|i| c.data.get_value(i).to_string()).collect())
		.unwrap_or_default();
	out.sort();
	out
}

fn created_id(frames: &[Frame]) -> u64 {
	// Every CREATE reports the new object id, which is the only handle a test has on the
	// ObjectId that the committed change stream tags its changes with.
	frames
		.first()
		.and_then(|f| f.columns.iter().find(|c| c.name == "id"))
		.map(|c| c.data.get_value(0).to_string())
		.expect("a create statement must report the new object id")
		.parse()
		.expect("an object id must be numeric")
}

struct CommitCapture(Arc<Mutex<Vec<PostCommitEvent>>>);

impl EventListener<PostCommitEvent> for CommitCapture {
	fn on(&self, event: &PostCommitEvent) {
		self.0.lock().push(event.clone());
	}
}

fn capture_commits(t: &TestEngine) -> Arc<Mutex<Vec<PostCommitEvent>>> {
	// PostCommitEvent.flow_changes IS the committed change stream: it is what CDC persists and
	// what the deferred subsystem consumes. Reading it directly is the only way to prove the
	// barrier did not swallow a change on its way to those consumers.
	let sink = Arc::new(Mutex::new(Vec::new()));
	let bus = t.inner().ioc().resolve::<EventBus>().expect("event bus must be registered");
	bus.register::<PostCommitEvent, _>(CommitCapture(sink.clone()));
	bus.wait_for_completion();
	sink
}

fn settle(t: &TestEngine) {
	let bus = t.inner().ioc().resolve::<EventBus>().expect("event bus must be registered");
	bus.wait_for_completion();
}

fn diffs_per_object(events: &[PostCommitEvent]) -> HashMap<u64, usize> {
	let mut counts: HashMap<u64, usize> = HashMap::new();
	for event in events {
		for change in event.flow_changes() {
			if let ChangeOrigin::Object(object) = &change.origin {
				*counts.entry(object.to_u64()).or_insert(0) += change.diffs.len();
			}
		}
	}
	counts
}

#[test]
fn a_write_and_a_view_read_in_one_request_see_the_write() {
	// The objective of the whole refactor: the read below runs in the same transaction as the
	// insert above it and before that transaction commits, so it can only succeed if the flow
	// ran inline at the read barrier. Before this work the same request raised TXN_015.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");

	let frames = t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::v");

	assert_eq!(
		read_values(&frames, "v"),
		vec!["10"],
		"the view must show the row inserted earlier in this request"
	);
}

#[test]
fn an_update_and_a_delete_in_the_same_request_are_visible_in_the_view() {
	// Insert is the easy diff: a sink that only handles Insert would pass the test above and
	// still leave the view reporting rows the source no longer has. Update and delete must reach
	// the view through the same inline barrier.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");
	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");

	let updated = t.command("UPDATE tv::src { v: 99 } FILTER { id == 1 }; FROM tv::v");
	assert_eq!(read_values(&updated, "v"), vec!["20", "99"], "the update must be visible in the view already");

	let deleted = t.command("DELETE tv::src FILTER { id == 2 }; FROM tv::v");
	assert_eq!(read_values(&deleted, "v"), vec!["99"], "the delete must be visible in the view already");
}

#[test]
fn a_view_over_a_view_is_current_when_read_in_the_writing_request() {
	// The barrier walks the whole upstream closure, not just the direct parent. Reading v2 must
	// drag src -> v1 -> v2 through in topological order; running only the last hop would read v1
	// as it was before the insert and produce nothing.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	t.admin("CREATE TABLE tv::src { id: int4, v: int4 }");
	t.admin(
		"CREATE TRANSACTIONAL VIEW tv::v1 { id: int4, doubled: int4 } AS { FROM tv::src MAP { id, doubled: v * 2 } }",
	);
	t.admin("CREATE TRANSACTIONAL VIEW tv::v2 { id: int4, quadrupled: int4 } AS { FROM tv::v1 MAP { id, quadrupled: doubled * 2 } }");

	let frames = t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::v2");

	assert_eq!(
		read_values(&frames, "quadrupled"),
		vec!["40"],
		"the write must reach the second hop, not stop at the view it feeds directly"
	);
}

#[test]
fn a_diamond_applies_the_shared_source_to_the_sink_exactly_once() {
	// src fans out to a and b, which both feed c. Without the in-degree bookkeeping in
	// calculate_schedule, c is dispatched once per parent: the first pass joins a against an
	// empty b and emits a row, the second pass corrects it. The end state hides that, so the
	// assertion is on the committed change stream, where a double dispatch shows up as two diffs
	// for c instead of one.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	t.admin("CREATE TABLE tv::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW tv::a { id: int4, a: int4 } AS { FROM tv::src MAP { id, a: v * 2 } }");
	t.admin("CREATE TRANSACTIONAL VIEW tv::b { id: int4, b: int4 } AS { FROM tv::src MAP { id, b: v * 3 } }");
	let c = created_id(&t.admin(
		"CREATE TRANSACTIONAL VIEW tv::c { id: int4, total: int4 } AS { FROM tv::a LEFT JOIN { FROM tv::b } AS b USING (id, b.id) MAP { id, total: a + b.b } }",
	));
	let commits = capture_commits(&t);

	let frames = t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::c");

	assert_eq!(read_values(&frames, "total"), vec!["50"], "the sink must see both branches of the diamond");
	settle(&t);
	assert_eq!(
		diffs_per_object(&commits.lock()).get(&c).copied(),
		Some(1),
		"the shared source must reach the sink once, not once per branch"
	);
}

#[test]
fn a_rolled_back_request_leaves_the_view_as_it_was() {
	// The inline barrier writes view rows through the transaction, so a rollback must take them
	// with it. If the barrier wrote around the transaction, the row below would survive the
	// discard and the view would report a row no committed source write ever produced.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");
	let engine = t.inner();

	let mut txn = engine.begin_command(TestEngine::identity()).unwrap();
	let outcome = engine.executor().command(
		&mut txn,
		Command {
			rql: "INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::v",
			params: Params::None,
		},
	);
	assert!(outcome.error.is_none(), "the in-transaction read must succeed: {:?}", outcome.error);
	assert_eq!(read_values(&outcome.frames, "v"), vec!["10"], "the uncommitted read must still see the write");
	txn.rollback().unwrap();

	assert_eq!(TestEngine::row_count(&t.query("FROM tv::v")), 0, "a rolled back barrier must leave no view rows");
	assert_eq!(TestEngine::row_count(&t.query("FROM tv::src")), 0, "the rolled back insert must be gone too");
}

#[test]
fn reading_a_view_whose_sources_are_untouched_changes_nothing() {
	// A read barrier that fires on every view read, rather than only when the view's upstream is
	// dirty, would re-feed already-consumed changes and apply the sink a second time. The
	// repeated reads below must be inert: same rows, and no flow change published by the reading
	// requests at all.
	let t = source_and_view("{ id: int4, v: int4 } AS { FROM tv::src }");
	t.command("INSERT tv::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	let commits = capture_commits(&t);

	t.command("FROM tv::v");
	t.command("FROM tv::v");
	let frames = t.command("FROM tv::v");

	assert_eq!(read_values(&frames, "v"), vec!["10", "20"], "repeated reads must return the same rows");
	settle(&t);
	assert!(
		diffs_per_object(&commits.lock()).is_empty(),
		"reading a view with no dirty upstream must publish no flow change at all"
	);
}

#[test]
fn the_committed_change_stream_still_carries_the_source_write_after_a_barrier() {
	// The barrier takes the source changes out of the accumulator so a later read cannot replay
	// them. A naive take-and-drop loses them for good: CDC and every deferred consumer would
	// never see the insert into src, only the view rows it produced. Both must be in the stream,
	// each exactly once.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	let src = created_id(&t.admin("CREATE TABLE tv::src { id: int4, v: int4 }"));
	let view = created_id(&t.admin("CREATE TRANSACTIONAL VIEW tv::v { id: int4, v: int4 } AS { FROM tv::src }"));
	let commits = capture_commits(&t);

	t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::v");
	settle(&t);

	let counts = diffs_per_object(&commits.lock());
	assert_eq!(
		counts.get(&src).copied(),
		Some(1),
		"the source write must survive the barrier into the commit stream"
	);
	assert_eq!(counts.get(&view).copied(), Some(1), "the view rows the barrier produced must be published once");
}

#[test]
fn the_durable_cdc_record_of_a_barrier_commit_still_carries_the_source_write() {
	// A deferred view downstream of the same source replays the persisted CDC record, not the
	// in-process event. TestEngine wires no deferred subsystem, so the closest observable is the
	// record itself: if the barrier consumed src's insert on its way to the transactional view,
	// nothing downstream can ever rebuild from it. A deferred view over src is created anyway so
	// the request carries the same catalog shape the real failure needs.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE tv");
	let src = created_id(&t.admin("CREATE TABLE tv::src { id: int4, v: int4 }"));
	let live = created_id(&t.admin("CREATE TRANSACTIONAL VIEW tv::live { id: int4, v: int4 } AS { FROM tv::src }"));
	t.admin("CREATE DEFERRED VIEW tv::later { id: int4, v: int4 } AS { FROM tv::src }");

	let frames = t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::live");
	assert_eq!(read_values(&frames, "v"), vec!["10"], "the transactional view must be current inside the request");

	let version = t.await_cdc();
	let store = t.inner().ioc().resolve::<CdcStore>().expect("cdc store must be registered");
	let record = store.read(version).unwrap().expect("the barrier commit must produce a cdc record");
	let mut counts: HashMap<u64, usize> = HashMap::new();
	for change in &record.changes {
		if let ChangeOrigin::Object(object) = &change.origin {
			*counts.entry(object.to_u64()).or_insert(0) += change.diffs.len();
		}
	}
	assert_eq!(counts.get(&src).copied(), Some(1), "the source write must be replayable by a deferred consumer");
	assert_eq!(counts.get(&live).copied(), Some(1), "the inline view rows must be in the record exactly once");
}

#[test]
fn a_barrier_runs_only_the_flows_upstream_of_the_view_being_read() {
	// Two independent trees. Reading v1 must flush s1 only: if the barrier ignored its object
	// filter it would run s2's flow too, and because that flow's input is still in the
	// accumulator the commit would run it a second time and publish v2 twice.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	let s1 = created_id(&t.admin("CREATE TABLE tv::s1 { id: int4, v: int4 }"));
	let s2 = created_id(&t.admin("CREATE TABLE tv::s2 { id: int4, v: int4 }"));
	let v1 = created_id(&t.admin("CREATE TRANSACTIONAL VIEW tv::v1 { id: int4, v: int4 } AS { FROM tv::s1 }"));
	let v2 = created_id(&t.admin("CREATE TRANSACTIONAL VIEW tv::v2 { id: int4, v: int4 } AS { FROM tv::s2 }"));
	let commits = capture_commits(&t);

	let frames = t.command("INSERT tv::s1 [{ id: 1, v: 10 }]; INSERT tv::s2 [{ id: 2, v: 20 }]; FROM tv::v1");

	assert_eq!(read_values(&frames, "v"), vec!["10"], "the read must see only its own tree");
	settle(&t);
	let counts = diffs_per_object(&commits.lock());
	assert_eq!(counts.get(&s1).copied(), Some(1), "s1 must be published once");
	assert_eq!(counts.get(&s2).copied(), Some(1), "s2 must be published once");
	assert_eq!(counts.get(&v1).copied(), Some(1), "the barrier must apply v1 exactly once");
	assert_eq!(
		counts.get(&v2).copied(),
		Some(1),
		"v2 must be applied once at commit, not at the unrelated barrier"
	);
	assert_eq!(values(&t.query("FROM tv::v2"), "v"), vec!["20"], "v2 must still be materialized by the commit");
}

#[test]
fn two_reads_after_one_insert_apply_the_flow_once() {
	// The second read must find nothing left to do. If the barrier failed to consume what it
	// fed, or parked it somewhere the next barrier reads back, the same insert is applied twice:
	// invisible in a row-keyed sink, but plain in the change stream as two diffs for the view.
	let t = TestEngine::builder().build();
	t.admin("CREATE NAMESPACE tv");
	t.admin("CREATE TABLE tv::src { id: int4, v: int4 }");
	let view = created_id(&t.admin("CREATE TRANSACTIONAL VIEW tv::v { id: int4, v: int4 } AS { FROM tv::src }"));
	let commits = capture_commits(&t);

	let frames = t.command("INSERT tv::src [{ id: 1, v: 10 }]; FROM tv::v; FROM tv::v");

	assert_eq!(read_values(&frames, "v"), vec!["10"], "the second read must return the same single row");
	settle(&t);
	assert_eq!(
		diffs_per_object(&commits.lock()).get(&view).copied(),
		Some(1),
		"one insert read twice must apply the view flow exactly once"
	);
}
