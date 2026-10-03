// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	ops::Bound,
};

use reifydb_cdc::lift::lift_changes;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{config::ConfigKey, object::ObjectId},
		cdc::Cdc,
		change::{ChangeOrigin, Diff},
	},
};
use reifydb_store_cdc::storage::CdcStorage;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	params::Params,
	value::{Value, frame::frame::Frame, identity::IdentityId, system_columns::row_numbers},
};

const COLUMNS: &str = "id: int4, sym: utf8, v: int4, at: datetime";

const DICTIONARY_COLUMNS: &str = "id: int4, sym: utf8 with { dictionary: bf::syms }, v: int4, at: datetime";

type Script = fn(&str) -> Vec<String>;

type Reader = fn(&[Frame]) -> Vec<String>;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE bf");
	t.admin("CREATE DICTIONARY bf::syms FOR utf8 AS uint4");
	t.admin(&format!("CREATE TABLE bf::src {{ {COLUMNS} }}"));
	t.admin(&format!("CREATE TABLE bf::src2 {{ {COLUMNS} }}"));
	t.admin(&format!("CREATE TABLE bf::dsrc {{ {DICTIONARY_COLUMNS} }}"));
	t.admin(&format!("CREATE TABLE bf::psrc {{ {COLUMNS} }} WITH {{ time: processing }}"));
	t.admin(&format!("CREATE TABLE bf::esrc {{ {COLUMNS} }} WITH {{ time: event(at) }}"));
	t
}

fn engine_with_batch(batch: u16) -> TestEngine {
	let t = engine();
	t.set_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch));
	t
}

fn history(table: &str) -> Vec<String> {
	vec![
		format!(
			"INSERT {table} [{{ id: 1, sym: 'a', v: 5, at: @2020-01-01T00:00:01Z }}, {{ id: 2, sym: 'b', v: 50, at: @2020-01-01T00:00:02Z }}, {{ id: 3, sym: 'c', v: 95, at: @2020-01-01T00:00:03Z }}]"
		),
		format!("UPDATE {table} {{ v: 60, at: @2020-02-01T00:00:01Z }} FILTER {{ id == 1 }}"),
		format!("UPDATE {table} {{ v: 5 }} FILTER {{ id == 2 }}"),
		format!("DELETE {table} FILTER {{ id == 3 }}"),
		format!(
			"INSERT {table} [{{ id: 4, sym: 'd', v: 40, at: @2020-01-01T00:00:04Z }}]; UPDATE {table} {{ v: 70 }} FILTER {{ id == 4 }}"
		),
		format!(
			"INSERT {table} [{{ id: 5, sym: 'e', v: 90, at: @2020-01-01T00:00:05Z }}]; DELETE {table} FILTER {{ id == 5 }}"
		),
		format!("UPDATE {table} {{ sym: 'f' }} FILTER {{ id == 1 }}"),
		format!(
			"INSERT {table} [{{ id: 6, sym: 'a', v: 30, at: @2020-01-01T00:00:06Z }}, {{ id: 7, sym: 'g', v: 85, at: @2020-01-01T00:00:07Z }}]"
		),
	]
}

fn later(table: &str) -> Vec<String> {
	vec![
		format!("INSERT {table} [{{ id: 8, sym: 'h', v: 55, at: @2020-01-01T00:00:08Z }}]"),
		format!("UPDATE {table} {{ v: 99 }} FILTER {{ id == 1 }}"),
		format!("UPDATE {table} {{ v: 1 }} FILTER {{ id == 4 }}"),
		format!("UPDATE {table} {{ v: 77, sym: 'i' }} FILTER {{ id == 2 }}"),
		format!("DELETE {table} FILTER {{ id == 7 }}"),
		format!("DELETE {table} FILTER {{ id == 8 }}"),
		format!(
			"INSERT {table} [{{ id: 9, sym: 'j', v: 65, at: @2020-01-01T00:00:09Z }}]; UPDATE {table} {{ v: 66 }} FILTER {{ id == 9 }}; DELETE {table} FILTER {{ id == 6 }}"
		),
		format!("UPDATE {table} {{ at: @2021-01-01T00:00:00Z }} FILTER {{ id == 1 }}"),
	]
}

fn insert_range(table: &str, from: i32, to: i32) -> String {
	let rows: Vec<String> = (from..=to)
		.map(|id| format!("{{ id: {id}, sym: 's{id}', v: {}, at: @2020-01-01T00:00:{id:02}Z }}", id * 5))
		.collect();
	format!("INSERT {table} [{}]", rows.join(", "))
}

fn bulk(table: &str) -> Vec<String> {
	vec![
		insert_range(table, 1, 12),
		insert_range(table, 13, 20),
		format!("DELETE {table} FILTER {{ id > 4 and id < 8 }}"),
		format!("UPDATE {table} {{ v: 1 }} FILTER {{ id > 14 }}"),
		format!("DELETE {table} FILTER {{ id == 20 }}"),
	]
}

fn bulk_ids() -> Vec<i32> {
	(1..=4).chain(8..=19).collect()
}

fn statements(script: Script, tables: &[&str]) -> Vec<String> {
	let per_table: Vec<Vec<String>> = tables.iter().map(|table| script(table)).collect();
	(0..per_table[0].len()).flat_map(|i| per_table.iter().map(|s| s[i].clone()).collect::<Vec<_>>()).collect()
}

fn write(t: &TestEngine, rql: &str) {
	t.mock_clock().advance_secs(1);
	t.command(rql);
}

fn create(t: &TestEngine, view: &str, columns: &str, body: &str) {
	t.mock_clock().advance_secs(1);
	t.admin(&format!("CREATE TRANSACTIONAL VIEW {view} {{ {columns} }} AS {{ {body} }}"));
}

fn cells(frames: &[Frame], keep: fn(&str) -> bool) -> Vec<String> {
	frames.iter()
		.flat_map(|frame| frame.to_rows())
		.map(|row| {
			assert!(
				row.iter().any(|(name, _)| name == "#rownum"),
				"a row came back without #rownum: {row:?}"
			);
			let mut cells: Vec<String> = row
				.into_iter()
				.filter(|(name, _)| keep(name))
				.map(|(name, value)| format!("{name}={value}"))
				.collect();
			cells.sort();
			cells.join(",")
		})
		.collect()
}

fn rows(frames: &[Frame]) -> Vec<String> {
	cells(frames, |name| name == "#rownum" || !name.starts_with('#'))
}

fn stamps(frames: &[Frame]) -> Vec<String> {
	cells(frames, |name| matches!(name, "#rownum" | "#created_at" | "#updated_at" | "#time"))
}

fn ids(frames: &[Frame]) -> Vec<i32> {
	let mut ids: Vec<i32> = frames
		.iter()
		.flat_map(|frame| frame.rows().map(|row| row.get::<i32>("id").unwrap().unwrap()).collect::<Vec<_>>())
		.collect();
	ids.sort();
	ids
}

fn pairs(frames: &[Frame]) -> Vec<(i32, i32)> {
	let mut pairs: Vec<(i32, i32)> = frames
		.iter()
		.flat_map(|frame| {
			frame.rows()
				.map(|row| {
					(row.get::<i32>("id").unwrap().unwrap(), row.get::<i32>("v").unwrap().unwrap())
				})
				.collect::<Vec<_>>()
		})
		.collect();
	pairs.sort();
	pairs
}

fn distinct_rownums(frames: &[Frame]) -> usize {
	frames.iter()
		.flat_map(|frame| {
			row_numbers(&frame.batch).expect("row numbers").iter().map(|r| r.0).collect::<Vec<_>>()
		})
		.collect::<BTreeSet<u64>>()
		.len()
}

fn agree(t: &TestEngine, twins: &[(&str, &str)], readers: &[Reader], when: &str) {
	for (early, late) in twins {
		let early_frames = t.query(&format!("FROM {early}"));
		let late_frames = t.query(&format!("FROM {late}"));
		for read in readers {
			assert_eq!(read(&late_frames), read(&early_frames), "{late} diverged from {early} {when}");
		}
	}
}

fn late_equals_early(
	t: &TestEngine,
	columns: &str,
	body: &str,
	tables: &[&str],
	script: Script,
	want: &[i32],
	readers: &[Reader],
) {
	create(t, "bf::early", columns, body);
	for rql in statements(script, tables) {
		write(t, &rql);
	}
	assert_eq!(ids(&t.query("FROM bf::early")), want, "the early view is off before the late create");
	create(t, "bf::late", columns, body);
	agree(t, &[("bf::early", "bf::late")], readers, "right after the late create");
	for rql in statements(later, tables) {
		write(t, &rql);
		agree(t, &[("bf::early", "bf::late")], readers, &format!("after: {rql}"));
	}
}

fn in_txn(txn: &mut AdminTransaction, rql: &str) -> Vec<Frame> {
	let r = txn.rql(rql, Params::None);
	assert!(r.error.is_none(), "{rql} failed: {:?}", r.error);
	r.frames
}

fn named(t: &TestEngine, catalog_table: &str, name: &str) -> usize {
	TestEngine::row_count(&t.query(&format!("FROM system::{catalog_table} filter {{ name == '{name}' }}")))
}

fn view_object(t: &TestEngine, name: &str) -> ObjectId {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	let mut txn = Transaction::Query(&mut query);
	let catalog = t.catalog();
	let namespace =
		catalog.find_namespace_by_name(&mut txn, "bf").expect("namespace lookup").expect("namespace bf");
	let view = catalog.find_view_by_name(&mut txn, namespace.id(), name).expect("view lookup").expect("view");
	ObjectId::View(view.id())
}

fn cdc_at(t: &TestEngine, version: CommitVersion) -> Cdc {
	t.await_cdc();
	t.cdc_store()
		.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
		.expect("cdc read")
		.items
		.into_iter()
		.find(|cdc| cdc.version.commit == version)
		.unwrap_or_else(|| panic!("no cdc record for commit {version:?}"))
}

fn inserted_rows_per_object(t: &TestEngine, cdc: &Cdc) -> BTreeMap<ObjectId, usize> {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	let mut counts = BTreeMap::new();
	for change in lift_changes(cdc, &t.catalog(), &mut Transaction::Query(&mut query)).expect("lift") {
		let ChangeOrigin::Object(object) = change.origin else {
			continue;
		};
		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => *counts.entry(object).or_insert(0) += post.num_rows(),
				other => {
					panic!("the create commit carried a non-insert diff for {object:?}: {other:?}")
				}
			}
		}
	}
	counts
}

fn seeded_with_an_early_view() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE bf");
	t.admin("CREATE TABLE bf::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW bf::early { id: int4, v: int4 } AS { FROM bf::src }");
	t.command("INSERT bf::src [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]");
	t
}

#[test]
fn a_late_filter_view_equals_the_early_one() {
	// Rows that crossed the filter in both directions before the create must end up exactly as live ones did.
	let t = engine();
	late_equals_early(&t, COLUMNS, "FROM bf::src | filter { v > 50 }", &["bf::src"], history, &[1, 4, 7], &[rows]);
}

#[test]
fn a_late_map_view_equals_the_early_one() {
	// The projection must run on the snapshot rows and keep each source row number.
	let t = engine();
	late_equals_early(
		&t,
		"id: int4, v: int4",
		"FROM bf::src | map { id, v }",
		&["bf::src"],
		history,
		&[1, 2, 4, 6, 7],
		&[rows],
	);
}

#[test]
fn a_late_extend_view_equals_the_early_one() {
	// The computed column must be computed from the row as it is at the create, not as it was first inserted.
	let t = engine();
	late_equals_early(
		&t,
		&format!("{COLUMNS}, w: int4"),
		"FROM bf::src | extend { w: v + 1 }",
		&["bf::src"],
		history,
		&[1, 2, 4, 6, 7],
		&[rows],
	);
}

#[test]
fn a_late_filter_then_map_view_equals_the_early_one() {
	// A later update must retract a backfilled row by the value the map dropped, or the view keeps a stale row.
	let t = engine();
	late_equals_early(
		&t,
		"id: int4, sym: utf8",
		"FROM bf::src | filter { v < 50 } | map { id, sym }",
		&["bf::src"],
		history,
		&[2, 6],
		&[rows],
	);
}

#[test]
fn a_late_append_of_two_tables_equals_the_early_one() {
	// Both inputs must be scanned and get the lane row numbers live rows get, or rows collide or double.
	let t = engine();
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | append { FROM bf::src2 }",
		&["bf::src", "bf::src2"],
		history,
		&[1, 1, 2, 2, 4, 4, 6, 6, 7, 7],
		&[rows],
	);
}

#[test]
fn a_late_append_of_a_table_with_itself_equals_the_early_one() {
	// Both source operators read the scanned table; feeding only the first one leaves the second lane empty.
	let t = engine();
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | append { FROM bf::src }",
		&["bf::src"],
		history,
		&[1, 1, 2, 2, 4, 4, 6, 6, 7, 7],
		&[rows],
	);
}

#[test]
fn a_late_append_of_a_table_with_a_filtered_copy_of_itself_is_complete_in_chunks() {
	// Every chunk must reach both branches, so the filtered lane holds exactly the rows its filter keeps.
	let t = engine_with_batch(1);
	let mut want: Vec<i32> = bulk_ids().into_iter().chain([11, 12, 13, 14]).collect();
	want.sort();
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | append { FROM bf::src | filter { v > 50 } }",
		&["bf::src"],
		bulk,
		&want,
		&[rows],
	);
}

#[test]
fn a_late_sorted_view_equals_the_early_one_in_key_order() {
	// Backfilled rows must be keyed by the sort value, or a later sort value change leaves the old row behind.
	let t = engine();
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | filter { v > 20 } | sort { v }",
		&["bf::src"],
		history,
		&[1, 4, 6, 7],
		&[rows],
	);
}

#[test]
fn a_late_view_over_a_dictionary_column_equals_the_early_one() {
	// The snapshot must hand the view the text, not the dictionary id, or the view shows numbers.
	let t = engine();
	late_equals_early(&t, COLUMNS, "FROM bf::dsrc", &["bf::dsrc"], history, &[1, 2, 4, 6, 7], &[rows]);
}

#[test]
fn a_late_view_with_a_dictionary_column_equals_the_early_one() {
	// Backfilled text must be interned into the view's dictionary, or a later delete fails its lookup.
	let t = engine();
	late_equals_early(&t, DICTIONARY_COLUMNS, "FROM bf::dsrc", &["bf::dsrc"], history, &[1, 2, 4, 6, 7], &[rows]);
}

fn keeps_the_row_stamps(table: &str) {
	let t = engine();
	let body = format!("FROM {table}");
	late_equals_early(&t, COLUMNS, &body, &[table], history, &[1, 2, 4, 6, 7], &[rows, stamps]);
	create(&t, "bf::copy", COLUMNS, &body);
	let source = t.query(&body);
	let copy = t.query("FROM bf::copy");
	assert_eq!(rows(&copy), rows(&source), "a late view must hold the source rows at the create");
	assert_eq!(stamps(&copy), stamps(&source), "a late view must keep each row's stored stamps");
}

#[test]
fn a_late_view_over_a_table_without_time_keeps_the_row_stamps() {
	// Stamping backfilled rows with the create time instead of the stored created_at and updated_at must fail here.
	keeps_the_row_stamps("bf::src");
}

#[test]
fn a_late_view_over_a_processing_time_table_keeps_the_row_stamps() {
	// The stored #time is the arrival time; a backfill that re-stamps it with the create time must fail here.
	keeps_the_row_stamps("bf::psrc");
}

#[test]
fn a_late_view_over_an_event_time_table_keeps_the_row_stamps() {
	// #time follows the at column through updates; the backfill must carry the stored value, not recompute it.
	keeps_the_row_stamps("bf::esrc");
}

fn create_chain(t: &TestEngine, first: &str, second: &str, upstream: &str) {
	create(t, first, COLUMNS, &format!("FROM {upstream} | filter {{ v > 10 }}"));
	create(t, second, "id: int4, v: int4", &format!("FROM {first} | filter {{ v < 90 }} | map {{ id, v }}"));
}

#[test]
fn a_late_chain_equals_the_early_chain() {
	// The second late view must backfill from the rows the first late view's own backfill wrote.
	let t = engine();
	create_chain(&t, "bf::early1", "bf::early2", "bf::src");
	for rql in history("bf::src") {
		write(&t, &rql);
	}
	assert_eq!(ids(&t.query("FROM bf::early2")), vec![1, 4, 6, 7]);
	create_chain(&t, "bf::late1", "bf::late2", "bf::src");
	let twins = [("bf::early1", "bf::late1"), ("bf::early2", "bf::late2")];
	agree(&t, &twins, &[rows], "right after the late creates");
	for rql in later("bf::src") {
		write(&t, &rql);
		agree(&t, &twins, &[rows], &format!("after: {rql}"));
	}
}

#[test]
fn a_late_view_over_an_early_view_equals_the_early_chain() {
	// Backfilling from a transactional view must read its live-maintained rows, not the table behind it.
	let t = engine();
	create_chain(&t, "bf::early1", "bf::early2", "bf::src");
	for rql in history("bf::src") {
		write(&t, &rql);
	}
	create(&t, "bf::late2", "id: int4, v: int4", "FROM bf::early1 | filter { v < 90 } | map { id, v }");
	agree(&t, &[("bf::early2", "bf::late2")], &[rows], "right after the late create");
	for rql in later("bf::src") {
		write(&t, &rql);
		agree(&t, &[("bf::early2", "bf::late2")], &[rows], &format!("after: {rql}"));
	}
}

#[test]
fn a_late_view_over_an_empty_or_emptied_table_starts_empty_and_follows_later_writes() {
	// An empty snapshot, and one made only of deleted rows, must neither fail the create nor stop live upkeep.
	let t = engine();
	create(&t, "bf::early", COLUMNS, "FROM bf::src");
	create(&t, "bf::never", COLUMNS, "FROM bf::src");
	assert!(ids(&t.query("FROM bf::never")).is_empty());
	for rql in history("bf::src") {
		write(&t, &rql);
	}
	write(&t, "DELETE bf::src FILTER { id > 0 }");
	create(&t, "bf::emptied", COLUMNS, "FROM bf::src");
	assert!(ids(&t.query("FROM bf::emptied")).is_empty());
	let twins = [("bf::early", "bf::never"), ("bf::early", "bf::emptied")];
	for rql in later("bf::src") {
		write(&t, &rql);
		agree(&t, &twins, &[rows], &format!("after: {rql}"));
	}
	assert_eq!(ids(&t.query("FROM bf::emptied")), vec![9]);
}

#[test]
fn a_late_view_over_a_source_many_times_the_batch_size_is_complete() {
	// At batch size 1 every row is its own chunk; a chunk dropped or fed twice shows as a missing or extra row.
	let t = engine_with_batch(1);
	late_equals_early(&t, COLUMNS, "FROM bf::src", &["bf::src"], bulk, &bulk_ids(), &[rows, stamps]);
}

#[test]
fn a_late_filter_view_over_a_source_that_is_not_a_multiple_of_the_batch_is_complete() {
	// 16 live rows at batch size 3 end in a short chunk, which must be fed like any other.
	let t = engine_with_batch(3);
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | filter { v > 20 }",
		&["bf::src"],
		bulk,
		&[8, 9, 10, 11, 12, 13, 14],
		&[rows],
	);
}

#[test]
fn a_late_append_over_two_sources_larger_than_the_batch_is_complete() {
	// Chunks of the second source must not reset or reuse the first source's lane row numbers.
	let t = engine_with_batch(1);
	let want: Vec<i32> = bulk_ids().into_iter().flat_map(|id| [id, id]).collect();
	late_equals_early(
		&t,
		COLUMNS,
		"FROM bf::src | append { FROM bf::src2 }",
		&["bf::src", "bf::src2"],
		bulk,
		&want,
		&[rows],
	);
}

#[test]
fn a_late_chain_over_a_source_larger_than_the_batch_is_complete() {
	// A view source is scanned in chunks too; the second hop must see every row the first hop backfilled.
	let t = engine_with_batch(1);
	create_chain(&t, "bf::early1", "bf::early2", "bf::src");
	for rql in bulk("bf::src") {
		write(&t, &rql);
	}
	assert_eq!(ids(&t.query("FROM bf::early2")), vec![3, 4, 8, 9, 10, 11, 12, 13, 14]);
	create_chain(&t, "bf::late1", "bf::late2", "bf::src");
	let twins = [("bf::early1", "bf::late1"), ("bf::early2", "bf::late2")];
	agree(&t, &twins, &[rows], "right after the late creates");
	for rql in later("bf::src") {
		write(&t, &rql);
		agree(&t, &twins, &[rows], &format!("after: {rql}"));
	}
}

#[test]
fn a_late_create_commits_its_backfill_in_one_version_and_leaves_other_views_alone() {
	// A side commit adds a version; running every flow on the snapshot puts the early view in the CDC.
	let t = engine();
	create(&t, "bf::early", COLUMNS, "FROM bf::src | filter { v > 50 }");
	for rql in history("bf::src") {
		write(&t, &rql);
	}
	let before = t.current_version().expect("current version");
	create(&t, "bf::late", COLUMNS, "FROM bf::src | filter { v > 50 }");
	let created = t.current_version().expect("current version");
	assert_eq!(created, CommitVersion(before.0 + 1), "the create and its backfill must be one commit");
	let counts = inserted_rows_per_object(&t, &cdc_at(&t, created));
	assert_eq!(counts, BTreeMap::from([(view_object(&t, "late"), 3)]));
}

#[test]
fn rows_written_earlier_in_the_create_txn_land_in_the_view_exactly_once() {
	// The scan sees the txn's own pending rows; feeding them again from the txn's change list would double them.
	let t = seeded_with_an_early_view();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(&mut txn, "INSERT bf::src [{ id: 4, v: 40 }, { id: 5, v: 50 }]");
	in_txn(&mut txn, "UPDATE bf::src { v: 11 } FILTER { id == 1 }");
	in_txn(&mut txn, "CREATE TRANSACTIONAL VIEW bf::late { id: int4, v: int4 } AS { FROM bf::src }");
	let want = vec![(1, 11), (2, 20), (3, 30), (4, 40), (5, 50)];
	let inside = in_txn(&mut txn, "FROM bf::late");
	assert_eq!(pairs(&inside), want, "a read later in the create txn must see the backfilled rows");
	assert_eq!(rows(&inside), rows(&in_txn(&mut txn, "FROM bf::early")));
	txn.commit().unwrap();
	let late = t.query("FROM bf::late");
	assert_eq!(pairs(&late), want);
	assert_eq!(distinct_rownums(&late), want.len());
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after the create txn committed");
}

#[test]
fn dml_after_the_create_in_the_same_txn_reaches_the_view_exactly_once() {
	// Writes after the create must reach the new flow exactly once, on top of the backfill, old rows or new.
	let t = seeded_with_an_early_view();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(&mut txn, "INSERT bf::src [{ id: 4, v: 40 }]");
	in_txn(&mut txn, "CREATE TRANSACTIONAL VIEW bf::late { id: int4, v: int4 } AS { FROM bf::src }");
	in_txn(&mut txn, "INSERT bf::src [{ id: 6, v: 60 }]");
	in_txn(&mut txn, "UPDATE bf::src { v: 41 } FILTER { id == 4 }");
	in_txn(&mut txn, "UPDATE bf::src { v: 21 } FILTER { id == 2 }");
	in_txn(&mut txn, "DELETE bf::src FILTER { id == 3 }");
	let want = vec![(1, 10), (2, 21), (4, 41), (6, 60)];
	let inside = in_txn(&mut txn, "FROM bf::late");
	assert_eq!(pairs(&inside), want);
	assert_eq!(rows(&inside), rows(&in_txn(&mut txn, "FROM bf::early")));
	txn.commit().unwrap();
	let late = t.query("FROM bf::late");
	assert_eq!(pairs(&late), want);
	assert_eq!(distinct_rownums(&late), want.len());
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after the create txn committed");
	t.command("UPDATE bf::src { v: 42 } FILTER { id == 4 }");
	assert_eq!(pairs(&t.query("FROM bf::late")), vec![(1, 10), (2, 21), (4, 42), (6, 60)]);
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after a later commit");
}

#[test]
fn a_row_deleted_before_the_create_in_the_same_txn_stays_out_of_the_view() {
	// The snapshot is the txn's own view of the table, so rows it already deleted must never reach the view.
	let t = seeded_with_an_early_view();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(&mut txn, "INSERT bf::src [{ id: 4, v: 40 }]");
	in_txn(&mut txn, "DELETE bf::src FILTER { id == 4 }");
	in_txn(&mut txn, "DELETE bf::src FILTER { id == 2 }");
	in_txn(&mut txn, "CREATE TRANSACTIONAL VIEW bf::late { id: int4, v: int4 } AS { FROM bf::src }");
	assert_eq!(pairs(&in_txn(&mut txn, "FROM bf::late")), vec![(1, 10), (3, 30)]);
	txn.commit().unwrap();
	assert_eq!(pairs(&t.query("FROM bf::late")), vec![(1, 10), (3, 30)]);
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after the create txn committed");
}

#[test]
fn rolling_back_the_create_txn_leaves_no_view_no_view_rows_and_no_source_rows() {
	// Backfilled rows live in the create txn's write set; anything written beside it survives the rollback.
	let t = seeded_with_an_early_view();
	let before = t.current_version().expect("current version");
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(&mut txn, "INSERT bf::src [{ id: 4, v: 40 }]");
	in_txn(&mut txn, "CREATE TRANSACTIONAL VIEW bf::late { id: int4, v: int4 } AS { FROM bf::src }");
	assert_eq!(
		ids(&in_txn(&mut txn, "FROM bf::late")),
		vec![1, 2, 3, 4],
		"the backfill must have run before the rollback"
	);
	txn.rollback().unwrap();
	assert_eq!(t.current_version().expect("current version"), before, "a rolled back create must commit nothing");
	assert_eq!(named(&t, "views", "late"), 0);
	assert_eq!(named(&t, "flows", "late"), 0);
	assert_eq!(pairs(&t.query("FROM bf::src")), vec![(1, 10), (2, 20), (3, 30)]);
	assert_eq!(pairs(&t.query("FROM bf::early")), vec![(1, 10), (2, 20), (3, 30)]);
	t.admin("CREATE TRANSACTIONAL VIEW bf::late { id: int4, v: int4 } AS { FROM bf::src }");
	assert_eq!(pairs(&t.query("FROM bf::late")), vec![(1, 10), (2, 20), (3, 30)]);
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after the create was redone");
}

#[test]
fn a_chain_created_over_existing_rows_in_one_txn_and_written_after_holds_each_row_once() {
	// The second create scans the first view's pending rows; the next write must not feed them to it a second time.
	let t = seeded_with_an_early_view();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(
		&mut txn,
		"CREATE TRANSACTIONAL VIEW bf::late1 { id: int4, v: int4 } AS { FROM bf::src | filter { v > 10 } }",
	);
	in_txn(
		&mut txn,
		"CREATE TRANSACTIONAL VIEW bf::late2 { id: int4, v: int4 } AS { FROM bf::late1 | filter { v < 30 } }",
	);
	assert_eq!(pairs(&in_txn(&mut txn, "FROM bf::late2")), vec![(2, 20)]);
	in_txn(&mut txn, "INSERT bf::src [{ id: 4, v: 25 }]");
	in_txn(&mut txn, "UPDATE bf::src { v: 15 } FILTER { id == 3 }");
	assert_eq!(pairs(&in_txn(&mut txn, "FROM bf::late1")), vec![(2, 20), (3, 15), (4, 25)]);
	assert_eq!(pairs(&in_txn(&mut txn, "FROM bf::late2")), vec![(2, 20), (3, 15), (4, 25)]);
	txn.commit().unwrap();
	let late2 = t.query("FROM bf::late2");
	assert_eq!(pairs(&late2), vec![(2, 20), (3, 15), (4, 25)]);
	assert_eq!(distinct_rownums(&late2), 3);
	t.command("DELETE bf::src FILTER { id == 2 }");
	assert_eq!(pairs(&t.query("FROM bf::late1")), vec![(3, 15), (4, 25)]);
	assert_eq!(pairs(&t.query("FROM bf::late2")), vec![(3, 15), (4, 25)]);
}

fn partitioned_with_an_early_view() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE bf");
	t.admin("CREATE TABLE bf::p { id: int4, pool: utf8 } WITH { partition: { by: { pool } } }");
	t.admin("CREATE TRANSACTIONAL VIEW bf::early { id: int4, pool: utf8 } AS { FROM bf::p }");
	t.command("INSERT bf::p [{ id: 1, pool: 'a' }, { id: 2, pool: 'b' }, { id: 3, pool: 'b' }]");
	t.command("INSERT bf::p [{ id: 4, pool: 'a' }]");
	t
}

fn sorted(frames: &[Frame]) -> Vec<String> {
	let mut rows = rows(frames);
	rows.sort();
	rows
}

fn create_after_a_partition_removal_in_one_txn(removal: &str, after: Option<&str>, want: &[i32]) {
	let t = partitioned_with_an_early_view();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	in_txn(&mut txn, &format!("ALTER TABLE bf::p {removal} PARTITION {{ pool: 'a' }}"));
	in_txn(&mut txn, "CREATE TRANSACTIONAL VIEW bf::late { id: int4, pool: utf8 } AS { FROM bf::p }");
	if let Some(rql) = after {
		in_txn(&mut txn, rql);
	}
	let inside = sorted(&in_txn(&mut txn, "FROM bf::p"));
	assert_eq!(sorted(&in_txn(&mut txn, "FROM bf::late")), inside, "the late view is off inside the create txn");
	assert_eq!(sorted(&in_txn(&mut txn, "FROM bf::early")), inside, "the early view is off inside the create txn");
	txn.commit().unwrap();
	let source = t.query("FROM bf::p");
	assert_eq!(ids(&source), want, "the table is off after the create txn committed");
	assert_eq!(sorted(&t.query("FROM bf::late")), sorted(&source), "the late view must equal the table");
	assert_eq!(sorted(&t.query("FROM bf::early")), sorted(&source), "the early view must lose the removed rows");
	agree(&t, &[("bf::early", "bf::late")], &[rows], "after the create txn committed");
}

#[test]
fn a_create_after_a_truncate_partition_in_the_same_txn_leaves_the_early_view_without_the_truncated_rows() {
	// Without a flush before the create, the cursor jump skips the truncate and the early view keeps ids 1 and 4.
	create_after_a_partition_removal_in_one_txn("TRUNCATE", None, &[2, 3]);
}

#[test]
fn a_create_after_a_truncate_partition_and_before_an_insert_in_the_same_txn_keeps_every_view_exact() {
	// The later insert syncs from the cursor, so a jump past the pending truncate still leaves ids 1 and 4 behind.
	create_after_a_partition_removal_in_one_txn(
		"TRUNCATE",
		Some("INSERT bf::p [{ id: 5, pool: 'a' }, { id: 6, pool: 'b' }]"),
		&[2, 3, 5, 6],
	);
}

#[test]
fn a_create_after_a_drop_partition_in_the_same_txn_leaves_the_early_view_without_the_dropped_rows() {
	// Dropping the partition removes its rows like a truncate; skipping them keeps ids 1 and 4 in the early view.
	create_after_a_partition_removal_in_one_txn("DROP", None, &[2, 3]);
}

#[test]
fn a_create_after_a_drop_partition_and_before_an_insert_in_the_same_txn_keeps_every_view_exact() {
	// A row written back into the dropped partition must reach both views once, and the dropped rows neither.
	create_after_a_partition_removal_in_one_txn(
		"DROP",
		Some("INSERT bf::p [{ id: 5, pool: 'a' }, { id: 6, pool: 'b' }]"),
		&[2, 3, 5, 6],
	);
}

#[cfg(feature = "testing")]
mod scan_hooks {
	use std::sync::Arc;

	use reifydb_core::interface::catalog::object::ObjectId;
	use reifydb_flow::backfill::testing::{InstalledScanHooks, Outcome, ScanHooks};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		error::{Diagnostic, Error},
		params::Params,
		value::identity::IdentityId,
	};

	use super::{
		COLUMNS, agree, bulk, create, engine, engine_with_batch, history, ids, named, rows, view_object, write,
	};

	const REFUSED: &str = "TEST_BACKFILL_REFUSED";

	enum Refuse {
		Never,
		Open,
		PullFrom(u64),
	}

	struct Recorder {
		refuse: Refuse,
		opens: Mutex<Vec<ObjectId>>,
		pulls: Mutex<Vec<(ObjectId, u64)>>,
	}

	impl ScanHooks for Recorder {
		fn on_open(&self, source: ObjectId) -> Outcome {
			self.opens.lock().push(source);
			match self.refuse {
				Refuse::Open => Outcome::Err(refused()),
				_ => Outcome::Land,
			}
		}

		fn on_next(&self, source: ObjectId, pull: u64) -> Outcome {
			self.pulls.lock().push((source, pull));
			match self.refuse {
				Refuse::PullFrom(from) if pull >= from => Outcome::Err(refused()),
				_ => Outcome::Land,
			}
		}
	}

	fn refused() -> Error {
		Error(Box::new(Diagnostic {
			code: REFUSED.to_string(),
			message: "refused backfill scan".to_string(),
			..Default::default()
		}))
	}

	fn has_code(error: &Error, code: &str) -> bool {
		let mut diagnostic = Some(&**error);
		while let Some(current) = diagnostic {
			if current.code == code {
				return true;
			}
			diagnostic = current.cause.as_deref();
		}
		false
	}

	fn install(t: &TestEngine, refuse: Refuse) -> Arc<Recorder> {
		let recorder = Arc::new(Recorder {
			refuse,
			opens: Mutex::new(Vec::new()),
			pulls: Mutex::new(Vec::new()),
		});
		t.ioc().register_service(InstalledScanHooks(recorder.clone()));
		recorder
	}

	fn table_object(t: &TestEngine, name: &str) -> ObjectId {
		let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
		let mut txn = Transaction::Query(&mut query);
		let catalog = t.catalog();
		let namespace = catalog
			.find_namespace_by_name(&mut txn, "bf")
			.expect("namespace lookup")
			.expect("namespace bf");
		let table = catalog
			.find_table_by_name(&mut txn, namespace.id(), name)
			.expect("table lookup")
			.expect("table");
		ObjectId::Table(table.id)
	}

	fn refused_create(t: &TestEngine, refuse: Refuse) {
		let before = t.current_version().expect("current version");
		let recorder = install(t, refuse);
		let r = t.inner().admin_as(
			IdentityId::system(),
			&format!("CREATE TRANSACTIONAL VIEW bf::late {{ {COLUMNS} }} AS {{ FROM bf::src }}"),
			Params::None,
		);
		let error = r.error.expect("a refused scan must fail the create");
		assert!(has_code(&error, REFUSED), "the create failed with a different error: {error:?}");
		assert!(!recorder.opens.lock().is_empty(), "the refusing hook never ran");
		assert_eq!(
			t.current_version().expect("current version"),
			before,
			"a failed create must commit nothing"
		);
		assert_eq!(named(t, "views", "late"), 0);
		assert_eq!(named(t, "flows", "late"), 0);
		assert_eq!(ids(&t.query("FROM bf::src")), vec![1, 2, 4, 6, 7]);
		install(t, Refuse::Never);
		create(t, "bf::late", COLUMNS, "FROM bf::src");
		agree(t, &[("bf::early", "bf::late")], &[rows], "after the refused create was redone");
	}

	#[test]
	fn a_late_create_scans_only_its_own_source_in_chunks_of_the_configured_batch_size() {
		// A backfill that ignores the batch setting pulls the table in a few big chunks and holds it all.
		let t = engine_with_batch(1);
		create(&t, "bf::early", COLUMNS, "FROM bf::src");
		for rql in bulk("bf::src") {
			write(&t, &rql);
		}
		let recorder = install(&t, Refuse::Never);
		create(&t, "bf::late", COLUMNS, "FROM bf::src");
		let src = table_object(&t, "src");
		assert_eq!(*recorder.opens.lock(), vec![src], "the create must scan exactly its one source, once");
		let pulls = recorder.pulls.lock().iter().filter(|(source, _)| *source == src).count();
		assert!(pulls >= 16, "16 live rows at batch size 1 need at least 16 pulls, got {pulls}");
		agree(&t, &[("bf::early", "bf::late")], &[rows], "right after the late create");
	}

	#[test]
	fn a_late_append_of_a_table_with_itself_scans_the_table_once_and_feeds_both_branches() {
		// One scan per branch would feed each branch every row twice, and upserts by row number would hide it.
		let t = engine();
		create(&t, "bf::early", COLUMNS, "FROM bf::src | append { FROM bf::src }");
		for rql in history("bf::src") {
			write(&t, &rql);
		}
		let recorder = install(&t, Refuse::Never);
		create(&t, "bf::late", COLUMNS, "FROM bf::src | append { FROM bf::src }");
		assert_eq!(*recorder.opens.lock(), vec![table_object(&t, "src")], "a table read twice is scanned once");
		assert_eq!(ids(&t.query("FROM bf::late")), vec![1, 1, 2, 2, 4, 4, 6, 6, 7, 7]);
		agree(&t, &[("bf::early", "bf::late")], &[rows], "right after the late create");
	}

	#[test]
	fn a_late_view_over_a_view_scans_that_view_and_nothing_else() {
		// Scanning the table behind the view skips its filter; scanning more feeds foreign rows.
		let t = engine();
		for rql in history("bf::src") {
			write(&t, &rql);
		}
		create(&t, "bf::late1", COLUMNS, "FROM bf::src | filter { v > 10 }");
		let recorder = install(&t, Refuse::Never);
		create(&t, "bf::late2", "id: int4, v: int4", "FROM bf::late1 | filter { v < 90 } | map { id, v }");
		assert_eq!(*recorder.opens.lock(), vec![view_object(&t, "late1")]);
		assert_eq!(ids(&t.query("FROM bf::late2")), vec![1, 4, 6, 7]);
	}

	#[test]
	fn a_scan_refused_at_open_fails_the_create_and_leaves_nothing() {
		// A create that swallows the scan error would commit an empty view that looks complete.
		let t = engine();
		create(&t, "bf::early", COLUMNS, "FROM bf::src");
		for rql in history("bf::src") {
			write(&t, &rql);
		}
		refused_create(&t, Refuse::Open);
	}

	#[test]
	fn a_scan_refused_after_the_first_chunks_fails_the_create_and_leaves_no_partial_view() {
		// Chunks already fed to the flow sit in the create txn; the failure must take them down with the view.
		let t = engine_with_batch(1);
		create(&t, "bf::early", COLUMNS, "FROM bf::src");
		for rql in history("bf::src") {
			write(&t, &rql);
		}
		refused_create(&t, Refuse::PullFrom(2));
	}
}
