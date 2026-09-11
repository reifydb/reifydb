// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	column.data.get_type()
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int4 }");
	t.admin("CREATE TABLE test::optional { x: Option(int4) }");
	t
}

#[test]
fn empty_table_scan_keeps_the_declared_plain_type() {
	// A client checks the wire type against the schema, so an empty result must not widen int4 to Option(int4).
	let t = engine();

	let frames = t.query("FROM test::items");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}

#[test]
fn map_over_an_empty_table_keeps_the_declared_plain_type() {
	// The projection forwards whatever buffer the scan built, so it must see the same type a populated scan gives.
	let t = engine();

	let frames = t.query("FROM test::items | map { id }");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}

#[test]
fn empty_deferred_view_scan_keeps_the_declared_plain_type() {
	// Views take a different empty-batch constructor than tables and must agree with them.
	let t = engine();
	t.admin("CREATE DEFERRED VIEW test::v { id: int4 } AS { FROM test::items }");

	let frames = t.query("FROM test::v");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}

#[test]
fn empty_ringbuffer_scan_keeps_the_declared_plain_type() {
	// The ring buffer scan builds its empty batch the same way the table scan does.
	let t = engine();
	t.admin("CREATE RINGBUFFER test::rb { id: int4 } WITH { capacity: 4 }");

	let frames = t.query("FROM test::rb");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}

#[test]
fn empty_queue_scan_keeps_the_declared_plain_type() {
	// The queue scan has its own copy of the empty-batch helper and must agree with the table scan.
	let t = engine();
	t.admin("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	let frames = t.query("FROM test::jobs");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}

#[test]
fn empty_table_scan_keeps_an_optional_column_optional() {
	// The fix for plain columns must not strip the Option wrapper from a column that really is optional.
	let t = engine();

	let frames = t.query("FROM test::optional");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "x"), ValueType::Option(Box::new(ValueType::Int4)));
}

#[test]
fn populated_table_scan_reports_the_same_types_as_the_empty_one() {
	// The populated path is the reference the empty path is being aligned to, so pin it here.
	let t = engine();
	t.command("INSERT test::items [{id: 1}]");
	t.command("INSERT test::optional [{x: 1}]");

	assert_eq!(column_type(&t.query("FROM test::items"), "id"), ValueType::Int4);
	assert_eq!(column_type(&t.query("FROM test::optional"), "x"), ValueType::Option(Box::new(ValueType::Int4)));
}

fn notes() -> TestEngine {
	// A populated table so every empty result below comes from a node dropping rows, not from an empty scan.
	let t = engine();
	t.admin("CREATE TABLE test::notes { id: int4, label: Option(utf8) }");
	t.command("INSERT test::notes [{id: 1, label: 'one'}, {id: 2, label: 'two'}]");
	t
}

fn assert_notes_types(frames: &[Frame]) {
	assert_eq!(TestEngine::row_count(frames), 0);
	assert_eq!(column_type(frames, "id"), ValueType::Int4);
	assert_eq!(column_type(frames, "label"), ValueType::Option(Box::new(ValueType::Utf8)));
}

#[test]
fn filter_matching_nothing_keeps_the_declared_types() {
	// The filter consumed a typed batch and dropped every row; the fallback that types every
	// column Option(Boolean) must not be what reaches the client.
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter id == 3"));
}

#[test]
fn filter_over_an_empty_table_keeps_the_declared_types() {
	// The scan hands the filter one empty typed batch; the filter must forward it, not swallow it.
	let t = engine();
	t.admin("CREATE TABLE test::notes { id: int4, label: Option(utf8) }");

	assert_notes_types(&t.query("FROM test::notes | filter id == 3"));
}

#[test]
fn map_after_a_filter_matching_nothing_evaluates_on_zero_rows() {
	// A projection over the forwarded empty batch produces its real output types.
	let t = notes();

	let populated = t.query("FROM test::notes | map { label, doubled: id * 2 }");
	let frames = t.query("FROM test::notes | filter id == 3 | map { label, doubled: id * 2 }");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "label"), ValueType::Option(Box::new(ValueType::Utf8)));
	assert_eq!(column_type(&frames, "doubled"), column_type(&populated, "doubled"));
}

#[test]
fn take_zero_keeps_the_declared_types() {
	// take 0 never needs a row, but it still has to say what the rows would have been.
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | take 0"));
}

#[test]
fn take_over_a_filter_matching_nothing_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter id == 3 | take 1"));
}

#[test]
fn distinct_over_a_filter_matching_nothing_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter id == 3 | distinct { id, label }"));
}

#[test]
fn sort_over_a_filter_matching_nothing_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter id == 3 | sort {id}"));
}

#[test]
fn sort_then_take_over_a_filter_matching_nothing_keeps_the_declared_types() {
	// sort followed by take compiles to the top-k node, which collects its input separately from sort.
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter id == 3 | sort {id} | take 5"));
}

#[test]
fn sort_then_take_zero_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | sort {id} | take 0"));
}

#[test]
fn aggregate_group_key_over_a_filter_matching_nothing_keeps_the_key_type() {
	// With no groups the key column has no values to take a type from; it must come from the input batch.
	let t = notes();

	let frames = t.query("FROM test::notes | filter id == 3 | aggregate { total: math::count(id) } by { label }");

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "label"), ValueType::Option(Box::new(ValueType::Utf8)));
}

#[test]
fn row_point_lookup_missing_the_row_keeps_the_declared_types() {
	// A #rownum predicate plans as a point lookup instead of a filter and must agree with it.
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter #rownum == 999"));
}

#[test]
fn row_list_lookup_missing_every_row_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter #rownum in [998, 999]"));
}

#[test]
fn row_range_scan_missing_every_row_keeps_the_declared_types() {
	let t = notes();

	assert_notes_types(&t.query("FROM test::notes | filter #rownum between 998 and 999"));
}

#[test]
fn udf_in_a_map_after_a_filter_matching_nothing_evaluates_on_zero_rows() {
	// The udf node skips its calls on an empty batch, so the projection must still find its columns.
	let t = notes();

	let frames = t.query(
		"UDF twice ($x: int4) { RETURN $x * 2 }; FROM test::notes | filter id == 3 | map { id, r: twice(id) }",
	);

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(column_type(&frames, "id"), ValueType::Int4);
}
