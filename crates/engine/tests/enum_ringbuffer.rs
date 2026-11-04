// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, column_view::ColumnView, frame::frame::Frame, system_columns::user_columns};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Dot }");
	t.admin("CREATE TABLE s::t { id: int4, status: s::status }");
	t.admin("CREATE RINGBUFFER s::r { id: int4, status: s::status } WITH { capacity: 2 }");
	t
}

fn column_rows(frames: &[Frame], prefix: &str) -> Vec<Vec<(String, Value)>> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let columns: Vec<_> = user_columns(&frames[0].batch)
		.filter(|(field, _)| field.name().starts_with(prefix))
		.map(|(field, array)| ColumnView::try_from((array, field.as_ref())).unwrap())
		.collect();
	assert!(!columns.is_empty(), "no {prefix} column in {:?}", frames[0].batch.schema());
	(0..columns[0].len())
		.map(|row| columns.iter().map(|c| (c.field.name().clone(), c.get_value(row))).collect())
		.collect()
}

fn ids(frames: &[Frame]) -> Vec<String> {
	let column = frames[0].column("id").unwrap().expect("column id");
	let mut ids: Vec<String> = (0..column.len()).map(|row| column.get_value(row).to_string()).collect();
	ids.sort();
	ids
}

#[test]
fn a_ringbuffer_enum_column_reads_back_exactly_as_a_table_enum_column_does() {
	// Without the tag column the variant cannot be stored, so a ringbuffer must match a table row for row.
	let t = engine();
	t.command("INSERT s::t [{ id: 1, status: 'inactive' }]");
	t.command("INSERT s::r [{ id: 1, status: 'inactive' }]");

	let from_table = column_rows(&t.query("FROM s::t"), "status");
	let from_ring = column_rows(&t.query("FROM s::r"), "status");

	assert_eq!(from_ring, from_table, "the ringbuffer must store the variant as the table does");
}

#[test]
fn a_ringbuffer_enum_field_is_stored_in_its_own_typed_column() {
	// Without the field columns the variant's data has nowhere to go and the insert must not pass silently.
	let t = engine();
	t.admin("CREATE RINGBUFFER s::g { id: int4, shape: s::shape } WITH { capacity: 2 }");
	t.command("INSERT s::g [{ id: 1, shape: 'circle', shape_circle_radius: 0.5 }]");

	let rows = column_rows(&t.query("FROM s::g"), "shape_circle_radius");

	assert_eq!(rows.len(), 1, "the row must be stored, got {rows:?}");
	assert_eq!(rows[0][0].1, Value::float8(0.5), "the field must read back as written");
}

#[test]
fn an_optional_ringbuffer_enum_column_holds_a_variant_and_a_none() {
	// Option must resolve the enum as the plain form does, and a none must read back as none.
	let t = engine();
	t.admin("CREATE RINGBUFFER s::o { id: int4, status: Option(s::status) } WITH { capacity: 2 }");
	t.command("INSERT s::o [{ id: 1, status: 'inactive' }, { id: 2, status: none }]");

	let rows = column_rows(&t.query("FROM s::o | sort { id: ASC }"), "status");

	assert_eq!(rows.len(), 2, "both rows must be stored, got {rows:?}");
	assert!(
		!matches!(rows[0][0].1, Value::None { .. }),
		"the variant must not read back as none, got {:?}",
		rows[0]
	);
	assert!(matches!(rows[1][0].1, Value::None { .. }), "the none must read back as none, got {:?}", rows[1]);
}

#[test]
fn filtering_a_ringbuffer_by_variant_name_keeps_only_that_variant() {
	// The scan must mark the tag as an enum, otherwise a variant name compares against a bare number.
	let t = engine();
	t.command("INSERT s::r [{ id: 1, status: 'active' }, { id: 2, status: 'inactive' }]");

	assert_eq!(ids(&t.query("FROM s::r | filter { status == 'active' }")), vec!["1"]);
	assert_eq!(ids(&t.query("FROM s::r | filter { status == 'inactive' }")), vec!["2"]);
}

#[test]
fn a_ringbuffer_with_an_enum_column_still_evicts_at_capacity() {
	// The extra physical columns must not change the row count the ring enforces.
	let t = engine();
	t.command("INSERT s::r [{ id: 1, status: 'active' }]");
	t.command("INSERT s::r [{ id: 2, status: 'inactive' }]");
	t.command("INSERT s::r [{ id: 3, status: 'active' }]");

	assert_eq!(ids(&t.query("FROM s::r")), vec!["2", "3"]);
}

#[test]
fn creating_a_ringbuffer_with_an_unknown_enum_is_refused() {
	// A missing enum must fail the create, never fall through to an untyped column.
	let t = engine();

	let error = t.admin_err("CREATE RINGBUFFER s::u { id: int4, status: s::missing } WITH { capacity: 2 }");

	assert!(error.contains("missing"), "the error must name the missing enum, got {error}");
}
