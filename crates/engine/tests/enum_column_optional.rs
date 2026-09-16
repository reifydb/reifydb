// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, frame::frame::Frame};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.command("INSERT s::e [{ id: 1, status: s::status::Inactive }]");
	t
}

fn status_rows(frames: &[Frame]) -> Vec<Vec<(String, Value)>> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let columns: Vec<_> = frames[0].columns.iter().filter(|c| c.name.starts_with("status")).collect();
	assert!(!columns.is_empty(), "the status column is missing from {:?}", frames[0].columns);
	(0..columns[0].data.len())
		.map(|row| columns.iter().map(|c| (c.name.clone(), c.data.get_value(row))).collect())
		.collect()
}

#[test]
fn an_optional_enum_column_stores_and_reads_back_a_variant_and_a_none() {
	// Option(s::status) must resolve the enum as s::status does and hold a none, never fail as type not found.
	let t = engine();
	t.admin("CREATE TABLE s::o { id: int4, status: Option(s::status) }");
	t.command("INSERT s::o [{ id: 1, status: s::status::Inactive }, { id: 2, status: none }]");

	let rows = status_rows(&t.query("FROM s::o | sort { id: ASC }"));

	assert_eq!(rows.len(), 2, "both rows must be stored, got {rows:?}");
	assert_eq!(
		rows[0],
		status_rows(&t.query("FROM s::e"))[0],
		"the variant must read back as it does from a non-optional enum column"
	);
	assert!(
		rows[1].iter().all(|(_, value)| matches!(value, Value::None { .. })),
		"the none row must read back as none in every status column, got {:?}",
		rows[1]
	);
}

#[test]
fn a_none_in_a_non_optional_enum_column_is_rejected_as_a_none_not_a_missing_column() {
	// the tag column is physical, so a none must be refused for being non-optional, never reported as a typo.
	let t = engine();

	let error = t.command_err("INSERT s::e [{ id: 2, status: none }]");

	assert!(
		!error.contains("column not found"),
		"a none must not be reported as an unknown column, got {error}"
	);
	assert!(
		error.contains("non-optional"),
		"the error must say the column is not optional, got {error}"
	);
}
