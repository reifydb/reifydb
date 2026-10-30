// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{ExportOptions, ObjectKind, testing::db::TestDb};
use reifydb_test_harness::assert::{assert_same_rows, rows};

fn prepare(setup: &str) -> (TestDb, TestDb, String) {
	let a = TestDb::memory();
	a.admin(setup);
	let dump = a.export(&ExportOptions::all()).expect("export failed");
	let b = TestDb::memory();
	if let Err(e) = b.import(&dump) {
		panic!("import failed: {e}\n=== DUMP ===\n{dump}\n=== END DUMP ===");
	}
	(a, b, dump)
}

#[test]
fn primitives_and_temporals_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE TABLE rt::prims {
    id: int4, f4: float4, f8: float8, big: int16, txt: utf8, flag: bool,
    d: date, t: time, dt: datetime, dur: duration, bl: blob, dec: decimal(12,3),
    note: option(utf8)
};
INSERT rt::prims [
    { id: 1, f4: 1.5, f8: -2.5, big: 1234567890123456789, txt: 'hi', flag: true,
      d: @2024-03-15, t: @14:30:15.123456789, dt: @2024-03-15T14:30:00Z, dur: @PT1H30M,
      bl: blob::hex('deadbeef'), dec: '123.456', note: 'present' },
    { id: 2, f4: 0, f8: 0, big: -5, txt: "has ' apostrophe", flag: false,
      d: @2000-01-01, t: @00:00:00, dt: @2020-01-01T00:00:00Z, dur: @P1Y2M3D,
      bl: blob::hex('00'), dec: '0.000', note: none }
];
"#;
	let (a, b, _dump) = prepare(setup);
	// assert_same_rows sorts both sides deterministically and compares with the database's own
	// Value equality, the correct notion of "same data" (e.g. decimal 0.000 == 0).
	assert_same_rows(&a.query("from rt::prims"), &b.query("from rt::prims"));
}

#[test]
fn dictionary_column_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE DICTIONARY rt::tokens FOR utf8 AS uint4;
CREATE TABLE rt::events { id: int4, sym: utf8 with { dictionary: rt::tokens } };
INSERT rt::events [{ id: 1, sym: 'AAPL' }, { id: 2, sym: 'AAPL' }, { id: 3, sym: 'MSFT' }];
"#;
	let (a, b, dump) = prepare(setup);
	assert!(dump.contains("CREATE DICTIONARY rt::tokens FOR utf8 AS uint4"), "dump:\n{dump}");
	assert!(dump.contains("dictionary: rt::tokens"), "dump:\n{dump}");
	assert_same_rows(&a.query("from rt::events"), &b.query("from rt::events"));
}

#[test]
fn ringbuffer_wrap_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE RINGBUFFER rt::rb { id: int4, msg: utf8 } WITH { capacity: 3 };
INSERT rt::rb [{ id: 1, msg: 'a' }, { id: 2, msg: 'b' }, { id: 3, msg: 'c' }, { id: 4, msg: 'd' }, { id: 5, msg: 'e' }];
"#;
	let (a, b, dump) = prepare(setup);
	let a_rows = rows(&a.query("from rt::rb"));
	assert_eq!(a_rows.len(), 3, "ring buffer should retain only capacity rows; dump:\n{dump}");
	assert_eq!(a_rows, rows(&b.query("from rt::rb")));
}

#[test]
fn a_wrapped_ringbuffer_pushes_out_the_same_oldest_row_after_a_roundtrip() {
	// Export must write ring rows oldest first, otherwise the re-imported ring evicts its newest row next.
	let setup = r#"
CREATE NAMESPACE rt;
CREATE RINGBUFFER rt::rb { id: int4, msg: utf8 } WITH { capacity: 3 };
INSERT rt::rb [{ id: 1, msg: 'a' }, { id: 2, msg: 'b' }, { id: 3, msg: 'c' }, { id: 4, msg: 'd' }, { id: 5, msg: 'e' }];
"#;
	let (a, b, dump) = prepare(setup);
	a.admin("INSERT rt::rb [{ id: 6, msg: 'f' }]");
	b.admin("INSERT rt::rb [{ id: 6, msg: 'f' }]");
	let ids = "from rt::rb sort { id: asc } map { id }";
	assert_eq!(rows(&a.query(ids)), rows(&b.query(ids)), "dump:\n{dump}");
	assert_eq!(rows(&b.query(ids)).len(), 3, "dump:\n{dump}");
}

#[test]
fn series_datetime_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE SERIES rt::metrics { ts: datetime, v: int4 } WITH { key: ts, precision: millisecond };
INSERT rt::metrics [
    { ts: @2024-01-01T00:00:00Z, v: 10 },
    { ts: @2024-01-01T00:00:01Z, v: 20 },
    { ts: @2024-01-01T00:00:02Z, v: 30 }
];
"#;
	let (a, b, dump) = prepare(setup);
	let a_rows = rows(&a.query("from rt::metrics"));
	assert!(!a_rows.is_empty(), "series should have rows; dump:\n{dump}");
	assert_eq!(a_rows, rows(&b.query("from rt::metrics")), "dump:\n{dump}");
}

#[test]
fn enum_type_definition_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE ENUM rt::status { Active, Inactive, Pending };
"#;
	let (_a, _b, dump) = prepare(setup);
	// The catalog stores enum variant names lowercased; export reproduces stored form,
	// and prepare() already asserts the dump re-imports cleanly.
	assert!(dump.contains("CREATE ENUM rt::status { active, inactive, pending }"), "dump:\n{dump}");
}

#[test]
fn enum_column_roundtrip() {
	// An enum column's logical name and its values must survive export -> import. Expected to
	// fail until export reconstructs enum columns from their physical tag columns; do not weaken
	// or #[ignore] it to make the suite green.
	let setup = r#"
CREATE NAMESPACE rt;
CREATE ENUM rt::status { Active, Inactive };
CREATE TABLE rt::items { id: int4, state: rt::status };
INSERT rt::items [
    { id: 1, state: 'active' },
    { id: 2, state: 'inactive' },
    { id: 3, state: 'active' }
];
"#;
	let a = TestDb::memory();
	a.admin(setup);
	let dump = a.export(&ExportOptions::all()).expect("export failed");

	assert!(
		dump.contains("state: rt::status"),
		"export rendered the physical tag column instead of the logical enum column;\ndump:\n{dump}"
	);

	let b = TestDb::memory();
	b.import(&dump).unwrap_or_else(|e| panic!("import of the enum-column dump failed: {e}\ndump:\n{dump}"));

	assert_same_rows(&a.query("from rt::items"), &b.query("from rt::items"));
}

#[test]
fn enum_structured_column_roundtrip() {
	// A variant with fields must round-trip its name and flat fields, not just the tag.
	let setup = r#"
CREATE NAMESPACE rt;
CREATE ENUM rt::shape { Circle { radius: float8 }, Rectangle { width: float8, height: float8 } };
CREATE TABLE rt::shapes { id: int4, shape: rt::shape };
INSERT rt::shapes [
    { id: 1, shape: 'circle', shape_circle_radius: 5.0 },
    { id: 2, shape: 'rectangle', shape_rectangle_width: 3.0, shape_rectangle_height: 4.0 },
    { id: 3, shape: 'circle', shape_circle_radius: 10.0 }
];
"#;
	let (a, b, dump) = prepare(setup);
	assert!(dump.contains("shape: rt::shape"), "logical enum column expected;\ndump:\n{dump}");
	assert!(
		dump.contains("shape: 'circle', shape_circle_radius: 5"),
		"name and flat field expected;\ndump:\n{dump}"
	);
	assert!(
		dump.contains("shape: 'rectangle', shape_rectangle_width: 3, shape_rectangle_height: 4"),
		"name and flat fields expected;\ndump:\n{dump}"
	);
	assert_same_rows(&a.query("from rt::shapes"), &b.query("from rt::shapes"));
}

#[test]
fn schema_only_and_data_only() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE TABLE rt::t { id: int4, name: utf8 };
INSERT rt::t [{ id: 1, name: 'x' }];
"#;
	let a = TestDb::memory();
	a.admin(setup);

	let schema = a.export(&ExportOptions::all().schema_only()).expect("schema export");
	assert!(schema.contains("CREATE TABLE rt::t"), "{schema}");
	assert!(!schema.contains("INSERT rt::t"), "schema-only must not contain inserts:\n{schema}");

	let data = a.export(&ExportOptions::all().data_only()).expect("data export");
	assert!(data.contains("INSERT rt::t"), "{data}");
	assert!(!data.contains("CREATE TABLE"), "data-only must not contain DDL:\n{data}");

	let b = TestDb::memory();
	b.import(&schema).expect("import schema");
	b.import(&data).expect("import data");
	assert_same_rows(&a.query("from rt::t"), &b.query("from rt::t"));
}

#[test]
fn selection_by_kind_excludes_other_kinds() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE TABLE rt::t { id: int4 };
CREATE RINGBUFFER rt::rb { id: int4 } WITH { capacity: 4 };
"#;
	let a = TestDb::memory();
	a.admin(setup);
	let dump = a.export(&ExportOptions::all().kind(ObjectKind::Table)).expect("export");
	assert!(dump.contains("CREATE TABLE rt::t"), "{dump}");
	assert!(!dump.contains("CREATE RINGBUFFER"), "kind=Table must exclude ring buffers:\n{dump}");
}
