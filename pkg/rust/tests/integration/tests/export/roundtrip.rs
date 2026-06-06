// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, ExportOptions, Params, ShapeKind, Value, embedded};

fn fresh() -> Database {
	embedded::memory().build().expect("build in-memory database")
}

fn prepare(setup: &str) -> (Database, Database, String) {
	let a = fresh();
	a.admin_as_root(setup, Params::None).expect("setup failed");
	let dump = a.export(&ExportOptions::all()).expect("export failed");
	let b = fresh();
	if let Err(e) = b.import(&dump) {
		panic!("import failed: {}\n=== DUMP ===\n{}\n=== END DUMP ===", e, dump);
	}
	(a, b, dump)
}

fn rows(db: &Database, query: &str) -> Vec<Vec<(String, Value)>> {
	db.query_as_root(query, Params::None)
		.expect("query failed")
		.into_iter()
		.flat_map(|frame| frame.to_rows())
		.collect()
}

/// Order rows deterministically (by debug form) so two unordered result sets can be
/// compared positionally, while the equality assertion itself uses the database's own
/// `Value` equality - the correct notion of "same data" (e.g. decimal 0.000 == 0).
fn sorted(mut rows: Vec<Vec<(String, Value)>>) -> Vec<Vec<(String, Value)>> {
	rows.sort_by_key(|row| format!("{:?}", row));
	rows
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
	assert_eq!(sorted(rows(&a, "from rt::prims")), sorted(rows(&b, "from rt::prims")));
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
	assert_eq!(sorted(rows(&a, "from rt::events")), sorted(rows(&b, "from rt::events")));
}

#[test]
fn ringbuffer_wrap_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE RINGBUFFER rt::rb { id: int4, msg: utf8 } WITH { capacity: 3 };
INSERT rt::rb [{ id: 1, msg: 'a' }, { id: 2, msg: 'b' }, { id: 3, msg: 'c' }, { id: 4, msg: 'd' }, { id: 5, msg: 'e' }];
"#;
	let (a, b, dump) = prepare(setup);
	let a_rows = rows(&a, "from rt::rb");
	assert_eq!(a_rows.len(), 3, "ring buffer should retain only capacity rows; dump:\n{dump}");
	assert_eq!(a_rows, rows(&b, "from rt::rb"));
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
	let a_rows = rows(&a, "from rt::metrics");
	assert!(!a_rows.is_empty(), "series should have rows; dump:\n{dump}");
	assert_eq!(a_rows, rows(&b, "from rt::metrics"), "dump:\n{dump}");
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

// Regression for an export-fidelity bug with enum-typed table COLUMNS (distinct from the standalone
// CREATE ENUM covered above). The engine physically expands `state: rt::status` into a hidden tag
// column `state_tag` of type Uint1 (see crates/engine/src/vm/instruction/ddl/create/table.rs,
// `expand_sumtype_columns`). Export currently dumps that physical column verbatim - producing
// `CREATE TABLE rt::items { id: int4, state_tag: rt::status }` and `INSERT ... { state_tag: 0 }` -
// instead of reconstructing the logical `state` column and `rt::status::Active` values. On re-import
// the dumped `state_tag: rt::status` expands AGAIN into `state_tag_tag`, which the `state_tag: 0`
// insert never populates, so the import fails with CONSTRAINT_007.
//
// This test pins the INTENDED behavior: an enum column's logical name and its values must survive an
// export -> import round-trip. It is EXPECTED TO FAIL until export reconstructs enum columns from
// their physical tag/value columns. Do not weaken or #[ignore] it to make the suite green - fix the
// export instead.
#[test]
fn enum_column_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE ENUM rt::status { Active, Inactive };
CREATE TABLE rt::items { id: int4, state: rt::status };
INSERT rt::items [
    { id: 1, state: rt::status::Active },
    { id: 2, state: rt::status::Inactive },
    { id: 3, state: rt::status::Active }
];
"#;
	let a = fresh();
	a.admin_as_root(setup, Params::None).expect("setup failed");
	let dump = a.export(&ExportOptions::all()).expect("export failed");

	// The export must render the logical enum column `state`, not the physical `state_tag` tag column.
	assert!(
		dump.contains("state: rt::status"),
		"export rendered the physical tag column instead of the logical enum column;\ndump:\n{dump}"
	);

	// The dump must re-import cleanly into a fresh database.
	let b = fresh();
	b.import(&dump).unwrap_or_else(|e| panic!("import of the enum-column dump failed: {e}\ndump:\n{dump}"));

	// The enum values must round-trip unchanged.
	assert_eq!(sorted(rows(&a, "from rt::items")), sorted(rows(&b, "from rt::items")), "dump:\n{dump}");
}

// The structured-variant counterpart: an enum column whose variants carry fields expands to a tag
// column plus per-field columns (`shape_circle_radius`, ...). Export must reconstruct the logical
// column AND the full `ns::enum::variant { field: value }` constructor for each row.
#[test]
fn enum_structured_column_roundtrip() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE ENUM rt::shape { Circle { radius: float8 }, Rectangle { width: float8, height: float8 } };
CREATE TABLE rt::shapes { id: int4, shape: rt::shape };
INSERT rt::shapes [
    { id: 1, shape: rt::shape::Circle { radius: 5.0 } },
    { id: 2, shape: rt::shape::Rectangle { width: 3.0, height: 4.0 } },
    { id: 3, shape: rt::shape::Circle { radius: 10.0 } }
];
"#;
	let (a, b, dump) = prepare(setup);
	assert!(dump.contains("shape: rt::shape"), "logical enum column expected;\ndump:\n{dump}");
	assert!(dump.contains("rt::shape::circle {"), "structured constructor expected;\ndump:\n{dump}");
	assert!(dump.contains("rt::shape::rectangle {"), "structured constructor expected;\ndump:\n{dump}");
	assert_eq!(sorted(rows(&a, "from rt::shapes")), sorted(rows(&b, "from rt::shapes")), "dump:\n{dump}");
}

#[test]
fn schema_only_and_data_only() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE TABLE rt::t { id: int4, name: utf8 };
INSERT rt::t [{ id: 1, name: 'x' }];
"#;
	let a = fresh();
	a.admin_as_root(setup, Params::None).expect("setup");

	let schema = a.export(&ExportOptions::all().schema_only()).expect("schema export");
	assert!(schema.contains("CREATE TABLE rt::t"), "{schema}");
	assert!(!schema.contains("INSERT rt::t"), "schema-only must not contain inserts:\n{schema}");

	let data = a.export(&ExportOptions::all().data_only()).expect("data export");
	assert!(data.contains("INSERT rt::t"), "{data}");
	assert!(!data.contains("CREATE TABLE"), "data-only must not contain DDL:\n{data}");

	let b = fresh();
	b.import(&schema).expect("import schema");
	b.import(&data).expect("import data");
	assert_eq!(sorted(rows(&a, "from rt::t")), sorted(rows(&b, "from rt::t")));
}

#[test]
fn selection_by_kind_excludes_other_kinds() {
	let setup = r#"
CREATE NAMESPACE rt;
CREATE TABLE rt::t { id: int4 };
CREATE RINGBUFFER rt::rb { id: int4 } WITH { capacity: 4 };
"#;
	let a = fresh();
	a.admin_as_root(setup, Params::None).expect("setup");
	let dump = a.export(&ExportOptions::all().kind(ShapeKind::Table)).expect("export");
	assert!(dump.contains("CREATE TABLE rt::t"), "{dump}");
	assert!(!dump.contains("CREATE RINGBUFFER"), "kind=Table must exclude ring buffers:\n{dump}");
}
