// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! An `Option(utf8)` column carrying a dictionary. The combination is what a nullable low
//! cardinality column needs to be stored as an id, and nothing else in the suite covers it: every
//! other dictionary test declares a non-optional `utf8`.

use reifydb::{
	Frame, Value,
	testing::db::{TempDbPath, TestDb},
};
use reifydb_test_harness::assert::column_values;

fn codes(frames: &[Frame]) -> Vec<Value> {
	column_values(&frames[0], "code")
}

fn absent(inner: &str) -> Value {
	Value::None {
		inner: inner.parse().expect("a value type name"),
	}
}

fn utf8(v: &str) -> Value {
	Value::Utf8(v.into())
}

#[test]
fn an_optional_dictionary_column_round_trips_values_and_absences() {
	// The whole point of the declaration is that a present value survives the trip through the
	// dictionary and an absent one stays absent. Interning the absence instead would hand back a
	// string where the caller expects nothing.
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create dictionary app::codes for utf8 as uint4");
	db.admin("create table app::t { id: int4, code: Option(utf8) with { dictionary: app::codes } }");
	db.command(
		"insert app::t [{ id: 1, code: 'aa' }, { id: 2, code: undefined }, { id: 3, code: 'bb' }, { id: 4 }]",
	);

	assert_eq!(
		codes(&db.query("from app::t | sort { id: asc }")),
		vec![utf8("aa"), absent("utf8"), utf8("bb"), absent("utf8")],
		"present values must decode to themselves and absent ones must stay absent, in row order"
	);
}

#[test]
fn an_absent_value_does_not_consume_a_dictionary_id() {
	// A dictionary keyed on uint4 has a finite id space. If every absence interned a sentinel the
	// space would burn down on a column that is mostly null, and the sentinel could later decode
	// into a row as a real value. The dictionary must hold the distinct present values and nothing
	// else.
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create dictionary app::codes for utf8 as uint4");
	db.admin("create table app::t { id: int4, code: Option(utf8) with { dictionary: app::codes } }");
	db.command(
		"insert app::t [{ id: 1, code: undefined }, { id: 2, code: 'aa' }, { id: 3, code: undefined }, \
		 { id: 4, code: 'aa' }, { id: 5 }]",
	);

	let entries = db.query("from app::codes | sort { id: asc }");
	assert_eq!(
		column_values(&entries[0], "value"),
		vec![utf8("aa")],
		"three absences and a repeat of 'aa' must leave exactly one entry: absences intern nothing and a \
		 repeated value reuses its id"
	);
}

#[test]
fn two_optional_columns_sharing_one_dictionary_intern_one_id_per_value() {
	// This is the shape polaris uses for base_symbol and quote_symbol: two nullable columns drawing
	// from one value space. Sharing is the reason to point them at one dictionary, so a value first
	// seen through one column must reuse that id when it arrives through the other rather than
	// allocating a second.
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create dictionary app::syms for utf8 as uint4");
	db.admin(
		"create table app::t { id: int4, base: Option(utf8) with { dictionary: app::syms }, \
		 quote: Option(utf8) with { dictionary: app::syms } }",
	);
	db.command(
		"insert app::t [{ id: 1, base: 'sol', quote: 'usdc' }, { id: 2, base: 'usdc', quote: undefined }]",
	);

	let entries = db.query("from app::syms | sort { id: asc }");
	assert_eq!(
		column_values(&entries[0], "value"),
		vec![utf8("sol"), utf8("usdc")],
		"'usdc' arrived as a quote and then as a base; one shared dictionary must hold it once"
	);

	let rows = db.query("from app::t | sort { id: asc }");
	assert_eq!(
		column_values(&rows[0], "base"),
		vec![utf8("sol"), utf8("usdc")],
		"a shared id space must not cross wires between the two columns"
	);
	assert_eq!(column_values(&rows[0], "quote"), vec![utf8("usdc"), absent("utf8")]);
}

#[test]
fn a_filter_on_an_optional_dictionary_column_matches_by_decoded_value() {
	// The predicate has to compare what the user wrote against the decoded value. Comparing against
	// the raw id would silently return the wrong rows rather than fail, and an absence must never
	// satisfy an equality test.
	let db = TestDb::memory();
	db.admin("create namespace app");
	db.admin("create dictionary app::codes for utf8 as uint4");
	db.admin("create table app::t { id: int4, code: Option(utf8) with { dictionary: app::codes } }");
	db.command(
		"insert app::t [{ id: 1, code: 'aa' }, { id: 2, code: 'bb' }, { id: 3, code: undefined }, \
		 { id: 4, code: 'aa' }]",
	);

	let hit = db.query("from app::t | filter { code == 'aa' } | sort { id: asc }");
	assert_eq!(
		column_values(&hit[0], "id"),
		vec![Value::Int4(1), Value::Int4(4)],
		"equality must select the two rows holding 'aa' and neither 'bb' nor the absence"
	);

	assert_eq!(
		db.row_count("from app::t | filter { code == 'zz' }"),
		0,
		"a value that was never interned must match nothing rather than colliding with an id"
	);
}

#[test]
fn optional_dictionary_entries_and_absences_survive_a_reopen() {
	// A row stores the id, not the string, so the entry that decodes it has to be durable too. A
	// memory store cannot observe this because the entry outlives nothing; only a real sqlite store,
	// stopped and reopened, can. The absences matter as much as the values here: a null that comes
	// back as a decoded string, or a value that comes back absent, are both silent corruption.
	let path = TempDbPath::new("dict_optional_reopen");

	{
		let mut db = TestDb::sqlite_at(&path);
		db.admin("create namespace app");
		db.admin("create dictionary app::codes for utf8 as uint4");
		db.admin("create table app::t { id: int4, code: Option(utf8) with { dictionary: app::codes } }");
		db.command("insert app::t [{ id: 1, code: 'aa' }, { id: 2, code: undefined }, { id: 3, code: 'bb' }]");

		assert_eq!(
			codes(&db.query("from app::t | sort { id: asc }")),
			vec![utf8("aa"), absent("utf8"), utf8("bb")],
			"precondition: the column reads correctly before the restart"
		);
		db.stop();
	}

	let mut db = TestDb::sqlite_at(&path);
	assert_eq!(
		codes(&db.query("from app::t | sort { id: asc }")),
		vec![utf8("aa"), absent("utf8"), utf8("bb")],
		"after a reopen the ids must still decode and the absence must still be absent: the entries reached \
		 the persistent tier, not just the in-memory commit buffer"
	);
	db.stop();
}
