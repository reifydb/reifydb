// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end coverage for the vector(N) column type: DDL, insert (via a list literal, which is how
//! a client actually supplies an embedding), read-back, and the dimension constraint. These are the
//! paths that cross every layer touched by the feature - rql type resolution, the engine's
//! List -> Vector coercion, the row codec's dynamic section, and the columnar buffer.

use reifydb::{Params, embedded};
use reifydb_value::value::{Value, frame::frame::Frame};

fn column<'a>(frames: &'a [Frame], name: &str) -> &'a reifydb_value::value::frame::column::FrameColumn {
	frames.iter()
		.flat_map(|f| f.columns.iter())
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} not in result"))
}

#[test]
fn vector_column_round_trips_through_storage() {
	let mut db = embedded::memory().build().unwrap();

	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::mem { id: int4, embedding: vector(4) }", Params::None).unwrap();

	db.command_as_root(
		"insert test::mem [{ id: 1, embedding: [0.1, 0.2, 0.3, 0.4] }, { id: 2, embedding: [1.0, 0.0, -1.0, 0.5] }]",
		Params::None,
	)
	.unwrap();

	let frames = db.query_as_root("from test::mem sort { id: asc }", Params::None).unwrap();
	let embedding = column(&frames, "embedding");

	assert_eq!(embedding.data.len(), 2, "both rows must come back");
	assert_eq!(
		embedding.data.get_value(0),
		Value::vector(vec![0.1, 0.2, 0.3, 0.4]),
		"a vector must survive the row codec's dynamic section unchanged"
	);
	assert_eq!(embedding.data.get_value(1), Value::vector(vec![1.0, 0.0, -1.0, 0.5]));

	db.stop().unwrap();
}

#[test]
fn vector_column_rejects_a_wrong_dimension() {
	let mut db = embedded::memory().build().unwrap();

	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::dims { id: int4, embedding: vector(4) }", Params::None).unwrap();

	// Three elements into a vector(4) column. If the dimension constraint is not enforced, this
	// silently stores a short row and every later distance computation reads across row boundaries.
	let err = db
		.command_as_root("insert test::dims [{ id: 1, embedding: [0.1, 0.2, 0.3] }]", Params::None)
		.unwrap_err();

	assert_eq!(err.code, "CONSTRAINT_008", "expected a vector dimension violation, got: {}", err.message);

	db.stop().unwrap();
}

#[test]
fn vector_column_rejects_a_non_finite_element() {
	let mut db = embedded::memory().build().unwrap();

	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::finite { id: int4, embedding: vector(2) }", Params::None).unwrap();

	// A NaN cannot be written as an RQL literal, but a client can send one in a parameter - which
	// is exactly how an embedding arrives. A stored NaN makes every distance against this row NaN,
	// which then sorts to an arbitrary position in a nearest-neighbour query.
	let params = Params::Positional(std::sync::Arc::new(vec![Value::vector(vec![0.1, f32::NAN])]));
	let err = db.command_as_root("insert test::finite [{ id: 1, embedding: $1 }]", params).unwrap_err();

	assert_eq!(err.code, "CONSTRAINT_009", "expected a non-finite element rejection, got: {}", err.message);

	db.stop().unwrap();
}

#[test]
fn bare_vector_without_a_dimension_is_rejected() {
	let mut db = embedded::memory().build().unwrap();

	db.admin_as_root("create namespace test", Params::None).unwrap();

	// A vector's dimension is part of its type; without it the column has no stride.
	let err =
		db.admin_as_root("create table test::bare { id: int4, embedding: vector }", Params::None).unwrap_err();

	assert_eq!(err.code, "AST_011", "expected a missing-dimension error, got: {}", err.message);

	db.stop().unwrap();
}
