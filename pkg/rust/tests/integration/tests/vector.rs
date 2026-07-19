// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end coverage for the vector(N) column type: DDL, insert (via a list literal, which is how
//! a client actually supplies an embedding), read-back, and the dimension constraint. These are the
//! paths that cross every layer touched by the feature - rql type resolution, the engine's
//! List -> Vector coercion, the row codec's dynamic section, and the columnar buffer.

use reifydb::{Database, Params, embedded};
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

fn float8(frames: &[Frame], name: &str, row: usize) -> f64 {
	match column(frames, name).data.get_value(row) {
		Value::Float8(v) => *v,
		other => panic!("column {name} row {row} is {other:?}, expected a Float8 distance"),
	}
}

fn knn_db() -> Database {
	let db = embedded::memory().build().unwrap();
	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::knn { id: int4, embedding: vector(4) }", Params::None).unwrap();
	db.command_as_root(
		"insert test::knn [\
			{ id: 1, embedding: [0.1, 0.2, 0.3, 0.4] }, \
			{ id: 2, embedding: [-0.1, -0.2, -0.3, -0.4] }, \
			{ id: 3, embedding: [0.4, 0.3, 0.2, 0.1] }]",
		Params::None,
	)
	.unwrap();
	db
}

// The whole point of the feature: a query vector supplied as a list literal must rank the identical
// row first. This is the only test that proves the engine's list-literal argument (a ColumnBuffer of
// Value::List, NOT a vector) is coerced to a vector before the kernel sees it - without that
// coercion the function errors and KNN is impossible.
#[test]
fn cosine_distance_ranks_the_identical_row_first() {
	let mut db = knn_db();

	let frames = db
		.query_as_root(
			"from test::knn \
			 extend { dist: vector::cosine_distance(embedding, [0.1, 0.2, 0.3, 0.4]) } \
			 sort { dist: asc } \
			 take 3",
			Params::None,
		)
		.unwrap();

	let ids = column(&frames, "id");
	assert_eq!(ids.data.get_value(0), Value::Int4(1), "the identical vector must be nearest");
	assert_eq!(ids.data.get_value(2), Value::Int4(2), "the opposed vector must be farthest");

	// Self-distance is exact only to one ulp of 1.0 and the error can be negative, so this must be
	// an absolute-value bound, never dist >= 0.0.
	assert!(float8(&frames, "dist", 0).abs() <= 1e-6, "self-distance must be ~0");
	assert!((float8(&frames, "dist", 2) - 2.0).abs() <= 1e-6, "the opposed vector is distance 2");

	db.stop().unwrap();
}

// The other way an embedding arrives: bound as a parameter, which reaches the function already as a
// vector column rather than a list. Both argument shapes must produce the same answer.
#[test]
fn cosine_distance_accepts_a_query_vector_parameter() {
	let mut db = knn_db();

	let params = Params::Positional(std::sync::Arc::new(vec![Value::vector(vec![0.1, 0.2, 0.3, 0.4])]));
	let frames = db
		.query_as_root(
			"from test::knn extend { dist: vector::cosine_distance(embedding, $1) } sort { dist: asc } take 1",
			params,
		)
		.unwrap();

	assert_eq!(column(&frames, "id").data.get_value(0), Value::Int4(1));
	assert!(float8(&frames, "dist", 0).abs() <= 1e-6);

	db.stop().unwrap();
}

#[test]
fn l2_distance_dot_and_norm_compute_known_values() {
	let mut db = embedded::memory().build().unwrap();
	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::metrics { id: int4, embedding: vector(4) }", Params::None).unwrap();
	db.command_as_root("insert test::metrics [{ id: 1, embedding: [3.0, 4.0, 0.0, 0.0] }]", Params::None).unwrap();

	let frames = db
		.query_as_root(
			"from test::metrics extend { \
				l2: vector::l2_distance(embedding, [0.0, 0.0, 0.0, 0.0]), \
				d: vector::dot(embedding, [1.0, 1.0, 0.0, 0.0]), \
				n: vector::norm(embedding) }",
			Params::None,
		)
		.unwrap();

	assert!((float8(&frames, "l2", 0) - 5.0).abs() <= 1e-6, "3-4-5 triangle");
	assert!((float8(&frames, "d", 0) - 7.0).abs() <= 1e-6, "3*1 + 4*1");
	assert!((float8(&frames, "n", 0) - 5.0).abs() <= 1e-6, "norm of [3,4,0,0]");

	db.stop().unwrap();
}

// A query vector of the wrong width is a user error, not a silent truncation: comparing a vector(4)
// column against a 3-element query must fail loudly rather than read across row boundaries.
#[test]
fn distance_rejects_a_query_vector_of_the_wrong_dimension() {
	let mut db = knn_db();

	let err = db
		.query_as_root(
			"from test::knn extend { dist: vector::cosine_distance(embedding, [0.1, 0.2, 0.3]) }",
			Params::None,
		)
		.unwrap_err();

	// An error raised inside a function body is wrapped as FUNCTION_007 by the routine layer, so the
	// dimension violation is the cause rather than the top-level code. Assert both: the wrapper alone
	// would still pass if the dimension check vanished and some other failure took its place.
	assert_eq!(err.code, "FUNCTION_007", "expected a function execution failure, got: {}", err.message);
	let cause = err.cause.as_ref().expect("the dimension violation must be preserved as the cause");
	assert_eq!(cause.code, "CONSTRAINT_008", "expected a vector dimension violation, got: {}", cause.message);

	db.stop().unwrap();
}

// A zero vector has no direction, so its cosine distance is undefined. We return NONE rather than
// NaN: a NaN would poison the SORT that every nearest-neighbour query depends on, silently placing
// the row at an arbitrary rank.
#[test]
fn cosine_distance_of_a_zero_vector_is_none() {
	let mut db = embedded::memory().build().unwrap();
	db.admin_as_root("create namespace test", Params::None).unwrap();
	db.admin_as_root("create table test::zero { id: int4, embedding: vector(4) }", Params::None).unwrap();
	db.command_as_root("insert test::zero [{ id: 1, embedding: [0.0, 0.0, 0.0, 0.0] }]", Params::None).unwrap();

	let frames = db
		.query_as_root(
			"from test::zero extend { dist: vector::cosine_distance(embedding, [0.1, 0.2, 0.3, 0.4]) }",
			Params::None,
		)
		.unwrap();

	let dist = column(&frames, "dist").data.get_value(0);
	assert!(matches!(dist, Value::None { .. }), "a zero vector has an undefined cosine distance, got {dist:?}");

	// l2 and dot remain defined for a zero vector; only cosine is undefined.
	let frames = db
		.query_as_root(
			"from test::zero extend { l2: vector::l2_distance(embedding, [0.0, 0.0, 0.0, 0.0]) }",
			Params::None,
		)
		.unwrap();
	assert!((float8(&frames, "l2", 0)).abs() <= 1e-6);

	db.stop().unwrap();
}

// The engine dispatches a builtin two different ways. Every test above exercises the columnar path
// (MAP/EXTEND, a full batch). A `let` binding instead routes through the scalar VM, which calls the
// same function with a single row and renames the arguments to arg0/arg1. A kernel that trusted
// ctx.row_count or looked its arguments up by name would pass every test above and break here.
#[test]
fn cosine_distance_works_on_the_scalar_vm_path() {
	let mut db = knn_db();

	let params = Params::Positional(std::sync::Arc::new(vec![
		Value::vector(vec![1.0, 0.0, 0.0, 0.0]),
		Value::vector(vec![0.0, 1.0, 0.0, 0.0]),
	]));
	let frames = db.query_as_root("let $d = vector::cosine_distance($1, $2); $d", params).unwrap();

	let dist = match frames[0].columns[0].data.get_value(0) {
		Value::Float8(v) => *v,
		other => panic!("expected a Float8 distance on the scalar path, got {other:?}"),
	};
	assert!((dist - 1.0).abs() <= 1e-6, "orthogonal vectors are distance 1 on the scalar path too");

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

// A user-defined function that wraps a vector builtin is the natural way to name a distance metric
// once and reuse it. Its body runs through the VM rather than the columnar expression path, so this
// is a third dispatch route into the same kernel, over a multi-row batch.
#[test]
fn a_udf_body_can_call_a_vector_builtin_over_a_batch() {
	let mut db = knn_db();

	let frames = db
		.query_as_root(
			"udf nearest_dist($e: vector(4)): float8 { vector::cosine_distance($e, [0.1, 0.2, 0.3, 0.4]) }; \
			 from test::knn map { id, d: nearest_dist(embedding) }",
			Params::None,
		)
		.unwrap();

	let ids = column(&frames, "id");
	let distances: Vec<(Value, f64)> =
		(0..ids.data.len()).map(|i| (ids.data.get_value(i), float8(&frames, "d", i))).collect();

	for (id, distance) in distances {
		let expected = match id {
			Value::Int4(1) => 0.0,
			Value::Int4(2) => 2.0,
			Value::Int4(3) => 1.0 / 3.0,
			other => panic!("unexpected id {other:?}"),
		};
		assert!(
			(distance - expected).abs() <= 1e-6,
			"udf distance for {id:?} was {distance}, expected {expected}"
		);
	}

	db.stop().unwrap();
}
