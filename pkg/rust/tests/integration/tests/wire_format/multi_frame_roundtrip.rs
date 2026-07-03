// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, Params, RuntimeConfig, embedded as db_embedded};
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::frame::{data::FrameColumnData, frame::Frame};

fn new_db() -> Database {
	let db = db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build");
	db
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).expect("admin failed");
}

fn command(db: &Database, rql: &str) {
	db.command_as_root(rql, Params::None).expect("command failed");
}

fn query(db: &Database, rql: &str) -> Vec<Frame> {
	db.query_as_root(rql, Params::None).expect("query failed")
}

fn assert_col_data_eq(idx: usize, name: &str, a: &FrameColumnData, b: &FrameColumnData) {
	assert_eq!(
		a.len(),
		b.len(),
		"frame[{idx}] column '{name}': length mismatch (orig={}, decoded={})",
		a.len(),
		b.len()
	);
	for i in 0..a.len() {
		let va = a.get_value(i);
		let vb = b.get_value(i);
		assert_eq!(va, vb, "frame[{idx}] column '{name}' row {i}: {:?} != {:?}", va, vb);
	}
}

fn assert_frame_eq(idx: usize, a: &Frame, b: &Frame) {
	assert_eq!(
		a.row_numbers.len(),
		b.row_numbers.len(),
		"frame[{idx}]: row_numbers length mismatch (orig={}, decoded={})",
		a.row_numbers.len(),
		b.row_numbers.len()
	);
	for (i, (ra, rb)) in a.row_numbers.iter().zip(&b.row_numbers).enumerate() {
		assert_eq!(ra.value(), rb.value(), "frame[{idx}]: row_number[{i}] mismatch");
	}
	assert_eq!(a.created_at.len(), b.created_at.len(), "frame[{idx}]: created_at length mismatch");
	assert_eq!(a.updated_at.len(), b.updated_at.len(), "frame[{idx}]: updated_at length mismatch");
	assert_eq!(a.columns.len(), b.columns.len(), "frame[{idx}]: column count mismatch");
	for (ca, cb) in a.columns.iter().zip(&b.columns) {
		assert_eq!(ca.name, cb.name, "frame[{idx}]: column name mismatch");
		assert_col_data_eq(idx, &ca.name, &ca.data, &cb.data);
	}
}

fn assert_rbcf_round_trip(frames: &[Frame]) {
	let encoded = encode_frames(frames, &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), frames.len(), "frame count mismatch");
	for (i, (orig, dec)) in frames.iter().zip(decoded.iter()).enumerate() {
		assert_frame_eq(i, orig, dec);
	}
}

fn seed_table(db: &Database) {
	admin(db, "CREATE NAMESPACE wf");
	admin(db, "CREATE TABLE wf::t { id: int4, name: text, v: int4 }");
	command(
		db,
		r#"INSERT wf::t [
			{ id: 1, name: 'alpha',   v: 100 },
			{ id: 2, name: 'bravo',   v: 200 },
			{ id: 3, name: 'charlie', v: 300 },
			{ id: 4, name: 'delta',   v: 400 },
			{ id: 5, name: 'echo',    v: 500 }
		]"#,
	);
}

#[test]
fn single_statement_sort_take_round_trips() {
	let db = new_db();
	seed_table(&db);
	let frames = query(&db, "FROM wf::t | SORT { id: DESC } | TAKE 1");
	assert_eq!(frames.len(), 1);
	assert_rbcf_round_trip(&frames);
}

/// Run several single-statement queries and concatenate their frames into one
/// Vec<Frame>, mirroring what the engine returns for a multi-statement RQL
/// over the gRPC path. The in-process SDK's `query_as_root` returns only the
/// first statement's frame, so we splice the multi-frame buffer manually here.
fn collect_frames(db: &Database, statements: &[&str]) -> Vec<Frame> {
	let mut out = Vec::with_capacity(statements.len());
	for rql in statements {
		let mut frames = query(db, rql);
		assert!(!frames.is_empty(), "statement returned no frame: {rql}");
		out.push(frames.swap_remove(0));
	}
	out
}

#[test]
fn multi_statement_two_sort_take_round_trips() {
	// The minimal repro of the birdeye observation: two `sort | take`
	// statements concatenated into a multi-frame buffer.
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(
		&db,
		&["FROM wf::t | SORT { id: DESC } | TAKE 1", "FROM wf::t | SORT { id: ASC } | TAKE 1"],
	);
	assert_eq!(frames.len(), 2);
	assert_rbcf_round_trip(&frames);
}

#[test]
fn multi_statement_sort_take_then_aggregate_round_trips() {
	// Mirrors the token_overview shape: latest-row by sort+take followed by
	// an aggregate-by-group.
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(
		&db,
		&[
			"FROM wf::t | SORT { id: DESC } | TAKE 1",
			"FROM wf::t | AGGREGATE { c: math::count(id) } BY { name }",
		],
	);
	assert_eq!(frames.len(), 2);
	assert_rbcf_round_trip(&frames);
}

#[test]
fn multi_statement_aggregate_then_sort_take_round_trips() {
	// Reverse order of the previous case.
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(
		&db,
		&[
			"FROM wf::t | AGGREGATE { c: math::count(id) } BY { name }",
			"FROM wf::t | SORT { id: DESC } | TAKE 1",
		],
	);
	assert_eq!(frames.len(), 2);
	assert_rbcf_round_trip(&frames);
}

#[test]
fn multi_statement_take_take_round_trips() {
	// Sanity: same shape minus the sort. Pins the passing baseline so a
	// regression in the take/no-sort path is also caught.
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(&db, &["FROM wf::t | TAKE 1", "FROM wf::t | TAKE 1"]);
	assert_eq!(frames.len(), 2);
	assert_rbcf_round_trip(&frames);
}

#[test]
fn multi_statement_filter_take_filter_take_round_trips() {
	// Sanity: filter+take shape (no sort).
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(
		&db,
		&["FROM wf::t | FILTER { id > 0 } | TAKE 1", "FROM wf::t | FILTER { id > 0 } | TAKE 1"],
	);
	assert_eq!(frames.len(), 2);
	assert_rbcf_round_trip(&frames);
}

#[test]
fn handler_shape_three_frames_round_trips() {
	// Mimics the birdeye token_overview multi-statement RQL shape with one
	// frame each from: sort+take (latest price), aggregate (markets count),
	// sort+take (history price).
	let db = new_db();
	seed_table(&db);
	let frames = collect_frames(
		&db,
		&[
			"FROM wf::t | SORT { id: DESC } | TAKE 1",
			"FROM wf::t | AGGREGATE { c: math::count(id) } BY { name }",
			"FROM wf::t | SORT { id: ASC } | TAKE 1",
		],
	);
	assert_eq!(frames.len(), 3);
	assert_rbcf_round_trip(&frames);
}
