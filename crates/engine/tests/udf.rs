// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Regression tests for the columnar VM batch-UDF path.
//!
//! Exercises UDF bodies containing each of the instructions that live in the
//! `is_vectorizable` whitelist: logical operators (AND/OR/XOR), BETWEEN, IN,
//! CAST, and conditional jumps via IF. A UDF invoked over a multi-row input
//! table must take the batch path in `UdfEvalNode::next` and produce the same
//! results the per-row scalar fallback would have produced.

use reifydb_engine::test_prelude::*;

fn setup() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::nums { id: int4, v: int4 }");
	t.command(
		r#"INSERT test::nums [
			{ id: 1, v: 0 },
			{ id: 2, v: 3 },
			{ id: 3, v: 5 },
			{ id: 4, v: 7 },
			{ id: 5, v: 10 }
		]"#,
	);
	t
}

fn bools(frames: &[Frame]) -> Vec<Option<bool>> {
	let frame = &frames[0];
	let out_col = frame.columns.iter().rev().next().unwrap();
	(0..out_col.data.len())
		.map(|i| match out_col.data.get_value(i) {
			Value::Boolean(b) => Some(b),
			Value::None {
				..
			} => None,
			other => panic!("expected Boolean, got {:?}", other),
		})
		.collect()
}

fn strings(frames: &[Frame]) -> Vec<String> {
	let frame = &frames[0];
	let out_col = frame.columns.iter().rev().next().unwrap();
	(0..out_col.data.len())
		.map(|i| match out_col.data.get_value(i) {
			Value::Utf8(s) => s,
			other => panic!("expected Utf8, got {:?}", other),
		})
		.collect()
}

fn ints(frames: &[Frame]) -> Vec<i64> {
	let frame = &frames[0];
	let out_col = frame.columns.iter().rev().next().unwrap();
	(0..out_col.data.len())
		.map(|i| match out_col.data.get_value(i) {
			Value::Int1(v) => v as i64,
			Value::Int2(v) => v as i64,
			Value::Int4(v) => v as i64,
			Value::Int8(v) => v,
			Value::Int16(v) => v as i64,
			other => panic!("expected integer, got {:?}", other),
		})
		.collect()
}

#[test]
fn test_batch_udf_logic_and() {
	let t = setup();
	let frames = t.query(r#"
			FUN in_range ($x: int) { RETURN $x > 2 AND $x < 8 };
			FROM test::nums MAP { id, r: in_range(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(false), Some(true), Some(true), Some(true), Some(false)]);
}

#[test]
fn test_batch_udf_logic_or_xor() {
	let t = setup();
	let frames = t.query(r#"
			FUN or_check ($x: int) { RETURN $x == 0 OR $x == 10 };
			FROM test::nums MAP { id, r: or_check(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(true), Some(false), Some(false), Some(false), Some(true)]);

	let frames = t.query(r#"
			FUN xor_check ($x: int) { RETURN ($x > 2) XOR ($x > 5) };
			FROM test::nums MAP { id, r: xor_check(v) } SORT { id: ASC }
		"#);
	// v=[0,3,5,7,10]: (x>2) XOR (x>5)
	// 0: F XOR F = F;  3: T XOR F = T;  5: T XOR F = T;  7: T XOR T = F;  10: T XOR T = F
	assert_eq!(bools(&frames), vec![Some(false), Some(true), Some(true), Some(false), Some(false)]);
}

#[test]
fn test_batch_udf_between() {
	let t = setup();
	let frames = t.query(r#"
			FUN in_range ($x: int) { RETURN $x BETWEEN 3 AND 7 };
			FROM test::nums MAP { id, r: in_range(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(false), Some(true), Some(true), Some(true), Some(false)]);
}

#[test]
fn test_batch_udf_in_list() {
	let t = setup();
	let frames = t.query(r#"
			FUN is_one_of ($x: int) { RETURN $x IN [0, 5, 10] };
			FROM test::nums MAP { id, r: is_one_of(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(true), Some(false), Some(true), Some(false), Some(true)]);

	let frames = t.query(r#"
			FUN not_in ($x: int) { RETURN $x NOT IN [0, 10] };
			FROM test::nums MAP { id, r: not_in(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(false), Some(true), Some(true), Some(true), Some(false)]);
}

#[test]
fn test_batch_udf_cast() {
	let t = setup();
	// CAST to utf8 — every integer round-trips unambiguously, unlike CAST-to-boolean
	// which in ReifyDB only accepts literal 0 or 1.
	let frames = t.query(r#"
			FUN as_utf8 ($x: int) { RETURN CAST($x, utf8) };
			FROM test::nums MAP { id, r: as_utf8(v) } SORT { id: ASC }
		"#);
	assert_eq!(strings(&frames), vec!["0", "3", "5", "7", "10"]);
}

#[test]
fn test_batch_udf_calls_vectorizable_udf() {
	let t = setup();
	// Outer UDF calls helper UDF. Inner body is vectorizable (arithmetic only), so
	// the outer's batch path dispatches Call into the columnar user-function path.
	let frames = t.query(r#"
			FUN helper ($y: int) { RETURN $y * 2 };
			FUN outer ($x: int) { RETURN helper($x) + 1 };
			FROM test::nums MAP { id, r: outer(v) } SORT { id: ASC }
		"#);
	// v = [0, 3, 5, 7, 10] → outer = v*2 + 1 = [1, 7, 11, 15, 21]
	assert_eq!(ints(&frames), vec![1, 7, 11, 15, 21]);
}

#[test]
fn test_batch_udf_calls_non_vectorizable_udf() {
	let t = setup();
	// Inner body uses BREAK inside WHILE; BREAK isn't in the vectorizability
	// whitelist, so the columnar Call path must fall back to per-row scalar
	// execution.
	let frames = t.query(r#"
			FUN helper ($x: int) : int2 {
				LET $i = 0;
				WHILE $i < 100 {
					IF $i >= $x { BREAK };
					$i = $i + 1
				};
				RETURN $i
			};
			FUN outer ($x: int) : int2 { RETURN helper($x) };
			FROM test::nums MAP { id, r: outer(v) } SORT { id: ASC }
		"#);
	// helper($x) counts $i from 0 until $i >= $x, then returns $i.
	// v = [0, 3, 5, 7, 10] → r = [0, 3, 5, 7, 10]
	assert_eq!(ints(&frames), vec![0, 3, 5, 7, 10]);
}

#[test]
fn test_batch_udf_if_branches() {
	let t = setup();
	let frames = t.query(r#"
			FUN classify ($x: int) {
				IF $x > 2 AND $x < 8 {
					RETURN TRUE
				}
				RETURN FALSE
			};
			FROM test::nums MAP { id, r: classify(v) } SORT { id: ASC }
		"#);
	assert_eq!(bools(&frames), vec![Some(false), Some(true), Some(true), Some(true), Some(false)]);
}
