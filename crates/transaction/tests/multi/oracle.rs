// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_type::util::cowvec::CowVec;

fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

fn make_key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

fn make_row(s: &str) -> EncodedRow {
	EncodedRow(CowVec::new(s.as_bytes().to_vec()))
}

fn is_conflict_error(err: &reifydb_type::error::Error) -> bool {
	let msg = err.to_string();
	msg.contains("Conflict") || msg.contains("conflict")
}

#[test]
fn test_oracle_initial_version_and_first_commit() {
	let engine = test_multi();
	let v0 = engine.version().unwrap();

	let mut tx = engine.begin_command().unwrap();
	tx.set(&make_key("k"), make_row("v")).unwrap();
	let committed = tx.commit().unwrap();

	assert!(committed.0 > v0.0);
}

#[test]
fn test_conflict_detection_between_transactions() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command().unwrap();
	let mut t2 = engine.begin_command().unwrap();

	t1.set(&key, make_row("v1")).unwrap();
	t2.get(&key).unwrap();
	t2.set(&key, make_row("v2")).unwrap();

	t1.commit().unwrap();

	let err = t2.commit().expect_err("t2 must conflict with t1's write of `shared`");
	assert!(is_conflict_error(&err), "expected Conflict error, got {err}");
}

#[test]
fn test_no_conflict_different_keys() {
	let engine = test_multi();

	let mut t1 = engine.begin_command().unwrap();
	let mut t2 = engine.begin_command().unwrap();

	t1.set(&make_key("k1"), make_row("v1")).unwrap();
	t2.set(&make_key("k2"), make_row("v2")).unwrap();

	t1.commit().unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_version_filtering_in_conflict_detection() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command().unwrap();
	t1.set(&key, make_row("v1")).unwrap();
	t1.commit().unwrap();

	let mut t2 = engine.begin_command().unwrap();
	t2.get(&key).unwrap();
	t2.set(&key, make_row("v2")).unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_sequential_transactions_no_conflict() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command().unwrap();
	t1.get(&key).unwrap();
	t1.set(&key, make_row("v1")).unwrap();
	t1.commit().unwrap();

	let mut t2 = engine.begin_command().unwrap();
	t2.get(&key).unwrap();
	t2.set(&key, make_row("v2")).unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_multi_key_chain_detects_dependency_conflict() {
	let engine = test_multi();
	let key_a = make_key("a");
	let key_b = make_key("b");
	let key_c = make_key("c");

	let mut t1 = engine.begin_command().unwrap();
	let mut t2 = engine.begin_command().unwrap();
	let mut t3 = engine.begin_command().unwrap();

	t1.get(&key_a).unwrap();
	t1.set(&key_b, make_row("vb")).unwrap();

	t2.get(&key_b).unwrap();
	t2.set(&key_c, make_row("vc")).unwrap();

	t3.get(&key_c).unwrap();
	t3.set(&key_a, make_row("va")).unwrap();

	t1.commit().unwrap();

	let err = t2.commit().expect_err("t2 read `b` which t1 wrote; commit must conflict");
	assert!(is_conflict_error(&err), "expected Conflict error, got {err}");

	t3.commit().unwrap();
}
