// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{interface::catalog::id::QueueId, key::queue::QueueDeduplicationKey};
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_value::util::cowvec::CowVec;

fn system_ctx() -> Arc<TransactionContext> {
	Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
}

fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

fn make_key(s: &str) -> QueueDeduplicationKey {
	QueueDeduplicationKey::new(QueueId(1), s.as_bytes().iter().map(|b| !b).collect::<Vec<u8>>())
}

fn make_bytes(s: &str) -> EncodedBytes {
	EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
}

fn is_conflict_error(err: &reifydb_value::error::Error) -> bool {
	let msg = err.to_string();
	msg.contains("Conflict") || msg.contains("conflict")
}

#[test]
fn test_oracle_initial_version_and_first_commit() {
	let engine = test_multi();
	let v0 = engine.version().unwrap();

	let mut tx = engine.begin_command(engine.generate_transaction_id()).unwrap();
	tx.set(vec![(make_key("k").into(), make_bytes("v"))]).unwrap();
	let committed = tx.commit().unwrap();

	assert!(committed.0 > v0.0);
}

#[test]
fn test_conflict_detection_between_transactions() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	let mut t2 = engine.begin_command(engine.generate_transaction_id()).unwrap();

	t1.set(vec![(key.clone().into(), make_bytes("v1"))]).unwrap();
	t2.get(&system_ctx(), &[key.clone().into()]).unwrap().pop().flatten();
	t2.set(vec![(key.clone().into(), make_bytes("v2"))]).unwrap();

	t1.commit().unwrap();

	let err = t2.commit().expect_err("t2 must conflict with t1's write of `shared`");
	assert!(is_conflict_error(&err), "expected Conflict error, got {err}");
}

#[test]
fn test_no_conflict_different_keys() {
	let engine = test_multi();

	let mut t1 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	let mut t2 = engine.begin_command(engine.generate_transaction_id()).unwrap();

	t1.set(vec![(make_key("k1").into(), make_bytes("v1"))]).unwrap();
	t2.set(vec![(make_key("k2").into(), make_bytes("v2"))]).unwrap();

	t1.commit().unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_version_filtering_in_conflict_detection() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	t1.set(vec![(key.clone().into(), make_bytes("v1"))]).unwrap();
	t1.commit().unwrap();

	let mut t2 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	t2.get(&system_ctx(), &[key.clone().into()]).unwrap().pop().flatten();
	t2.set(vec![(key.clone().into(), make_bytes("v2"))]).unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_sequential_transactions_no_conflict() {
	let engine = test_multi();
	let key = make_key("shared");

	let mut t1 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	t1.get(&system_ctx(), &[key.clone().into()]).unwrap().pop().flatten();
	t1.set(vec![(key.clone().into(), make_bytes("v1"))]).unwrap();
	t1.commit().unwrap();

	let mut t2 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	t2.get(&system_ctx(), &[key.clone().into()]).unwrap().pop().flatten();
	t2.set(vec![(key.clone().into(), make_bytes("v2"))]).unwrap();
	t2.commit().unwrap();
}

#[test]
fn test_multi_key_chain_detects_dependency_conflict() {
	let engine = test_multi();
	let key_a = make_key("a");
	let key_b = make_key("b");
	let key_c = make_key("c");

	let mut t1 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	let mut t2 = engine.begin_command(engine.generate_transaction_id()).unwrap();
	let mut t3 = engine.begin_command(engine.generate_transaction_id()).unwrap();

	t1.get(&system_ctx(), &[key_a.clone().into()]).unwrap().pop().flatten();
	t1.set(vec![(key_b.clone().into(), make_bytes("vb"))]).unwrap();

	t2.get(&system_ctx(), &[key_b.clone().into()]).unwrap().pop().flatten();
	t2.set(vec![(key_c.clone().into(), make_bytes("vc"))]).unwrap();

	t3.get(&system_ctx(), &[key_c.clone().into()]).unwrap().pop().flatten();
	t3.set(vec![(key_a.clone().into(), make_bytes("va"))]).unwrap();

	t1.commit().unwrap();

	let err = t2.commit().expect_err("t2 read `b` which t1 wrote; commit must conflict");
	assert!(is_conflict_error(&err), "expected Conflict error, got {err}");

	t3.commit().unwrap();
}
