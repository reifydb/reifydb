// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_value::{params::Params, value::Value};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::p { k: uint2, v: int4 } WITH { partition: { by: { k } } }");
	t.command(
		"INSERT test::p [{ k: 1, v: 10 }, { k: 2, v: 20 }, { k: 3, v: 30 }, { k: 4, v: 40 }, { k: 5, v: 50 }, \
		 { k: 6, v: 60 }]",
	);
	t
}

fn keys(ks: &[u16]) -> Params {
	Params::from(HashMap::from([("ks".to_string(), Value::List(ks.iter().copied().map(Value::Uint2).collect()))]))
}

fn open(t: &TestEngine, rql: &str, params: Params) -> CommandTransaction {
	let mut txn = t.begin_command(TestEngine::identity()).expect("begin command");
	if let Some(error) = txn.rql(rql, params).error {
		panic!("{rql} failed inside its transaction: {error:?}");
	}
	txn
}

fn values(t: &TestEngine) -> Vec<(u16, i32)> {
	let mut rows: Vec<(u16, i32)> = t
		.query("FROM test::p MAP { k, v }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| (row.get::<u16>("k").unwrap().unwrap(), row.get::<i32>("v").unwrap().unwrap()))
		.collect();
	rows.sort();
	rows
}

#[test]
fn an_update_over_listed_keys_does_not_conflict_with_a_write_to_an_unlisted_key() {
	// only a scan that reads just the listed partitions can commit past a concurrent write elsewhere.
	let t = engine();
	let mut update = open(&t, "UPDATE test::p { v: 0 } FILTER { k in $ks }", keys(&[1, 5]));
	t.command("UPDATE test::p { v: 99 } FILTER { k == 3 }");
	update.commit().expect("the update touched only keys 1 and 5, so a write to 3 must not conflict");
	assert_eq!(values(&t), vec![(1, 0), (2, 20), (3, 99), (4, 40), (5, 0), (6, 60)]);
}

#[test]
fn an_update_over_listed_keys_still_conflicts_with_a_write_to_a_listed_key() {
	// pruning must narrow the read set, never drop the partitions the update really read.
	let t = engine();
	let mut update = open(&t, "UPDATE test::p { v: 0 } FILTER { k in $ks }", keys(&[1, 5]));
	t.command("UPDATE test::p { v: 99 } FILTER { k == 5 }");
	assert!(update.commit().is_err(), "a concurrent write to a listed key must conflict");
	assert_eq!(values(&t), vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 99), (6, 60)]);
}

#[test]
fn an_update_over_an_empty_list_reads_nothing() {
	// an empty key list must not fall back to a full scan; the side insert makes the transaction a writer, which a
	// read-only one never conflicts as.
	let t = engine();
	t.admin("CREATE TABLE test::side { n: int4 }");
	let mut update = open(&t, "UPDATE test::p { v: 0 } FILTER { k in $ks }", keys(&[]));
	if let Some(error) = update.rql("INSERT test::side [{ n: 1 }]", Params::None).error {
		panic!("the side insert failed: {error:?}");
	}
	t.command("UPDATE test::p { v: 99 } FILTER { k == 3 }");
	update.commit().expect("an update over no keys read nothing, so it cannot conflict");
	assert_eq!(values(&t), vec![(1, 10), (2, 20), (3, 99), (4, 40), (5, 50), (6, 60)]);
}

#[test]
fn a_literal_list_with_a_variable_item_is_pruned_too() {
	// the bracket form must take the same pruned path as a list variable.
	let t = engine();
	let params = Params::from(HashMap::from([("one".to_string(), Value::Uint2(2))]));
	let mut update = open(&t, "UPDATE test::p { v: 0 } FILTER { k in [6, $one] }", params);
	t.command("UPDATE test::p { v: 99 } FILTER { k == 3 }");
	update.commit().expect("the update touched only keys 2 and 6, so a write to 3 must not conflict");
	assert_eq!(values(&t), vec![(1, 10), (2, 0), (3, 99), (4, 40), (5, 50), (6, 0)]);
}

#[test]
fn a_pruned_scan_returns_rows_in_full_scan_order() {
	// callers that page or take must see the same order whether or not the scan was pruned.
	let t = engine();
	let pruned: Vec<u16> = t
		.command_with_params("FROM test::p FILTER { k in $ks } MAP { k }", keys(&[6, 1, 4, 3]))
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| row.get::<u16>("k").unwrap().unwrap())
		.collect();
	let full: Vec<u16> = t
		.query("FROM test::p FILTER { k == 6 or k == 1 or k == 4 or k == 3 } MAP { k }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| row.get::<u16>("k").unwrap().unwrap())
		.collect();
	assert_eq!(pruned, full);
}

#[test]
fn duplicate_and_unknown_keys_neither_repeat_nor_invent_rows() {
	// a key listed twice must not update or return its row twice, and a missing key must stay missing.
	let t = engine();
	let got: Vec<u16> = t
		.command_with_params("FROM test::p FILTER { k in $ks } MAP { k }", keys(&[2, 2, 9]))
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| row.get::<u16>("k").unwrap().unwrap())
		.collect();
	assert_eq!(got, vec![2]);
}

#[test]
fn many_keys_fill_batches_across_partitions() {
	// the scan carries a batch across partition boundaries, so every listed row must arrive exactly once.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::p { k: uint2, v: int4 } WITH { partition: { by: { k } } }");
	let rows: Vec<String> = (0..3000).map(|k| format!("{{ k: {k}, v: {k} }}")).collect();
	t.command(&format!("INSERT test::p [{}]", rows.join(", ")));
	let wanted: Vec<u16> = (0..3000).filter(|k| k % 3 == 0).collect();
	let mut got: Vec<u16> = t
		.command_with_params("FROM test::p FILTER { k in $ks } MAP { k }", keys(&wanted))
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| row.get::<u16>("k").unwrap().unwrap())
		.collect();
	got.sort();
	assert_eq!(got, wanted);
}
