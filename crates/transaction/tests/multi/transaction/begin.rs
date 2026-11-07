// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::test_multi;

#[test]
fn test_begin_query() {
	let engine = test_multi();
	let tx = engine.begin_query(engine.generate_transaction_id()).unwrap();
	assert_eq!(tx.version(), 1);
}

#[test]
fn test_begin_command() {
	let engine = test_multi();
	let tx = engine.begin_command(engine.generate_transaction_id()).unwrap();
	assert_eq!(tx.version(), 1);
}
