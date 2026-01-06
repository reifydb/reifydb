// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::multi::TransactionMulti;

#[test]
fn test_begin_query() {
	let engine = TransactionMulti::testing();
	let tx = engine.begin_query().unwrap();
	assert_eq!(tx.version(), 1);
}

#[test]
fn test_begin_command() {
	let engine = TransactionMulti::testing();
	let tx = engine.begin_command().unwrap();
	assert_eq!(tx.version(), 1);
}
