// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::multi::Transaction;

#[test]
fn test_begin_query() {
	let engine = Transaction::testing();
	let tx = engine.begin_query().unwrap();
	assert_eq!(tx.version(), 1);
}

#[test]
fn test_begin_command() {
	let engine = Transaction::testing();
	let tx = engine.begin_command().unwrap();
	assert_eq!(tx.version(), 1);
}
