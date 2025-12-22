// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::multi::TransactionMulti;

#[tokio::test]
async fn test_begin_query() {
	let engine = TransactionMulti::testing().await;
	let tx = engine.begin_query().await.unwrap();
	assert_eq!(tx.version(), 1);
}

#[tokio::test]
async fn test_begin_command() {
	let engine = TransactionMulti::testing().await;
	let tx = engine.begin_command().await.unwrap();
	assert_eq!(tx.version(), 1);
}
