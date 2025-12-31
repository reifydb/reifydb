// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
