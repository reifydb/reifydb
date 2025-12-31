// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_transaction::multi::TransactionMulti;

use crate::{as_key, as_values};

#[tokio::test]
async fn test_rollback_same_tx() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.rollback().unwrap();
	assert!(txn.get(&as_key!(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn test_rollback_different_tx() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.rollback().unwrap();

	let rx = engine.begin_query().await.unwrap();
	assert!(rx.get(&as_key!(1)).await.unwrap().is_none());
}
