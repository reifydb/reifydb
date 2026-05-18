// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::test_multi;
use crate::{as_key, as_values};

#[test]
fn test_rollback_same_tx() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.rollback().unwrap();
	assert!(txn.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_rollback_different_tx() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.rollback().unwrap();

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_savepoint_restore_drops_post_savepoint_writes() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();

	// Write key 1 BEFORE the savepoint - should survive restore + commit.
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	let sp = txn.savepoint();

	// Write key 2 AFTER the savepoint - must NOT survive restore + commit.
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.restore_savepoint(sp);

	// Commit the restored transaction.
	let v = txn.commit().unwrap();
	assert!(v.0 > 0, "commit should produce a non-zero version");

	// Verify what landed in storage.
	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_some(), "key 1 was written before the savepoint and must be committed");
	assert!(
		rx.get(&as_key!(2)).unwrap().is_none(),
		"key 2 was written after the savepoint and rolled back via restore_savepoint; \
		 it must not be in storage. If this asserts, WriteSavepoint did not snapshot \
		 delta_log and the commit replayed the post-savepoint write."
	);
}
