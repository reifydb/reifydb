// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::test_multi;

#[test]
fn test_begin_query() {
	let engine = test_multi();
	let tx = engine.begin_query().unwrap();
	assert_eq!(tx.version(), 1);
}

#[test]
fn test_begin_command() {
	let engine = test_multi();
	let tx = engine.begin_command().unwrap();
	assert_eq!(tx.version(), 1);
}
