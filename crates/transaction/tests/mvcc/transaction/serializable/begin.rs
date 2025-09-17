// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_transaction::mvcc::transaction::serializable::Serializable;

#[test]
fn test_begin_query() {
	let engine = Serializable::testing();
	let tx = engine.begin_query().unwrap();
	assert_eq!(tx.version(), 1);
}

#[test]
fn test_begin_command() {
	let engine = Serializable::testing();
	let tx = engine.begin_command().unwrap();
	assert_eq!(tx.version(), 1);
}
