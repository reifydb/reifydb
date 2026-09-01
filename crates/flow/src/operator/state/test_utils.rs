// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod test {
	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::key::operator::state::{GroupStateKey, custom_not_cached_key};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::value::identity::IdentityId;

	pub fn test_row() -> EncodedPodRow {
		EncodedPodRow::new(&[1, 2, 3, 4, 5])
	}

	pub fn test_key(suffix: &str) -> GroupStateKey {
		custom_not_cached_key(format!("test_{}", suffix).as_bytes())
			.expect("a fixture name must fit the keyspace's id width")
	}

	pub fn assert_row_eq(actual: &EncodedPodRow, expected: &EncodedPodRow) {
		assert_eq!(actual, expected, "Rows do not match");
	}

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
