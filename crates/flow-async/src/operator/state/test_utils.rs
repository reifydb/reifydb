// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod test {
	use std::ops::Bound;

	use reifydb_codec::{key::encoded::EncodedKeyRange, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::{catalog::flow::OperatorId, store::MultiVersionRow},
		key::{
			any::TaggedKey,
			operator::state::{
				GroupId, GroupStateKey, GroupStateRange, KeyspaceId, OperatorStateKey, inner_range,
				unmanaged_key,
			},
		},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{multi::RangeScope, transaction::admin::AdminTransaction};
	use reifydb_value::{Result, value::identity::IdentityId};

	use crate::transaction::FlowTransaction;

	pub fn whole_keyspace(group: GroupId, keyspace: KeyspaceId) -> GroupStateRange {
		GroupStateRange::Keyspace {
			group,
			keyspace,
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	pub fn inner_bytes(range: GroupStateRange) -> EncodedKeyRange {
		inner_range(&range.for_operator(OperatorId(0))).expect("a group state range addresses operator state").1
	}

	pub fn scan_operator(txn: &mut impl FlowTransaction, operator: OperatorId) -> Vec<MultiVersionRow<TaggedKey>> {
		txn.range(OperatorStateKey::node_range(operator), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()
			.unwrap()
	}

	pub fn test_row() -> EncodedPodRow {
		EncodedPodRow::new(&[1, 2, 3, 4, 5])
	}

	pub fn test_key(suffix: &str) -> GroupStateKey {
		unmanaged_key(format!("test_{}", suffix).as_bytes())
			.expect("a fixture name must fit the keyspace's id width")
			.into()
	}

	pub fn assert_row_eq(actual: &EncodedPodRow, expected: &EncodedPodRow) {
		assert_eq!(actual, expected, "Rows do not match");
	}

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
