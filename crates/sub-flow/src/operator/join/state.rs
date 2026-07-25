// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	metrics::heap::{StateCompleteness, StateMemory},
	state::{
		keyspace::{KeyspaceMembership, fold_hash128},
		membership::MEMBERSHIP_BYTE_CAP,
	},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::Result;

use crate::operator::{
	join::store::{Store, hash_from_group_bytes},
	stateful::utils::internal_state_range,
};

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new(node_id: FlowNodeId, membership: Arc<JoinMembership>) -> Self {
		Self {
			left: Store::new(node_id, JoinSide::Left, membership.clone()),
			right: Store::new(node_id, JoinSide::Right, membership),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
	Left,
	Right,
}

impl JoinSide {
	pub(crate) fn keyspace(&self) -> Keyspace {
		match self {
			Self::Left => Keyspace::JOIN_LEFT,
			Self::Right => Keyspace::JOIN_RIGHT,
		}
	}

	pub(crate) fn tag(&self) -> u8 {
		match self {
			Self::Left => 0,
			Self::Right => 1,
		}
	}
}

pub(crate) struct JoinMembership {
	left: Arc<KeyspaceMembership>,
	right: Arc<KeyspaceMembership>,
}

impl JoinMembership {
	pub(crate) fn new() -> Self {
		Self::with_byte_cap(MEMBERSHIP_BYTE_CAP)
	}

	pub(crate) fn with_byte_cap(byte_cap: u64) -> Self {
		Self {
			left: Arc::new(KeyspaceMembership::new(byte_cap)),
			right: Arc::new(KeyspaceMembership::new(byte_cap)),
		}
	}

	pub(crate) fn side(&self, side: JoinSide) -> &KeyspaceMembership {
		match side {
			JoinSide::Left => &self.left,
			JoinSide::Right => &self.right,
		}
	}

	pub(crate) fn invalidate(&self) {
		self.left.invalidate();
		self.right.invalidate();
	}

	pub(crate) fn memory(&self) -> StateMemory {
		self.left.memory() + self.right.memory()
	}

	pub(crate) fn completeness(&self) -> StateCompleteness {
		self.left.completeness().merge(self.right.completeness())
	}

	pub(crate) fn hydrate(&self, node: FlowNodeId, txn: &mut FlowTransaction) -> Result<()> {
		if self.left.is_hydrated() && self.right.is_hydrated() {
			return Ok(());
		}

		let mut left_rows: Vec<GroupId> = Vec::new();
		let mut right_rows: Vec<GroupId> = Vec::new();
		for entry in internal_state_range(node, txn, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded)) {
			let (key, _) = entry?;
			let Some((group, keyspace, _)) = OperatorStateKey::decode_inner(key.as_ref()) else {
				continue;
			};
			if keyspace == Keyspace::JOIN_LEFT {
				left_rows.push(group);
			} else if keyspace == Keyspace::JOIN_RIGHT {
				right_rows.push(group);
			}
		}

		let mut folded: HashMap<GroupId, u64> = HashMap::new();
		for group in left_rows.iter().chain(right_rows.iter()) {
			if folded.contains_key(group) {
				continue;
			}
			let Some(bytes) = txn.group_bytes(node, *group)? else {
				continue;
			};
			let Some(hash) = hash_from_group_bytes(&bytes) else {
				continue;
			};
			folded.insert(*group, fold_hash128(&hash));
		}

		self.left.install(&folded_rows(&left_rows, &folded));
		self.right.install(&folded_rows(&right_rows, &folded));
		Ok(())
	}
}

fn folded_rows(rows: &[GroupId], folded: &HashMap<GroupId, u64>) -> Vec<u64> {
	rows.iter().filter_map(|group| folded.get(group).copied()).collect()
}
