// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_group_state::Keyspace};

use crate::operator::join::{snapshot::snapshot_ledger_keyspaces, store::Store};

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new(operator_id: OperatorId, snapshot: bool) -> Self {
		Self {
			left: Store::new(operator_id, JoinSide::Left)
				.also_stamping(snapshot_ledger_keyspaces(snapshot)),
			right: Store::new(operator_id, JoinSide::Right),
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
