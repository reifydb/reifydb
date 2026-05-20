// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;

use crate::operator::join::store::Store;

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new(node_id: FlowNodeId) -> Self {
		Self {
			left: Store::new(node_id, JoinSide::Left),
			right: Store::new(node_id, JoinSide::Right),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
	Left,
	Right,
}
