// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::flow::FlowNodeId, state::keyspace::KeyspaceMembership};

use crate::operator::join::store::Store;

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new(
		node_id: FlowNodeId,
		left_membership: Arc<KeyspaceMembership>,
		right_membership: Arc<KeyspaceMembership>,
	) -> Self {
		Self {
			left: Store::new(node_id, JoinSide::Left, left_membership),
			right: Store::new(node_id, JoinSide::Right, right_membership),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
	Left,
	Right,
}
