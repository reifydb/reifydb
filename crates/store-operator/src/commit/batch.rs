// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_value::value::row_number::RowNumber;

pub type StateKey = (OperatorId, EncodedKey);

pub type AnchorKey = (OperatorId, GroupId, u8, RowNumber);

pub type AnchorSlot = (u8, RowNumber);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
	AnchorsOperator(OperatorId),
	AnchorsGroup(OperatorId, GroupId),
}

#[derive(Debug, Default)]
pub struct FlushBatch {
	pub state: BTreeMap<StateKey, Option<EncodedOperatorRow>>,
	pub anchors: BTreeMap<AnchorKey, Option<u64>>,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
}

impl FlushBatch {
	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.anchors.is_empty() && self.checkpoints.is_empty() && self.drops.is_empty()
	}

	pub(super) fn clear_drop(&mut self, marker: DropMarker) {
		match marker {
			DropMarker::OperatorState(operator) => {
				self.state.retain(|(candidate, _), _| *candidate != operator);
				self.anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
			}
			DropMarker::AnchorsOperator(operator) => {
				self.anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
			}
			DropMarker::AnchorsGroup(operator, group) => {
				self.anchors.retain(|(candidate, candidate_group, _, _), _| {
					*candidate != operator || *candidate_group != group
				});
			}
		}
	}
}
