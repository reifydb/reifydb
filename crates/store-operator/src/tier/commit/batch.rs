// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, btree_map::Entry},
	mem,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_value::value::row_number::RowNumber;

use crate::types::DurablePre;

pub type StateKey = (OperatorId, EncodedKey);

pub type AnchorKey = (OperatorId, GroupId, u8, RowNumber);

pub type AnchorSlot = (u8, RowNumber);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
	AnchorsOperator(OperatorId),
	AnchorsGroup(OperatorId, GroupId),
}

#[derive(Debug, Clone)]
pub struct StateEntry {
	pub post: Option<EncodedPodRow>,
	pub durable_pre: DurablePre,
}

#[derive(Debug, Default)]
pub struct FlushBatch {
	pub state: BTreeMap<StateKey, StateEntry>,
	pub anchors: BTreeMap<AnchorKey, Option<u64>>,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
}

impl FlushBatch {
	pub(crate) fn record_state(&mut self, key: StateKey, post: Option<EncodedPodRow>, durable_pre: DurablePre) {
		match self.state.entry(key) {
			Entry::Occupied(mut slot) => slot.get_mut().post = post,
			Entry::Vacant(slot) => {
				slot.insert(StateEntry {
					post,
					durable_pre,
				});
			}
		}
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.anchors.is_empty() && self.checkpoints.is_empty() && self.drops.is_empty()
	}

	pub fn entries(&self) -> usize {
		self.state.len() + self.anchors.len()
	}

	pub(super) fn split_within(&mut self, budget: usize) -> FlushBatch {
		let budget = budget.max(1);
		let state = split_bounded(&mut self.state, budget);
		let anchors = split_bounded(&mut self.anchors, budget - state.len());
		FlushBatch {
			state,
			anchors,
			checkpoints: mem::take(&mut self.checkpoints),
			drops: mem::take(&mut self.drops),
		}
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

fn split_bounded<K: Ord + Clone, V>(source: &mut BTreeMap<K, V>, budget: usize) -> BTreeMap<K, V> {
	if source.len() <= budget {
		return mem::take(source);
	}
	if budget == 0 {
		return BTreeMap::new();
	}
	let boundary = source.keys().nth(budget).expect("the budget is below the map length").clone();
	let remainder = source.split_off(&boundary);
	mem::replace(source, remainder)
}
