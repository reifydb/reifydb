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
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use crate::{
	tier::commit::state_map::StateMap,
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, DurablePre},
};

pub type StateKey = (OperatorId, EncodedKey);

pub type AnchorKey = (OperatorId, GroupId, u8, RowNumber);

pub type AnchorSlot = (u8, RowNumber);

pub const ANCHOR_ENTRY_BYTES: ByteSize = ANCHOR_KEY_BYTES.saturating_add(ANCHOR_VALUE_BYTES);

pub const MAX_FREQUENCY: u8 = 15;

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
	pub count: u8,
}

#[derive(Debug, Default)]
pub struct FlushBatch {
	pub state: StateMap,
	pub anchors: BTreeMap<AnchorKey, Option<u64>>,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
	pub bytes: ByteSize,
}

impl FlushBatch {
	pub(crate) fn record_state(&mut self, key: StateKey, post: Option<EncodedPodRow>, durable_pre: DurablePre) {
		let incoming = post_bytes(&post);
		let key_bytes = ByteSize::from_bytes(key.1.len() as u64);
		let mut admitted = false;
		let outgoing = match self.state.slot(key) {
			Entry::Occupied(mut slot) => {
				let entry = slot.get_mut();
				let outgoing = post_bytes(&entry.post);
				entry.post = post;
				entry.count = entry.count.saturating_add(1).min(MAX_FREQUENCY);
				outgoing
			}
			Entry::Vacant(slot) => {
				slot.insert(StateEntry {
					post,
					durable_pre,
					count: 1,
				});
				admitted = true;
				ByteSize::ZERO
			}
		};
		if admitted {
			self.state.admit();
			self.bytes = self.bytes.saturating_add(key_bytes);
		}
		self.bytes = self.bytes.saturating_sub(outgoing).saturating_add(incoming);
	}

	pub(crate) fn record_anchor(&mut self, key: AnchorKey, expiry: Option<u64>) {
		if self.anchors.insert(key, expiry).is_none() {
			self.bytes = self.bytes.saturating_add(ANCHOR_ENTRY_BYTES);
		}
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.anchors.is_empty() && self.checkpoints.is_empty() && self.drops.is_empty()
	}

	pub(super) fn drain_within(&mut self, budget: ByteSize) -> FlushBatch {
		let (state, state_bytes) = self.state.take_within(budget, true);
		self.assemble(state, state_bytes, budget)
	}

	pub(super) fn evict_within(
		&mut self,
		budget: ByteSize,
		cursor: Option<StateKey>,
	) -> (FlushBatch, Option<StateKey>) {
		let swept = self.state.sweep(budget, cursor);
		(self.assemble(swept.taken, swept.bytes, budget), swept.cursor)
	}

	fn assemble(&mut self, state: StateMap, state_bytes: ByteSize, budget: ByteSize) -> FlushBatch {
		let (anchors, anchor_bytes) = split_bounded(
			&mut self.anchors,
			budget.saturating_sub(state_bytes),
			|_, _| ANCHOR_ENTRY_BYTES,
			state.is_empty(),
		);
		let bytes = state_bytes.saturating_add(anchor_bytes);
		self.bytes = self.bytes.saturating_sub(bytes);
		FlushBatch {
			state,
			anchors,
			checkpoints: mem::take(&mut self.checkpoints),
			drops: mem::take(&mut self.drops),
			bytes,
		}
	}

	pub(super) fn clear_drop(&mut self, marker: DropMarker) {
		match marker {
			DropMarker::OperatorState(operator) => {
				self.drop_state(operator);
				self.retain_anchors(|(candidate, _, _, _)| *candidate != operator);
			}
			DropMarker::AnchorsOperator(operator) => {
				self.retain_anchors(|(candidate, _, _, _)| *candidate != operator);
			}
			DropMarker::AnchorsGroup(operator, group) => {
				self.retain_anchors(|(candidate, candidate_group, _, _)| {
					*candidate != operator || *candidate_group != group
				});
			}
		}
	}

	fn drop_state(&mut self, operator: OperatorId) {
		let Some(keys) = self.state.remove_operator(operator) else {
			return;
		};
		for (key, entry) in keys.iter() {
			self.bytes = self.bytes.saturating_sub(state_entry_bytes(key, entry));
		}
	}

	fn retain_anchors(&mut self, keep: impl Fn(&AnchorKey) -> bool) {
		let bytes = &mut self.bytes;
		self.anchors.retain(|key, _| {
			if keep(key) {
				return true;
			}
			*bytes = bytes.saturating_sub(ANCHOR_ENTRY_BYTES);
			false
		});
	}
}

fn post_bytes(post: &Option<EncodedPodRow>) -> ByteSize {
	post.as_ref().map_or(ByteSize::ZERO, |row| ByteSize::from_bytes(row.bytes().len() as u64))
}

pub(crate) fn state_entry_bytes(key: &EncodedKey, entry: &StateEntry) -> ByteSize {
	ByteSize::from_bytes(key.len() as u64).saturating_add(post_bytes(&entry.post))
}

fn split_bounded<K: Ord + Clone, V>(
	source: &mut BTreeMap<K, V>,
	budget: ByteSize,
	charge: impl Fn(&K, &V) -> ByteSize,
	force_first: bool,
) -> (BTreeMap<K, V>, ByteSize) {
	let mut taken = ByteSize::ZERO;
	let mut count = 0usize;
	let mut boundary: Option<K> = None;
	for (key, value) in source.iter() {
		let cost = charge(key, value);
		if !(force_first && count == 0) && taken.saturating_add(cost) > budget {
			boundary = Some(key.clone());
			break;
		}
		taken = taken.saturating_add(cost);
		count += 1;
	}
	let Some(boundary) = boundary else {
		return (mem::take(source), taken);
	};
	let remainder = source.split_off(&boundary);
	(mem::replace(source, remainder), taken)
}
