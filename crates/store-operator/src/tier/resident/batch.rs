// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, btree_map::Entry},
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
	tier::resident::{census::BufferCensus, state_map::StateMap},
	types::{DurablePre, JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES},
};

pub type StateKey = (OperatorId, EncodedKey);

pub type JoinExpiryKey = (OperatorId, GroupId, u8, RowNumber);

pub type JoinExpirySlot = (u8, RowNumber);

pub const JOIN_EXPIRY_ENTRY_BYTES: ByteSize = JOIN_EXPIRY_KEY_BYTES.saturating_add(JOIN_EXPIRY_VALUE_BYTES);

pub const MAX_FREQUENCY: u8 = 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
	JoinExpiriesOperator(OperatorId),
	JoinExpiriesGroup(OperatorId, GroupId),
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
	pub join_expiries: BTreeMap<JoinExpiryKey, Option<u64>>,
	pub durable_join_expiries: BTreeSet<JoinExpiryKey>,
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

	pub(crate) fn record_join_expiry(&mut self, key: JoinExpiryKey, expiry: Option<u64>, durable: bool) {
		if durable && !self.join_expiries.contains_key(&key) {
			self.durable_join_expiries.insert(key);
		}
		if expiry.is_none() && !self.durable_join_expiries.contains(&key) {
			if self.join_expiries.remove(&key).is_some() {
				self.bytes = self.bytes.saturating_sub(JOIN_EXPIRY_ENTRY_BYTES);
			}
			return;
		}
		if self.join_expiries.insert(key, expiry).is_none() {
			self.bytes = self.bytes.saturating_add(JOIN_EXPIRY_ENTRY_BYTES);
		}
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty()
			&& self.join_expiries.is_empty()
			&& self.checkpoints.is_empty()
			&& self.drops.is_empty()
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
		let (join_expiries, join_expiry_bytes) = split_bounded(
			&mut self.join_expiries,
			budget.saturating_sub(state_bytes),
			|_, _| JOIN_EXPIRY_ENTRY_BYTES,
			state.is_empty(),
		);
		for (key, entry) in &join_expiries {
			match entry {
				Some(_) => self.durable_join_expiries.insert(*key),
				None => self.durable_join_expiries.remove(key),
			};
		}
		let bytes = state_bytes.saturating_add(join_expiry_bytes);
		self.bytes = self.bytes.saturating_sub(bytes);
		FlushBatch {
			state,
			join_expiries,
			durable_join_expiries: BTreeSet::new(),
			checkpoints: mem::take(&mut self.checkpoints),
			drops: mem::take(&mut self.drops),
			bytes,
		}
	}

	pub(super) fn clear_drop(&mut self, marker: DropMarker, census: &mut BufferCensus) {
		match marker {
			DropMarker::OperatorState(operator) => {
				self.drop_state(operator, census);
				self.retain_join_expiries(|(candidate, _, _, _)| *candidate != operator, census);
			}
			DropMarker::JoinExpiriesOperator(operator) => {
				self.retain_join_expiries(|(candidate, _, _, _)| *candidate != operator, census);
			}
			DropMarker::JoinExpiriesGroup(operator, group) => {
				self.retain_join_expiries(
					|(candidate, candidate_group, _, _)| {
						*candidate != operator || *candidate_group != group
					},
					census,
				);
			}
		}
	}

	fn drop_state(&mut self, operator: OperatorId, census: &mut BufferCensus) {
		let Some(keys) = self.state.remove_operator(operator) else {
			return;
		};
		for (key, entry) in keys.iter() {
			self.bytes = self.bytes.saturating_sub(state_entry_bytes(key, entry));
			if let Some(row) = &entry.post {
				census.retract_state(operator, key, row.bytes().len() as u64);
			}
		}
	}

	fn retain_join_expiries(&mut self, keep: impl Fn(&JoinExpiryKey) -> bool, census: &mut BufferCensus) {
		let bytes = &mut self.bytes;
		self.join_expiries.retain(|key, entry| {
			if keep(key) {
				return true;
			}
			*bytes = bytes.saturating_sub(JOIN_EXPIRY_ENTRY_BYTES);
			if entry.is_some() {
				census.retract_join_expiry(key.0);
			}
			false
		});
		self.durable_join_expiries.retain(&keep);
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
	let mut boundary: Option<K> = None;
	for (count, (key, value)) in source.iter().enumerate() {
		let cost = charge(key, value);
		if !(force_first && count == 0) && taken.saturating_add(cost) > budget {
			boundary = Some(key.clone());
			break;
		}
		taken = taken.saturating_add(cost);
	}
	let Some(boundary) = boundary else {
		return (mem::take(source), taken);
	};
	let remainder = source.split_off(&boundary);
	(mem::replace(source, remainder), taken)
}
