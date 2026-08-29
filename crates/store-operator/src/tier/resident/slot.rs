// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::{GroupId, OperatorStateKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use crate::{
	tier::resident::batch::{JOIN_EXPIRY_ENTRY_BYTES, MAX_FREQUENCY, StateEntry, state_entry_bytes},
	types::{DurablePre, OperatorStateCensus},
};

pub type OperatorKeys = BTreeMap<EncodedKey, StateEntry>;

pub type SlotJoinKey = (GroupId, u8, RowNumber);

pub type SlotJoinExpiries = BTreeMap<SlotJoinKey, Option<u64>>;

#[derive(Debug, Default, Clone, Copy)]
pub struct StateBucket {
	pub keys: u64,
	pub key_bytes: ByteSize,
	pub value_bytes: ByteSize,
}

#[derive(Debug, Default)]
pub struct SlotCensus {
	pub state: BTreeMap<Option<u8>, StateBucket>,
	pub join_expiries: u64,
}

impl SlotCensus {
	pub fn admit_state(&mut self, key: &EncodedKey, value_bytes: u64) {
		let bucket = self.state.entry(keyspace_of(key)).or_default();
		bucket.keys += 1;
		bucket.key_bytes = bucket.key_bytes.saturating_add(ByteSize::from_bytes(key.len() as u64));
		bucket.value_bytes = bucket.value_bytes.saturating_add(ByteSize::from_bytes(value_bytes));
	}

	pub fn retract_state(&mut self, key: &EncodedKey, value_bytes: u64) {
		let Entry::Occupied(mut slot) = self.state.entry(keyspace_of(key)) else {
			return;
		};
		let bucket = slot.get_mut();
		bucket.keys = bucket.keys.saturating_sub(1);
		bucket.key_bytes = bucket.key_bytes.saturating_sub(ByteSize::from_bytes(key.len() as u64));
		bucket.value_bytes = bucket.value_bytes.saturating_sub(ByteSize::from_bytes(value_bytes));
		if bucket.keys == 0 {
			slot.remove();
		}
	}

	pub fn admit_join_expiry(&mut self) {
		self.join_expiries += 1;
	}

	pub fn retract_join_expiry(&mut self) {
		self.join_expiries = self.join_expiries.saturating_sub(1);
	}

	pub fn entries(&self, operator: OperatorId) -> Vec<OperatorStateCensus> {
		self.state
			.iter()
			.map(|(stored, bucket)| {
				let stored = stored.expect("state keys carry a keyspace byte");
				OperatorStateCensus {
					operator,
					keyspace: OperatorStateKey::decode_keyspace(stored),
					keys: bucket.keys,
					key_bytes: bucket.key_bytes,
					value_bytes: bucket.value_bytes,
				}
			})
			.collect()
	}
}

pub fn keyspace_of(key: &EncodedKey) -> Option<u8> {
	key.as_slice().get(OperatorStateKey::KEYSPACE_INNER_OFFSET as usize).copied()
}

#[derive(Debug, Default)]
pub struct OperatorLive {
	pub state: OperatorKeys,
	pub join_expiries: SlotJoinExpiries,
	pub durable_join_expiries: BTreeSet<SlotJoinKey>,
	pub bytes: ByteSize,
}

impl OperatorLive {
	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.join_expiries.is_empty()
	}

	pub fn record_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>, durable_pre: DurablePre) {
		let incoming = post_bytes(&post);
		let key_bytes = ByteSize::from_bytes(key.len() as u64);
		let mut admitted = false;
		let outgoing = match self.state.entry(key) {
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
			self.bytes = self.bytes.saturating_add(key_bytes);
		}
		self.bytes = self.bytes.saturating_sub(outgoing).saturating_add(incoming);
	}

	pub fn record_join_expiry(&mut self, key: SlotJoinKey, expiry: Option<u64>, durable: bool) {
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

	pub fn clear_state(&mut self, census: &mut SlotCensus) -> OperatorKeys {
		let keys = std::mem::take(&mut self.state);
		for (key, entry) in keys.iter() {
			self.bytes = self.bytes.saturating_sub(state_entry_bytes(key, entry));
			if let Some(row) = &entry.post {
				census.retract_state(key, row.bytes().len() as u64);
			}
		}
		keys
	}

	pub fn retain_join_expiries(&mut self, keep: impl Fn(&SlotJoinKey) -> bool, census: &mut SlotCensus) {
		let bytes = &mut self.bytes;
		let mut retracted = 0u64;
		self.join_expiries.retain(|key, entry| {
			if keep(key) {
				return true;
			}
			*bytes = bytes.saturating_sub(JOIN_EXPIRY_ENTRY_BYTES);
			if entry.is_some() {
				retracted += 1;
			}
			false
		});
		for _ in 0..retracted {
			census.retract_join_expiry();
		}
		self.durable_join_expiries.retain(&keep);
	}
}

#[derive(Debug, Default)]
pub struct SlotInner {
	pub live: OperatorLive,
	pub in_flight: Option<std::sync::Arc<OperatorLive>>,
	pub census: SlotCensus,
	pub flow: Option<FlowId>,
	pub pending_seq: Option<u64>,
	pub durable_position: Option<CommitVersion>,
}

impl SlotInner {
	pub fn resident_bytes(&self) -> ByteSize {
		self.live
			.bytes
			.saturating_add(self.in_flight.as_ref().map_or(ByteSize::ZERO, |batch| batch.bytes))
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<&StateEntry> {
		match self.live.state.get(key) {
			Some(entry) => Some(entry),
			None => self.in_flight.as_ref()?.state.get(key),
		}
	}

	pub fn merged_value_bytes(&self, key: &EncodedKey) -> Option<u64> {
		self.lookup(key).and_then(|entry| entry.post.as_ref()).map(|row| row.bytes().len() as u64)
	}

	pub fn merged_join_expiry(&self, key: &SlotJoinKey) -> bool {
		match self.live.join_expiries.get(key) {
			Some(entry) => entry.is_some(),
			None => self
				.in_flight
				.as_ref()
				.and_then(|batch| batch.join_expiries.get(key))
				.is_some_and(|entry| entry.is_some()),
		}
	}
}

#[derive(Debug, Default)]
pub struct Slot {
	pub inner: Mutex<SlotInner>,
}

fn post_bytes(post: &Option<EncodedPodRow>) -> ByteSize {
	post.as_ref().map_or(ByteSize::ZERO, |row| ByteSize::from_bytes(row.bytes().len() as u64))
}
