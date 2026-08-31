// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, btree_map::Entry},
	mem,
	sync::Arc,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, OperatorStateKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use crate::{
	tier::{
		bucket::{BucketMap, write::WriteEntry},
		resident::batch::JOIN_EXPIRY_ENTRY_BYTES,
	},
	types::{DurablePre, OperatorStateCensus},
};

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

pub struct OperatorLive {
	pub operator: OperatorId,
	pub state: BucketMap,
	pub join_expiries: SlotJoinExpiries,
	pub durable_join_expiries: BTreeSet<SlotJoinKey>,
	pub bytes: ByteSize,
}

impl Default for OperatorLive {
	fn default() -> Self {
		Self::new(OperatorId(0))
	}
}

impl OperatorLive {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			state: BucketMap::default(),
			join_expiries: SlotJoinExpiries::new(),
			durable_join_expiries: BTreeSet::new(),
			bytes: ByteSize::ZERO,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.join_expiries.is_empty()
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.state.get_bytes_ref(self.operator, keyspace, group, &suffix)
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.lookup(key).is_some()
	}

	pub fn entries(&self) -> Vec<(EncodedKey, WriteEntry)> {
		self.state.encoded_entries(self.operator)
	}

	pub fn record_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>, durable_pre: DurablePre) {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("an operator state key must decode as its own framing");
		let operator = self.operator;
		let before = self.state.footprint();
		self.state.record_bytes(operator, keyspace, group, &suffix, post, durable_pre);
		let after = self.state.footprint();
		self.bytes = self.bytes.saturating_add(after).saturating_sub(before);
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

	pub fn clear_state(&mut self, census: &mut SlotCensus) -> BucketMap {
		let taken = mem::take(&mut self.state);
		self.bytes = self.bytes.saturating_sub(taken.footprint());
		for (key, entry) in taken.encoded_entries(self.operator) {
			if let Some(row) = &entry.post {
				census.retract_state(&key, row.bytes().len() as u64);
			}
		}
		taken
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

#[derive(Default)]
pub struct SlotInner {
	pub live: OperatorLive,
	pub in_flight: Option<Arc<OperatorLive>>,
	pub census: SlotCensus,
	pub flow: Option<FlowId>,
	pub pending_seq: Option<u64>,
	pub durable_position: Option<CommitVersion>,
}

impl SlotInner {
	pub fn resident_bytes(&self) -> ByteSize {
		self.live.bytes.saturating_add(self.in_flight.as_ref().map_or(ByteSize::ZERO, |batch| batch.bytes))
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		match self.live.lookup(key) {
			Some(entry) => Some(entry),
			None => self.in_flight.as_ref()?.lookup(key),
		}
	}

	pub fn merged_value_bytes(&self, key: &EncodedKey) -> Option<u64> {
		self.lookup(key).and_then(|entry| entry.post).map(|row| row.len() as u64)
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

#[derive(Default)]
pub struct Slot {
	pub inner: Mutex<SlotInner>,
}

impl Slot {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			inner: Mutex::new(SlotInner {
				live: OperatorLive::new(operator),
				..SlotInner::default()
			}),
		}
	}
}
