// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
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

use crate::tier::{
	bucket::{BucketMap, write::WriteEntry},
	resident::batch::JOIN_EXPIRY_ENTRY_BYTES,
};

pub type SlotJoinKey = (GroupId, u8, RowNumber);

pub type SlotJoinExpiries = BTreeMap<SlotJoinKey, Option<u64>>;

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

	pub fn entry_count(&self) -> usize {
		self.state.len().saturating_add(self.join_expiries.len())
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.state.get_bytes_ref(self.operator, keyspace, group, suffix)
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.lookup(key).is_some()
	}

	pub fn entries(&self) -> Vec<(EncodedKey, WriteEntry)> {
		self.state.encoded_entries(self.operator)
	}

	pub fn record_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>) {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("an operator state key must decode as its own framing");
		let operator = self.operator;
		let before = self.state.footprint();
		self.state.record_bytes(operator, keyspace, group, suffix, post);
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

	pub fn clear_state(&mut self) -> BucketMap {
		let taken = mem::take(&mut self.state);
		self.bytes = self.bytes.saturating_sub(taken.footprint());
		taken
	}

	pub fn retain_join_expiries(&mut self, keep: impl Fn(&SlotJoinKey) -> bool) {
		let bytes = &mut self.bytes;
		self.join_expiries.retain(|key, _| {
			if keep(key) {
				return true;
			}
			*bytes = bytes.saturating_sub(JOIN_EXPIRY_ENTRY_BYTES);
			false
		});
		self.durable_join_expiries.retain(&keep);
	}
}

#[derive(Default)]
pub struct SlotInner {
	pub live: OperatorLive,
	pub in_flight: Option<Arc<OperatorLive>>,
	pub flow: Option<FlowId>,
	pub pending_seq: Option<u64>,
	pub durable_position: Option<CommitVersion>,
}

impl SlotInner {
	pub fn resident_bytes(&self) -> ByteSize {
		self.live.bytes.saturating_add(self.in_flight.as_ref().map_or(ByteSize::ZERO, |batch| batch.bytes))
	}

	pub fn resident_entries(&self) -> usize {
		self.live.entry_count().saturating_add(self.in_flight.as_ref().map_or(0, |batch| batch.entry_count()))
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		match self.live.lookup(key) {
			Some(entry) => Some(entry),
			None => self.in_flight.as_ref()?.lookup(key),
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
