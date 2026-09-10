// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::OperatorStateKey,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::resident::bucket::{BucketMap, write::WriteEntry};

pub struct OperatorLive {
	pub operator: OperatorId,
	pub state: BucketMap,
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
			bytes: ByteSize::ZERO,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty()
	}

	pub fn entry_count(&self) -> usize {
		self.state.len()
	}

	pub fn dirty_count(&self) -> usize {
		self.state.dirty_len()
	}

	pub fn has_dirty(&self) -> bool {
		self.state.dirty_len() > 0
	}

	pub fn dirty_bytes(&self) -> ByteSize {
		self.state.dirty_footprint()
	}

	pub fn revert_flushing(&mut self) -> usize {
		self.state.revert_flushing()
	}

	pub fn settle_flushing(&mut self) {
		self.state.settle_flushing();
	}

	pub fn evict_clean(&mut self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize) {
		let (evicted, freed) = self.state.evict_clean(bytes, entries);
		self.bytes = self.bytes.saturating_sub(freed);
		(evicted, freed)
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.state.get_bytes_ref(self.operator, keyspace, group, suffix)
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.lookup(key).is_some()
	}

	pub fn is_deleted(&self, key: &EncodedKey) -> bool {
		self.lookup(key).is_some_and(|entry| entry.post.is_none())
	}

	pub fn entries(&self) -> Vec<(EncodedKey, WriteEntry)> {
		self.state.encoded_entries(self.operator)
	}

	pub fn record_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>) {
		self.write_state(key, post, false);
	}

	pub fn insert_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>) {
		self.write_state(key, post, true);
	}

	fn write_state(&mut self, key: EncodedKey, post: Option<EncodedPodRow>, fresh: bool) {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("an operator state key must decode as its own framing");
		let operator = self.operator;
		let before = self.state.footprint();
		match fresh {
			true => self.state.record_bytes_fresh(operator, keyspace, group, suffix, post),
			false => self.state.record_bytes(operator, keyspace, group, suffix, post),
		}
		let after = self.state.footprint();
		self.bytes = self.bytes.saturating_add(after).saturating_sub(before);
	}

	pub fn erase_state(&mut self, key: &EncodedKey) -> bool {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return false;
		};
		let operator = self.operator;
		let before = self.state.footprint();
		if !self.state.erase_bytes(operator, keyspace, group, suffix) {
			return false;
		}
		let after = self.state.footprint();
		self.bytes = self.bytes.saturating_add(after).saturating_sub(before);
		true
	}

	pub fn clear_state(&mut self) -> BucketMap {
		let taken = mem::take(&mut self.state);
		self.bytes = self.bytes.saturating_sub(taken.footprint());
		taken
	}
}

#[derive(Default)]
pub struct SlotInner {
	pub live: OperatorLive,
	pub flow: Option<FlowId>,
	pub pending_seq: Option<u64>,
}

impl SlotInner {
	pub fn resident_bytes(&self) -> ByteSize {
		self.live.bytes
	}

	pub fn resident_entries(&self) -> usize {
		self.live.entry_count()
	}

	pub fn dirty_entries(&self) -> usize {
		self.live.dirty_count()
	}

	pub fn dirty_bytes(&self) -> ByteSize {
		self.live.dirty_bytes()
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		self.live.lookup(key)
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
