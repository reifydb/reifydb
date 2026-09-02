// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::OperatorStateKey,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::tier::bucket::{BucketMap, write::WriteEntry};

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

	pub fn clear_state(&mut self) -> BucketMap {
		let taken = mem::take(&mut self.state);
		self.bytes = self.bytes.saturating_sub(taken.footprint());
		taken
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
