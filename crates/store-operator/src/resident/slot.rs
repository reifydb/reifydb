// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::{Deref, DerefMut};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::OperatorStateKey,
};
use reifydb_runtime::sync::mutex::Mutex;

use crate::resident::bucket::{BucketMap, write::WriteEntry};

pub struct ResidentState {
	pub operator: OperatorId,
	pub buckets: BucketMap,
}

impl ResidentState {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			buckets: BucketMap::default(),
		}
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<WriteEntry> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.buckets.get_bytes_ref(self.operator, keyspace, group, suffix)
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
		match fresh {
			true => self.buckets.record_bytes_fresh(operator, keyspace, group, suffix, post),
			false => self.buckets.record_bytes(operator, keyspace, group, suffix, post),
		}
	}

	pub fn erase_state(&mut self, key: &EncodedKey) -> bool {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return false;
		};
		let operator = self.operator;
		self.buckets.erase_bytes(operator, keyspace, group, suffix)
	}

	pub fn clear_state(&mut self) {
		self.buckets = BucketMap::default();
	}
}

pub struct SlotInner {
	pub resident: ResidentState,
	pub flow: Option<FlowId>,
	pub pending_seq: Option<u64>,
}

impl Deref for SlotInner {
	type Target = ResidentState;

	fn deref(&self) -> &Self::Target {
		&self.resident
	}
}

impl DerefMut for SlotInner {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.resident
	}
}

pub struct Slot {
	pub inner: Mutex<SlotInner>,
}

impl Slot {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			inner: Mutex::new(SlotInner {
				resident: ResidentState::new(operator),
				flow: None,
				pending_seq: None,
			}),
		}
	}
}
