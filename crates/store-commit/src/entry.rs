// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, btree_set},
	mem::size_of,
	ops::RangeBounds,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, metrics::heap::HeapSize};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

pub(super) type Value = Option<CowVec<u8>>;

pub(super) const ENTRY_OVERHEAD: usize = size_of::<EncodedKey>() + size_of::<CommitVersion>() + size_of::<Value>();

pub(super) fn entry_bytes(key: &EncodedKey, value: &Value) -> u64 {
	entry_bytes_with(key.heap_size(), value)
}

pub(super) fn entry_bytes_with(key_heap: usize, value: &Value) -> u64 {
	(ENTRY_OVERHEAD + key_heap + value.as_ref().map_or(0, |bytes| bytes.len())) as u64
}

pub(super) fn value_bytes_of(value: &Value) -> ByteSize {
	ByteSize::from_bytes(value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
}

#[derive(Default)]
pub(super) struct QueuedKeys {
	keys: BTreeSet<EncodedKey>,
	bytes: usize,
}

impl QueuedKeys {
	fn key_bytes(key: &EncodedKey) -> usize {
		size_of::<EncodedKey>() + key.heap_size()
	}

	pub fn bytes(&self) -> usize {
		self.bytes
	}

	pub fn len(&self) -> usize {
		self.keys.len()
	}

	pub fn insert(&mut self, key: EncodedKey) {
		let bytes = Self::key_bytes(&key);
		if self.keys.insert(key) {
			self.bytes += bytes;
		}
	}

	pub fn extend(&mut self, keys: impl IntoIterator<Item = EncodedKey>) {
		for key in keys {
			self.insert(key);
		}
	}

	pub fn remove(&mut self, key: &EncodedKey) {
		if self.keys.remove(key) {
			self.bytes -= Self::key_bytes(key);
		}
	}

	pub fn pop_first(&mut self) -> Option<EncodedKey> {
		let key = self.keys.pop_first()?;
		self.bytes -= Self::key_bytes(&key);
		Some(key)
	}

	pub fn iter(&self) -> btree_set::Iter<'_, EncodedKey> {
		self.keys.iter()
	}

	pub fn range(&self, range: impl RangeBounds<EncodedKey>) -> btree_set::Range<'_, EncodedKey> {
		self.keys.range(range)
	}
}

pub(super) struct Entry<R> {
	pub rows: R,

	pub pending: Mutex<QueuedKeys>,

	pub retained: Mutex<QueuedKeys>,

	pub retained_cursor: Mutex<Option<EncodedKey>>,
}

impl<R> Entry<R> {
	pub fn new(rows: R) -> Self {
		Self {
			rows,
			pending: Mutex::new(QueuedKeys::default()),
			retained: Mutex::new(QueuedKeys::default()),
			retained_cursor: Mutex::new(None),
		}
	}

	pub fn unqueue(&self, keys: &[EncodedKey]) {
		if keys.is_empty() {
			return;
		}
		let mut pending = self.pending.lock();
		for key in keys {
			pending.remove(key);
		}
		drop(pending);
		let mut retained = self.retained.lock();
		for key in keys {
			retained.remove(key);
		}
	}
}
