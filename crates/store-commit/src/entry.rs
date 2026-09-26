// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, mem::size_of};

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

pub(super) struct Entry<R> {
	pub rows: R,

	pub pending: Mutex<BTreeSet<EncodedKey>>,

	pub retained: Mutex<BTreeSet<EncodedKey>>,

	pub retained_cursor: Mutex<Option<EncodedKey>>,
}

impl<R> Entry<R> {
	pub fn new(rows: R) -> Self {
		Self {
			rows,
			pending: Mutex::new(BTreeSet::new()),
			retained: Mutex::new(BTreeSet::new()),
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
