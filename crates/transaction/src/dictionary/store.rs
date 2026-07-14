// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::change::Change,
	internal_error,
	key::{EncodableKey, dictionary::DictionaryEntryIndexKey},
};
use reifydb_value::{Result, value::dictionary::DictionaryId};

use crate::multi::{RangeScope, transaction::MultiTransaction};

pub trait DictionaryStore: Send + Sync {
	fn read_committed(&self, key: &EncodedKey) -> Result<Option<EncodedRow>>;

	fn max_index_id(&self, dictionary: DictionaryId) -> Result<Option<u128>>;

	fn commit_entries(&self, writes: &[DictEntryWrite], changes: Vec<Change>) -> Result<CommitVersion>;
}

pub struct DictEntryWrite {
	pub entry_key: EncodedKey,
	pub entry_value: EncodedRow,
	pub index_key: EncodedKey,
	pub index_value: EncodedRow,
}

pub struct UnconfiguredDictionaryStore;

impl DictionaryStore for UnconfiguredDictionaryStore {
	fn read_committed(&self, _key: &EncodedKey) -> Result<Option<EncodedRow>> {
		Err(internal_error!("dictionary store is not configured"))
	}

	fn max_index_id(&self, _dictionary: DictionaryId) -> Result<Option<u128>> {
		Err(internal_error!("dictionary store is not configured"))
	}

	fn commit_entries(&self, _writes: &[DictEntryWrite], _changes: Vec<Change>) -> Result<CommitVersion> {
		Err(internal_error!("dictionary store is not configured"))
	}
}

pub struct MultiDictionaryStore {
	multi: MultiTransaction,
	reads: AtomicU64,
}

impl MultiDictionaryStore {
	pub fn new(multi: MultiTransaction) -> Self {
		Self {
			multi,
			reads: AtomicU64::new(0),
		}
	}

	pub fn read_count(&self) -> u64 {
		self.reads.load(Ordering::Relaxed)
	}
}

impl DictionaryStore for MultiDictionaryStore {
	fn read_committed(&self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.reads.fetch_add(1, Ordering::Relaxed);
		let query = self.multi.begin_query()?;
		Ok(query.get_at_latest(key)?.map(|value| value.row().clone()))
	}

	fn max_index_id(&self, dictionary: DictionaryId) -> Result<Option<u128>> {
		let query = self.multi.begin_query()?;
		let range = DictionaryEntryIndexKey::full_scan(dictionary);
		let mut iter = query.range(range, RangeScope::All, 1);
		match iter.next() {
			Some(result) => Ok(DictionaryEntryIndexKey::decode(&result?.key).map(|key| key.id)),
			None => Ok(None),
		}
	}

	fn commit_entries(&self, writes: &[DictEntryWrite], changes: Vec<Change>) -> Result<CommitVersion> {
		debug_assert!(!writes.is_empty(), "commit_entries must not be called with no writes");

		let mut txn = self.multi.begin_command()?;
		txn.disable_conflict_tracking();
		for write in writes {
			txn.set(&write.entry_key, write.entry_value.clone())?;
			txn.set(&write.index_key, write.index_value.clone())?;
		}
		txn.commit_unchecked(changes)
	}
}
