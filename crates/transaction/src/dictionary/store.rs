// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::store::{SingleVersionGet, SingleVersionRange},
	internal_error,
	key::{
		EncodableKey,
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey},
	},
};
use reifydb_value::{Result, value::dictionary::DictionaryId};

use crate::single::SingleTransaction;

pub trait DictionaryStore: Send + Sync {
	fn read_committed(&self, key: &EncodedKey) -> Result<Option<EncodedRow>>;

	fn max_index_id(&self, dictionary: DictionaryId) -> Result<Option<u128>>;

	fn commit_entries(&self, dictionary: DictionaryId, writes: &[DictEntryWrite]) -> Result<()>;
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

	fn commit_entries(&self, _dictionary: DictionaryId, _writes: &[DictEntryWrite]) -> Result<()> {
		Err(internal_error!("dictionary store is not configured"))
	}
}

pub struct SingleDictionaryStore {
	single: SingleTransaction,
	reads: AtomicU64,
}

impl SingleDictionaryStore {
	pub fn new(single: SingleTransaction) -> Self {
		Self {
			single,
			reads: AtomicU64::new(0),
		}
	}

	pub fn read_count(&self) -> u64 {
		self.reads.load(Ordering::Relaxed)
	}
}

impl DictionaryStore for SingleDictionaryStore {
	fn read_committed(&self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.reads.fetch_add(1, Ordering::Relaxed);
		let store = self.single.read_store();
		Ok(SingleVersionGet::get(&store, key)?.map(|row| row.row))
	}

	fn max_index_id(&self, dictionary: DictionaryId) -> Result<Option<u128>> {
		let store = self.single.read_store();
		let batch = SingleVersionRange::range_batch(&store, DictionaryEntryIndexKey::full_scan(dictionary), 1)?;
		match batch.items.first() {
			Some(row) => Ok(DictionaryEntryIndexKey::decode(&row.key).map(|key| key.id)),
			None => Ok(None),
		}
	}

	fn commit_entries(&self, dictionary: DictionaryId, writes: &[DictEntryWrite]) -> Result<()> {
		debug_assert!(!writes.is_empty(), "commit_entries must not be called with no writes");

		let lock_key = DictionaryKey::encoded(dictionary);
		let ranges =
			vec![DictionaryEntryKey::full_scan(dictionary), DictionaryEntryIndexKey::full_scan(dictionary)];
		let mut txn = self.single.begin_command_ranged([&lock_key], ranges)?;
		for write in writes {
			txn.set(&write.index_key, write.index_value.clone())?;
			txn.set(&write.entry_key, write.entry_value.clone())?;
		}
		txn.commit()?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use postcard::to_stdvec;
	use reifydb_core::interface::catalog::{dictionary::Dictionary, id::NamespaceId};
	use reifydb_value::value::{Value, dictionary::DictionaryId, value_type::ValueType};

	use super::SingleDictionaryStore;
	use crate::{dictionary::DictionaryAllocatorRegistry, single::SingleTransaction};

	fn mints() -> Dictionary {
		Dictionary {
			id: DictionaryId(16385),
			namespace: NamespaceId::SYSTEM,
			name: "mints".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint4,
		}
	}

	// The id an intern hands back has a committed entry in the single store. A registry with a cold
	// cache (a rebuilt flow engine, a restarted process before eviction) must resolve that entry
	// through the store instead of minting a second id for the same value.
	#[test]
	fn a_cold_registry_resolves_an_interned_value_through_the_single_store() {
		let single = SingleTransaction::testing();
		let dictionary = mints();
		let value = Value::Utf8("GvUCjmWSXA5hrTh9smmNA1AU55YCtP9mDLQcrKA1pump".to_string());

		let warm = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));
		let first = warm.intern(&dictionary, &value).unwrap();
		assert!(first.created, "first intern must create a new entry");

		let cold = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));
		let second = cold.intern(&dictionary, &value).unwrap();

		assert_eq!(
			second.id, first.id,
			"a committed entry must be visible to a cold registry through the single store"
		);
		assert!(!second.created, "a cold registry must resolve the durable entry, not remint it");

		let bytes = cold.get(&dictionary, first.id.to_u128()).unwrap().expect("index row must resolve");
		assert_eq!(
			bytes.as_ref(),
			to_stdvec(&value).unwrap().as_slice(),
			"the index row must decode back to the interned value"
		);
	}
}
