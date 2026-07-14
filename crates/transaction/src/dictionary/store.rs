// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::{change::Change, store::classify_key},
	internal_error,
	key::{EncodableKey, dictionary::DictionaryEntryIndexKey},
};
use reifydb_store_multi::tier::TierBatch;
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
		let version = txn.commit_unchecked(changes)?;
		self.persist_now(writes, version)?;
		Ok(version)
	}
}

impl MultiDictionaryStore {
	fn persist_now(&self, writes: &[DictEntryWrite], version: CommitVersion) -> Result<()> {
		let Some(persistent) = self.multi.store.persistent() else {
			return Ok(());
		};

		let mut batch = TierBatch::default();
		for write in writes {
			batch.entry(classify_key(&write.entry_key))
				.or_default()
				.push((write.entry_key.clone(), Some(write.entry_value.0.clone())));
			batch.entry(classify_key(&write.index_key))
				.or_default()
				.push((write.index_key.clone(), Some(write.index_value.0.clone())));
		}

		persistent.persist_sweep(vec![(version, batch)])?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{mem, sync::Arc};

	use postcard::to_stdvec;
	use reifydb_core::{
		event::EventBus,
		interface::{
			catalog::{
				config::{ConfigKey, GetConfig},
				dictionary::Dictionary,
				id::NamespaceId,
			},
			store::EntryKind,
		},
		key::dictionary::DictionaryEntryKey,
	};
	use reifydb_runtime::{
		actor::system::ActorSystem,
		context::{
			clock::{Clock, MockClock},
			rng::Rng,
		},
		pool::{PoolConfig, Pools},
	};
	use reifydb_store_multi::{
		MultiStore,
		tier::{TierStorage, VersionedGetResult},
	};
	use reifydb_store_single::SingleStore;
	use reifydb_value::{
		util::hash::xxh3_128,
		value::{Value, dictionary::DictionaryId, value_type::ValueType},
	};

	use super::{CommitVersion, MultiDictionaryStore};
	use crate::{
		dictionary::DictionaryAllocatorRegistry, multi::transaction::MultiTransaction,
		single::SingleTransaction,
	};

	struct DummyConfig;

	impl GetConfig for DummyConfig {
		fn get_config(&self, key: ConfigKey) -> Value {
			key.default_value()
		}
		fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
			key.default_value()
		}
	}

	fn mints() -> Dictionary {
		Dictionary {
			id: DictionaryId(16385),
			namespace: NamespaceId::SYSTEM,
			name: "mints".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint4,
		}
	}

	#[test]
	fn interned_entry_is_persistent_without_a_flush_sweep() {
		let (multi_store, _guard) = MultiStore::testing_memory_with_persistent_sqlite();
		let single_store = SingleStore::testing_memory();
		let actor_system = ActorSystem::new(Pools::new(PoolConfig::sync_only()), Clock::Real);
		let spawner = actor_system.spawner();
		mem::forget(actor_system);
		let event_bus = EventBus::new(&spawner);

		let multi = MultiTransaction::new(
			multi_store,
			SingleTransaction::new(single_store, event_bus.clone()),
			event_bus,
			spawner,
			Clock::Mock(MockClock::from_millis(1000)),
			Rng::seeded(42),
			Arc::new(DummyConfig),
		)
		.unwrap();

		let registry = DictionaryAllocatorRegistry::new(Arc::new(MultiDictionaryStore::new(multi.clone())));

		let dictionary = mints();
		let mint = "GvUCjmWSXA5hrTh9smmNA1AU55YCtP9mDLQcrKA1pump";
		let value = Value::Utf8(mint.to_string());

		let batch = registry.intern(&dictionary, &value).unwrap();
		assert!(batch.outcomes[0].created, "first intern of {mint} must create a new entry");

		let hash = xxh3_128(&to_stdvec(&value).unwrap()).0.to_be_bytes();
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);

		let persistent = multi.store.persistent().expect("test store must have a persistent tier");
		let version = multi.version().unwrap();

		assert!(
			matches!(
				persistent.get(EntryKind::Multi, entry_key.as_ref(), version).unwrap(),
				VersionedGetResult::Value { .. }
			),
			"entry for {mint} must be on the persistent tier as soon as intern returns its id, \
			 with no flush sweep having run; an entry that only reaches the commit buffer is lost \
			 on an abrupt kill while the rows carrying its id survive"
		);
	}
}
