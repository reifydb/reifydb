// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::dictionary::Dictionary,
	key::{
		EncodableKey,
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey},
	},
};
use reifydb_transaction::{dictionary::DictionaryReader, multi::RangeScope};
use reifydb_value::{
	Result,
	util::hash::xxh3_128,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use super::FlowTransaction;

impl DictionaryReader for FlowTransaction {
	fn read(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.get(key)
	}

	fn read_latest(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let inner = self.inner();
		let query = inner.dictionary_query.as_ref().unwrap_or(&inner.query);
		Ok(query.get_at_latest(key)?.map(|value| value.row().clone()))
	}

	fn max_index_id(&mut self, dictionary: DictionaryId) -> Result<Option<u128>> {
		let range = DictionaryEntryIndexKey::full_scan(dictionary);
		let mut iter = self.range(range, RangeScope::All, 1);
		match iter.next() {
			Some(result) => Ok(DictionaryEntryIndexKey::decode(&result?.key).map(|key| key.id)),
			None => Ok(None),
		}
	}
}

impl FlowTransaction {
	pub fn find_dictionary(&self, id: DictionaryId) -> Option<Dictionary> {
		self.catalog().cache().find_dictionary_at(id, self.version())
	}

	pub fn find_dictionary_by_name(&self, name: &str) -> Option<Dictionary> {
		let version = self.version();
		let (namespace_name, dictionary_name) = name.rsplit_once("::")?;
		let namespace = self.catalog().cache().find_namespace_by_name_at(namespace_name, version)?;
		self.catalog().cache().find_dictionary_by_name_at(namespace.id(), dictionary_name, version)
	}

	pub fn find_in_dictionary(
		&mut self,
		dictionary: &Dictionary,
		value: &Value,
	) -> Result<Option<DictionaryEntryId>> {
		let value_bytes = to_stdvec(value).expect("failed to serialize dictionary value");
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		if let Some(v) = self.get(&entry_key)? {
			let id = u128::from_be_bytes(v.0[..16].try_into().unwrap());
			return Ok(Some(DictionaryEntryId::from_u128(id, dictionary.id_type.clone())?));
		}

		match self.dictionary_allocators().reserved_writes(dictionary, &hash, &value_bytes) {
			Some(writes) => {
				let id = u128::from_be_bytes(writes.entry_value.0[..16].try_into().unwrap());
				self.set(&writes.entry_key, writes.entry_value)?;
				self.set(&writes.index_key, writes.index_value)?;
				Ok(Some(DictionaryEntryId::from_u128(id, dictionary.id_type.clone())?))
			}
			None => Ok(None),
		}
	}

	pub fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		let index_key = DictionaryEntryIndexKey::new(dictionary.id, id.to_u128()).encode();
		match self.get(&index_key)? {
			Some(v) => Ok(Some(from_bytes(&v.0).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use postcard::to_stdvec;
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		actors::pending::{Pending, PendingWrite},
		interface::catalog::{dictionary::Dictionary, id::NamespaceId},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::{dictionary::DictionaryAllocatorRegistry, interceptor::interceptors::Interceptors};
	use reifydb_value::{
		util::hash::xxh3_128,
		value::{Value, dictionary::DictionaryId, identity::IdentityId, value_type::ValueType},
	};

	use crate::transaction::{DeferredParams, FlowTransaction, allocators::FlowAllocators};

	fn mints() -> Dictionary {
		Dictionary {
			id: DictionaryId(1),
			namespace: NamespaceId::SYSTEM,
			name: "mints".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint4,
		}
	}

	// Reproduces the production crash: one flow interns a first-seen mint into the shared allocator
	// (a reservation, not yet committed and not in any other transaction's pending), and a second
	// deferred flow that shares the same registry - as a downstream cascade does within one
	// uncommitted consume cycle - must resolve that mint. Its committed-version snapshot and its own
	// empty pending both miss it, so before the reservation read-through find_in_dictionary returned
	// None and the downstream mint operator aborted the process. A value that was never interned
	// still resolves to None, proving the Some result comes from the reservation, not a false hit.
	#[test]
	fn resolves_a_mint_interned_by_a_concurrent_flow_before_commit() {
		let engine = TestEngine::new();
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let dictionary = mints();

		let value = Value::Utf8("CuGJf6cfDfMh4UxVgNJ5KFQ6v8Wv3qrqop6cFKsGpump".to_string());
		let value_bytes = to_stdvec(&value).unwrap();

		let registry = DictionaryAllocatorRegistry::new();
		let outcome = {
			let mut reader = parent.multi.begin_query().unwrap();
			registry.intern(&dictionary, &value_bytes, &mut reader).unwrap()
		};
		assert!(outcome.writes.is_some(), "a first-seen mint must reserve a fresh id");

		let version = parent.version();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			dictionary_query: Some(parent.multi.begin_query().unwrap()),
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			allocators: FlowAllocators::with_dictionary(registry.clone()),
		});

		let unknown = Value::Utf8("never-interned-mint".to_string());
		assert_eq!(
			txn.find_in_dictionary(&dictionary, &unknown).unwrap(),
			None,
			"a mint with no committed entry and no reservation must resolve to None"
		);

		let found = txn.find_in_dictionary(&dictionary, &value).unwrap();
		assert_eq!(
			found.map(|id| id.to_u128()),
			Some(outcome.id.to_u128()),
			"a downstream flow must resolve a mint interned by a concurrent flow in the same cycle"
		);
	}

	// The restart crash loop, end to end. A first-seen mint is interned into the shared allocator as a
	// reservation only (never committed). A deferred flow resolves it and - as an operator would -
	// its referencing state is durably committed alongside whatever the resolve staged. Then the
	// process restarts: the in-memory reservation is gone, but the durable state that referenced the
	// id survives. Resolving the mint must still succeed, through its now-durable entry, instead of
	// returning None and aborting the operator. Without the co-write on resolve the flow persists the
	// reference but not the entry, and this resolve returns None on the fresh, empty registry -
	// exactly the production crash.
	#[test]
	fn resolving_a_reserved_mint_persists_it_so_a_restart_still_resolves() {
		let engine = TestEngine::new();
		let dictionary = mints();
		let value = Value::Utf8("CuGJf6cfDfMh4UxVgNJ5KFQ6v8Wv3qrqop6cFKsGpump".to_string());
		let value_bytes = to_stdvec(&value).unwrap();
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		// Session 1: an intern leaves only an in-memory reservation - nothing is committed.
		let registry = DictionaryAllocatorRegistry::new();
		let reserved_id = {
			let parent = engine.begin_admin(IdentityId::system()).unwrap();
			let mut reader = parent.multi.begin_query().unwrap();
			registry.intern(&dictionary, &value_bytes, &mut reader).unwrap().id.to_u128()
		};

		// A deferred flow resolves that mint via the reservation and co-writes its durable entry into
		// the flow transaction's pending.
		let pending = {
			let parent = engine.begin_admin(IdentityId::system()).unwrap();
			let version = parent.version();
			let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
				version,
				pending: Pending::new(),
				base_pending: Arc::new(Pending::new()),
				query: parent.multi.begin_query().unwrap(),
				state_query: parent.multi.begin_query().unwrap(),
				dictionary_query: Some(parent.multi.begin_query().unwrap()),
				single: parent.single.clone(),
				catalog: Catalog::testing(),
				interceptors: Interceptors::new(),
				clock: Clock::Mock(MockClock::from_millis(0)),
				allocators: FlowAllocators::with_dictionary(registry.clone()),
			});
			let id = txn
				.find_in_dictionary(&dictionary, &value)
				.unwrap()
				.expect("the reservation must resolve");
			assert_eq!(id.to_u128(), reserved_id, "the flow must resolve the reserved id");
			txn.take_pending()
		};
		assert!(
			pending.iter_sorted().next().is_some(),
			"resolving a reserved mint must stage its durable dictionary entry into the flow transaction"
		);

		// Persist exactly what the flow staged - what the committer applies on commit.
		{
			let mut admin = engine.begin_admin(IdentityId::system()).unwrap();
			for (key, pw) in pending.iter_sorted() {
				if let PendingWrite::Set(row) = pw {
					admin.set(key, row.clone()).unwrap();
				}
			}
			admin.commit().unwrap();
		}

		// Session 2 = restart: a brand-new, empty allocator registry - no reservation survives a crash.
		let restart_registry = DictionaryAllocatorRegistry::new();
		assert_eq!(
			restart_registry.reserved_id(dictionary.id, &hash, &value_bytes),
			None,
			"precondition: after restart the registry holds no reservation for the mint"
		);

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			dictionary_query: Some(parent.multi.begin_query().unwrap()),
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			allocators: FlowAllocators::with_dictionary(restart_registry.clone()),
		});
		let resolved = txn.find_in_dictionary(&dictionary, &value).unwrap();
		assert_eq!(
			resolved.map(|id| id.to_u128()),
			Some(reserved_id),
			"after a restart the mint must resolve through its persisted entry, not a lost reservation"
		);
	}

	#[test]
	fn intern_through_flow_resolves_value_committed_after_its_snapshot_not_a_duplicate() {
		let engine = TestEngine::new();
		let dictionary = mints();
		let value = Value::Utf8("CuGJf6cfDfMh4UxVgNJ5KFQ6v8Wv3qrqop6cFKsGpump".to_string());
		let value_bytes = to_stdvec(&value).unwrap();
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();
		let registry = DictionaryAllocatorRegistry::new();

		let stale = {
			let parent = engine.begin_admin(IdentityId::system()).unwrap();
			parent.multi.begin_query().unwrap()
		};

		let id_a = {
			let parent = engine.begin_admin(IdentityId::system()).unwrap();
			let mut reader = parent.multi.begin_query().unwrap();
			registry.intern(&dictionary, &value_bytes, &mut reader).unwrap().id.to_u128()
		};
		{
			let writes = registry
				.reserved_writes(&dictionary, &hash, &value_bytes)
				.expect("the interned value must expose a live reservation before it is committed");
			let mut admin = engine.begin_admin(IdentityId::system()).unwrap();
			admin.set(&writes.entry_key, writes.entry_value).unwrap();
			admin.set(&writes.index_key, writes.index_value).unwrap();
			admin.commit().unwrap();
		}
		registry.mark_committed(dictionary.id, &[hash]);

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			dictionary_query: Some(stale),
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			allocators: FlowAllocators::with_dictionary(registry.clone()),
		});

		let outcome = registry.intern(&dictionary, &value_bytes, &mut txn).unwrap();
		assert_eq!(
			outcome.id.to_u128(),
			id_a,
			"a value committed after this flow's dictionary snapshot must resolve to its existing id via read_latest, not fork into a second id"
		);
		assert!(outcome.writes.is_none(), "an already-committed value must not be re-minted or co-written");
	}
}
