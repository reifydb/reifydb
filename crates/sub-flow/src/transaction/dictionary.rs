// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};
use tracing::instrument;

use super::FlowTransaction;

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

	#[instrument(name = "flow::dictionary::find", level = "trace", skip(self, dictionary, value), fields(dictionary_id = dictionary.id.0))]
	pub fn find_in_dictionary(
		&mut self,
		dictionary: &Dictionary,
		value: &Value,
	) -> Result<Option<DictionaryEntryId>> {
		self.dictionary_allocators().find(dictionary, value)
	}

	#[instrument(name = "flow::dictionary::resolve", level = "trace", skip(self, dictionary, id), fields(dictionary_id = dictionary.id.0))]
	pub fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.dictionary_allocators().get(dictionary, id.to_u128())? {
			Some(bytes) => Ok(Some(from_bytes(&bytes).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		actors::pending::Pending,
		interface::catalog::{dictionary::Dictionary, id::NamespaceId},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::{
		dictionary::{DictionaryAllocatorRegistry, store::MultiDictionaryStore},
		interceptor::interceptors::Interceptors,
		multi::transaction::{MultiTransaction, read::MultiReadTransaction},
	};
	use reifydb_value::value::{Value, dictionary::DictionaryId, identity::IdentityId, value_type::ValueType};

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

	fn mint() -> Value {
		Value::Utf8("CuGJf6cfDfMh4UxVgNJ5KFQ6v8Wv3qrqop6cFKsGpump".to_string())
	}

	fn registry_on(multi: &MultiTransaction) -> DictionaryAllocatorRegistry {
		DictionaryAllocatorRegistry::new(Arc::new(MultiDictionaryStore::new(multi.clone())))
	}

	fn flow_txn(
		engine: &TestEngine,
		registry: DictionaryAllocatorRegistry,
		dictionary_query: Option<MultiReadTransaction>,
	) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let dictionary_query = Some(dictionary_query.unwrap_or_else(|| parent.multi.begin_query().unwrap()));
		FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			dictionary_query,
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			allocators: FlowAllocators::with_dictionary(registry),
		})
	}

	// The version gap, which is what the deleted reservation read-through existed to close. A
	// concurrent flow interns a first-seen mint, committing its entry at a version ABOVE the snapshot a
	// downstream flow is reading at. That downstream flow, with a cold cache, must still resolve the
	// mint - through the committed-at-latest read, not its pinned snapshot. Resolving against the
	// snapshot returns None here and aborts the operator, which is the original production crash.
	// A mint nobody interned still resolves to None, proving the hit is real and not a false positive.
	#[test]
	fn resolves_a_mint_a_concurrent_flow_interned_after_this_flows_snapshot() {
		let engine = TestEngine::new();
		let dictionary = mints();

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let stale_snapshot = parent.multi.begin_query().unwrap();

		let interned =
			registry_on(&parent.multi).intern(&dictionary, &mint()).unwrap().outcomes[0].id.to_u128();

		let mut txn = flow_txn(&engine, registry_on(&parent.multi), Some(stale_snapshot));

		assert_eq!(
			txn.find_in_dictionary(&dictionary, &Value::Utf8("never-interned".to_string())).unwrap(),
			None,
			"a mint that was never interned must resolve to None"
		);
		assert_eq!(
			txn.find_in_dictionary(&dictionary, &mint()).unwrap().map(|id| id.to_u128()),
			Some(interned),
			"a downstream flow must resolve a mint a concurrent flow interned after its snapshot began"
		);
	}

	// Restart. The id an intern hands out already has a durable entry, so a brand-new registry with an
	// empty cache - which is all a restarted process has - resolves it from the store. The old design
	// had to co-write the entry into whichever transaction happened to reference the id; here there is
	// nothing to co-write, because the entry was never not durable.
	#[test]
	fn an_interned_mint_is_durable_so_a_restart_still_resolves_it() {
		let engine = TestEngine::new();
		let dictionary = mints();

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let interned = registry_on(&parent.multi).intern(&dictionary, &mint()).unwrap().outcomes.remove(0).id;

		let mut txn = flow_txn(&engine, registry_on(&parent.multi), None);

		assert_eq!(
			txn.find_in_dictionary(&dictionary, &mint()).unwrap(),
			Some(interned.clone()),
			"after a restart the mint must resolve through its durable entry"
		);
		assert_eq!(
			txn.get_from_dictionary(&dictionary, interned).unwrap(),
			Some(mint()),
			"the id must decode back to its value after a restart"
		);
	}

	// The rolled-back slice, inverted. A slice interns a first-seen mint twice - two trades on one mint
	// in one block - and then fails. Under the old design the retry could be told the mint was already
	// durable when nothing had been written, and it committed a view row referencing an unresolvable
	// id. Now the entry is committed by the intern itself, so the rollback leaks a durable entry that
	// nobody references (harmless, exactly a sequence's gap) and the retry resolves the very same id.
	#[test]
	fn a_rolled_back_slice_leaves_its_mint_durable_and_the_retry_reuses_that_id() {
		let engine = TestEngine::new();
		let dictionary = mints();

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let registry = registry_on(&parent.multi);

		let interned = {
			let mut txn = flow_txn(&engine, registry.clone(), None);
			let first = txn.dictionary_allocators().intern(&dictionary, &mint()).unwrap();
			let second = txn.dictionary_allocators().intern(&dictionary, &mint()).unwrap();

			assert!(first.outcomes[0].created, "the first sight of the mint creates it");
			assert!(
				!second.outcomes[0].created,
				"re-interning inside one slice must not create a second id"
			);
			assert_eq!(first.outcomes[0].id, second.outcomes[0].id);

			first.outcomes[0].id.to_u128()
		};

		let mut retry = flow_txn(&engine, registry, None);
		assert_eq!(
			retry.find_in_dictionary(&dictionary, &mint()).unwrap().map(|id| id.to_u128()),
			Some(interned),
			"the retry must resolve the same id the rolled-back slice allocated"
		);

		let mut cold = flow_txn(&engine, registry_on(&parent.multi), None);
		assert_eq!(
			cold.find_in_dictionary(&dictionary, &mint()).unwrap().map(|id| id.to_u128()),
			Some(interned),
			"the entry survives the rollback in the store, not merely in the registry cache"
		);
	}
}
