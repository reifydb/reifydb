// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::{dictionary::Dictionary, id::NamespaceId},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction, deferred::DeferredTransaction, dictionary::DictionaryExtension,
	substrate::FlowSubstrate,
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	interceptor::interceptors::Interceptors,
	single::SingleTransaction,
};
use reifydb_value::value::{Value, dictionary::DictionaryId, identity::IdentityId, value_type::ValueType};

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

fn registry_on(single: &SingleTransaction) -> DictionaryAllocatorRegistry {
	DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())))
}

fn flow_txn(engine: &TestEngine, registry: DictionaryAllocatorRegistry) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate::with_dictionary(registry, engine.inner().operator_state()),
	})
}

#[test]
fn resolves_a_mint_a_concurrent_flow_interned_after_this_flows_snapshot() {
	// Dictionary entries live in the single-version store, so registry reads see the latest
	// committed entry regardless of the flow's pinned MVCC snapshot. The never-interned mint
	// proves the hit is real rather than a default.
	let engine = TestEngine::new();
	let dictionary = mints();

	let parent = engine.begin_admin(IdentityId::system()).unwrap();

	let interned = registry_on(&parent.single).intern(&dictionary, &mint()).unwrap().id.to_u128();

	let mut txn = flow_txn(&engine, registry_on(&parent.single));

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

#[test]
fn an_interned_mint_is_durable_so_a_restart_still_resolves_it() {
	// The id an intern hands out already has a durable entry, so a cold registry - all a
	// restarted process has - resolves it from the store with nothing to co-write.
	let engine = TestEngine::new();
	let dictionary = mints();

	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let interned = registry_on(&parent.single).intern(&dictionary, &mint()).unwrap().id;

	let mut txn = flow_txn(&engine, registry_on(&parent.single));

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

#[test]
fn a_rolled_back_slice_leaves_its_mint_durable_and_the_retry_reuses_that_id() {
	// A slice interns a first-seen mint twice and then fails. The entry is committed by the
	// intern itself, so the rollback leaks a durable entry nobody references (harmless, exactly
	// a sequence gap) and the retry resolves the very same id.
	let engine = TestEngine::new();
	let dictionary = mints();

	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let registry = registry_on(&parent.single);

	let interned = {
		let txn = flow_txn(&engine, registry.clone());
		let first = txn.dictionary_allocators().intern(&dictionary, &mint()).unwrap();
		let second = txn.dictionary_allocators().intern(&dictionary, &mint()).unwrap();

		assert!(first.created, "the first sight of the mint creates it");
		assert!(!second.created, "re-interning inside one slice must not create a second id");
		assert_eq!(first.id, second.id);

		first.id.to_u128()
	};

	let mut retry = flow_txn(&engine, registry);
	assert_eq!(
		retry.find_in_dictionary(&dictionary, &mint()).unwrap().map(|id| id.to_u128()),
		Some(interned),
		"the retry must resolve the same id the rolled-back slice allocated"
	);

	let mut cold = flow_txn(&engine, registry_on(&parent.single));
	assert_eq!(
		cold.find_in_dictionary(&dictionary, &mint()).unwrap().map(|id| id.to_u128()),
		Some(interned),
		"the entry survives the rollback in the store, not merely in the registry cache"
	);
}
