// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_operator::{
	config::OperatorStoreConfig,
	store::OperatorStore,
	types::{DurablePre, OperatorWrite},
};
use reifydb_testing::keyspace::state_key;
use reifydb_value::byte_size::ByteSize;

fn store() -> OperatorStore {
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		resident: Default::default(),
		persistent: None,
		point: None,
		range: None,
		spawner,
		clock: Clock::Real,
	})
}

fn key() -> EncodedKey {
	state_key(GroupId(1), KeyspaceId::JOIN_LEFT, 1)
}

#[test]
#[cfg_attr(not(reifydb_assertions), ignore)]
#[should_panic(expected = "classified a write against a pre-image")]
fn a_replace_over_an_absent_key_is_rejected() {
	// the census turns into delta arithmetic over the caller's claim, so a wrong claim drifts the bucket forever
	let store = store();
	store.apply_batch(&[OperatorWrite::Replace {
		operator: OperatorId(1),
		key: key(),
		pre_value_bytes: ByteSize::from_bytes(3),
		post: EncodedPodRow::new(b"new"),
	}]);
}

#[test]
#[cfg_attr(not(reifydb_assertions), ignore)]
#[should_panic(expected = "classified a write against a pre-image")]
fn an_insert_over_a_present_key_is_rejected() {
	// a second insert on the same key would count the key twice and leave the first value's bytes stranded
	let store = store();
	store.apply_batch(&[OperatorWrite::Insert {
		operator: OperatorId(1),
		key: key(),
		post: EncodedPodRow::new(b"one"),
	}]);
	store.apply_batch(&[OperatorWrite::Insert {
		operator: OperatorId(1),
		key: key(),
		post: EncodedPodRow::new(b"two"),
	}]);
}

#[test]
#[cfg_attr(not(reifydb_assertions), ignore)]
fn a_correct_chain_of_claims_is_accepted() {
	// a claim is measured against the write before it in the batch, not the store's state on entry
	let store = store();
	store.apply_batch(&[
		OperatorWrite::Insert {
			operator: OperatorId(1),
			key: key(),
			post: EncodedPodRow::new(b"one"),
		},
		OperatorWrite::Replace {
			operator: OperatorId(1),
			key: key(),
			pre_value_bytes: ByteSize::from_bytes(EncodedPodRow::new(b"one").bytes().len() as u64),
			post: EncodedPodRow::new(b"twelve"),
		},
		OperatorWrite::Remove {
			operator: OperatorId(1),
			key: key(),
			pre: DurablePre::Present(ByteSize::from_bytes(
				EncodedPodRow::new(b"twelve").bytes().len() as u64
			)),
		},
	]);
	assert!(store.get(OperatorId(1), &key()).is_none());
}
