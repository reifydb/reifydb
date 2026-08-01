// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A committed silent removal of operator state must be invisible to both gets and ranges from the very
//! next commit onward, with no reclamation in between: window engines delete range-scanned bookkeeping
//! and re-read it on the next apply, and a resurrected entry double-unmerges a running accumulator.

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionCommit, MultiVersionGet},
	},
	key::{
		operator_group_state::{GroupId, Keyspace, OperatorGroupStateKey},
		operator_state::OperatorStateKey,
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig},
	store::StandardMultiStore,
	tier::commit::buffer::MultiCommitBufferTier,
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn memory_store() -> StandardMultiStore {
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	StandardMultiStore::new(MultiStoreConfig {
		commit: CommitBufferConfig {
			storage: MultiCommitBufferTier::memory(),
		},
		persistent: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: reifydb_core::event::EventBus::new(&spawner),
		spawner,
		clock: Clock::Real,
	})
	.unwrap()
}

fn coord_key(node: u64, suffix: &[u8]) -> EncodedKey {
	let inner = OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::BUFFER, suffix);
	OperatorStateKey::encoded(OperatorId(node), inner.as_slice())
}

fn node_range(node: u64) -> reifydb_codec::key::encoded::EncodedKeyRange {
	OperatorStateKey::node_range(OperatorId(node))
}

fn row(bytes: &[u8]) -> EncodedRow {
	EncodedRow(CowVec::new(bytes.to_vec()))
}

fn range_keys(store: &StandardMultiStore, node: u64, version: u64) -> Vec<EncodedKey> {
	store.range(
		node_range(node),
		MultiVersionScope::AsOf {
			read: CommitVersion(version),
		},
		1024,
	)
	.map(|r| r.unwrap().key)
	.collect()
}

#[test]
fn committed_drop_is_invisible_to_next_transaction_get_and_range() {
	let store = memory_store();
	let node = 7u64;
	let key_a = coord_key(node, b"a");
	let key_b = coord_key(node, b"b");

	MultiVersionCommit::commit(
		&store,
		cow_vec![
			(Delta::Set {
				key: key_a.clone(),
				row: row(b"one"),
			})
		],
		CommitVersion(1),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		cow_vec![
			(Delta::Set {
				key: key_b.clone(),
				row: row(b"two"),
			})
		],
		CommitVersion(2),
	)
	.unwrap();

	assert_eq!(range_keys(&store, node, 2), vec![key_a.clone(), key_b.clone()], "both entries visible before drop");

	MultiVersionCommit::commit(&store, cow_vec![(Delta::remove_silent(key_a.clone()))], CommitVersion(3)).unwrap();

	assert!(
		store.get(&key_a, CommitVersion(4)).unwrap().is_none(),
		"a committed drop must be invisible to a later get without waiting for the drop worker"
	);
	assert_eq!(
		range_keys(&store, node, 4),
		vec![key_b.clone()],
		"a committed drop must be invisible to a later range without waiting for the drop worker"
	);
}
