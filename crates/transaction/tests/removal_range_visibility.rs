// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A committed silent removal must be invisible to a later range scan, exactly as it is to a get.
//! Window engines range-scan their own bookkeeping on every apply, so a resurrected entry
//! double-unmerges the running accumulators and empties the view.

use std::sync::Arc;

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::FlowNodeId,
	},
	key::{
		flow_node_state::FlowNodeStateKey,
		operator_state::{GroupId, Keyspace, OperatorStateKey},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::Pools,
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	multi::{
		RangeScope,
		transaction::{MultiTransaction, read::MultiReadTransaction, write::MultiWriteTransaction},
	},
	single::SingleTransaction,
};
use reifydb_value::{util::cowvec::CowVec, value::Value};

struct DefaultConfig;
impl GetConfig for DefaultConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		key.default_value()
	}
	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		key.default_value()
	}
}

fn test_engine() -> MultiTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();
	let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let bus = EventBus::new(&spawner);
	MultiTransaction::new(
		multi_store,
		SingleTransaction::new(single_store, bus.clone()),
		bus,
		spawner,
		Clock::Mock(MockClock::from_millis(1000)),
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(DefaultConfig),
	)
	.unwrap()
}

fn coord_key(node: u64, suffix: &[u8]) -> EncodedKey {
	let inner = OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::BUFFER, suffix);
	FlowNodeStateKey::encoded(FlowNodeId(node), inner.as_slice())
}

fn range_keys(engine: &MultiTransaction, node: u64) -> Vec<EncodedKey> {
	let query = MultiReadTransaction::new(engine.clone(), None).unwrap();
	query.range(FlowNodeStateKey::node_range(FlowNodeId(node)), RangeScope::All, 1024)
		.map(|r| r.unwrap().key)
		.collect()
}

#[test]
fn committed_drop_is_invisible_to_later_range_scan() {
	let engine = test_engine();
	let node = 7u64;
	let key_a = coord_key(node, b"a");
	let key_b = coord_key(node, b"b");

	let mut tx = MultiWriteTransaction::new(engine.clone()).unwrap();
	tx.set(&key_a, EncodedRow(CowVec::new(b"one".to_vec()))).unwrap();
	tx.set(&key_b, EncodedRow(CowVec::new(b"two".to_vec()))).unwrap();
	tx.commit(vec![]).unwrap();

	assert_eq!(range_keys(&engine, node), vec![key_a.clone(), key_b.clone()], "both entries visible before drop");

	let mut tx = MultiWriteTransaction::new(engine.clone()).unwrap();
	tx.remove_silent(&key_a).unwrap();
	tx.commit(vec![]).unwrap();

	assert_eq!(
		range_keys(&engine, node),
		vec![key_b.clone()],
		"a committed drop must be invisible to a later range scan"
	);
}
