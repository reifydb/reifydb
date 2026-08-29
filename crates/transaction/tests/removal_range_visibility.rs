// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A committed silent removal must be invisible to a later range scan, exactly as it is to a get.
//! Range scans decide what a reader sees; an entry that survives its own removal in the range path
//! but not the point path is a split-brain read the caller cannot detect.

use std::sync::Arc;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{id::TableId, storage::StorageId},
	key::row::RowKey,
	testing::ProfileConfig,
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
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

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
		Arc::new(ProfileConfig),
	)
	.unwrap()
}

fn coord_key(storage: u64, row: u64) -> EncodedKey {
	RowKey::encoded(StorageId::Table(TableId(storage)), RowNumber(row))
}

fn range_keys(engine: &MultiTransaction, storage: u64) -> Vec<EncodedKey> {
	let query = MultiReadTransaction::new(engine.clone(), None).unwrap();
	query.range(RowKey::full_scan(StorageId::Table(TableId(storage))), RangeScope::All, 1024)
		.map(|r| r.unwrap().key)
		.collect()
}

#[test]
fn committed_drop_is_invisible_to_later_range_scan() {
	// The removal contract is a property of the versioned range path, not of any one key kind, so
	// this exercises it through ordinary row keys. Falsified by having the second scan return key_a.
	let engine = test_engine();
	let node = 7u64;
	let key_a = coord_key(node, 1);
	let key_b = coord_key(node, 2);

	let mut tx = MultiWriteTransaction::new(engine.clone()).unwrap();
	tx.set(&key_a, EncodedBytes(CowVec::new(b"one".to_vec()))).unwrap();
	tx.set(&key_b, EncodedBytes(CowVec::new(b"two".to_vec()))).unwrap();
	tx.commit(vec![]).unwrap();

	// Row keys are keycode-encoded, so a scan returns them by descending row number.
	assert_eq!(range_keys(&engine, node), vec![key_b.clone(), key_a.clone()], "both entries visible before drop");

	let mut tx = MultiWriteTransaction::new(engine.clone()).unwrap();
	tx.remove_silent(&key_a).unwrap();
	tx.commit(vec![]).unwrap();

	assert_eq!(
		range_keys(&engine, node),
		vec![key_b.clone()],
		"a committed drop must be invisible to a later range scan"
	);
}
