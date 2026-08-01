// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, watermark::compute_pinning_watermark},
	storage::{CdcStorage, memory::MemoryCdcStorage},
};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		cdc::{Cdc, CdcConsumerId, ConsumerClass, SystemChange},
	},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	count::Count,
	util::cowvec::CowVec,
	value::{datetime::DateTime, identity::IdentityId},
};

fn make_cdc(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(12345 + version),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![version as u8]),
			post: EncodedRow(CowVec::new(vec![version as u8])),
		}],
	)
}

fn persist(t: &TestEngine, consumer: &str, version: u64, class: ConsumerClass) {
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new(consumer), CommitVersion(version), class).unwrap();
	txn.commit().unwrap();
}

fn pinning_watermark(t: &TestEngine) -> Option<CommitVersion> {
	let mut query_txn = t.begin_query(IdentityId::system()).unwrap();
	compute_pinning_watermark(&mut Transaction::Query(&mut query_txn)).unwrap()
}

#[test]
fn no_pinning_consumers_means_no_floor() {
	// None means "nothing pins retention"; reporting version 0 instead reads as a consumer parked at
	// the beginning and blocks truncation on a database with no pinning consumers at all.
	let t = TestEngine::new();
	assert_eq!(pinning_watermark(&t), None, "no consumers => None");
}

#[test]
fn a_single_pinning_consumer_is_the_floor() {
	let t = TestEngine::new();
	persist(&t, "consumer1", 42, ConsumerClass::Pinning);

	assert_eq!(pinning_watermark(&t), Some(CommitVersion(42)));
}

#[test]
fn the_floor_is_the_minimum_across_pinning_consumers() {
	let t = TestEngine::new();
	persist(&t, "consumer1", 100, ConsumerClass::Pinning);
	persist(&t, "consumer2", 85, ConsumerClass::Pinning);
	persist(&t, "consumer3", 95, ConsumerClass::Pinning);
	persist(&t, "consumer4", 110, ConsumerClass::Pinning);

	assert_eq!(pinning_watermark(&t), Some(CommitVersion(85)));
}

#[test]
fn an_ephemeral_consumer_is_invisible_to_the_pinning_floor() {
	// A wedged subscription must never stall retention system-wide. Its checkpoint is still kept,
	// for observability and overtaken detection, but carries no pinning power.
	let t = TestEngine::new();
	persist(&t, "flow_like", 900, ConsumerClass::Pinning);
	persist(&t, "wedged_subscription", 3, ConsumerClass::Ephemeral);

	assert_eq!(
		pinning_watermark(&t),
		Some(CommitVersion(900)),
		"the pinning floor must reflect only Pinning checkpoints; ephemeral lag cannot stall it"
	);
}

#[test]
fn only_ephemeral_consumers_present_means_no_floor() {
	// A database serving only subscriptions must truncate as if it had no consumers, with the TTL
	// alone governing; an ephemeral row leaking into the fold would make the class split a no-op.
	let t = TestEngine::new();
	persist(&t, "sub_a", 7, ConsumerClass::Ephemeral);
	persist(&t, "sub_b", 9000, ConsumerClass::Ephemeral);

	assert_eq!(pinning_watermark(&t), None);
}

#[test]
fn a_per_flow_checkpoint_row_pins_the_floor() {
	// Per-flow checkpoints persist under their own id, not the coordinator's. Deriving the class by
	// matching consumer ids would miss them and truncate under a lagging flow, losing view data;
	// storing the class on the row makes per-flow rows pin by construction.
	let t = TestEngine::new();

	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	CdcCheckpoint::persist(&mut txn, &FlowId(42), CommitVersion(11), ConsumerClass::Pinning).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::flow_consumer(), CommitVersion(500), ConsumerClass::Pinning)
		.unwrap();
	txn.commit().unwrap();

	assert_eq!(
		pinning_watermark(&t),
		Some(CommitVersion(11)),
		"a lagging per-flow checkpoint must hold the floor even though it is not the coordinator row"
	);
}

#[test]
fn the_floor_advances_as_the_slowest_pinning_consumer_catches_up() {
	let t = TestEngine::new();
	persist(&t, "fast_consumer", 100, ConsumerClass::Pinning);
	persist(&t, "slow_consumer", 50, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(50)));

	persist(&t, "slow_consumer", 80, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(80)));

	persist(&t, "slow_consumer", 100, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(100)));
}

#[test]
fn a_slow_pinning_consumer_prevents_cdc_cleanup_until_caught_up() {
	// CDC below the slowest pinning checkpoint must survive truncation: that consumer will still
	// read it after a restart.
	let storage = MemoryCdcStorage::new();
	let t = TestEngine::new();

	for version in [10u64, 20, 30, 40, 50] {
		storage.write(&make_cdc(version)).unwrap();
	}
	assert_eq!(storage.len(), 5);

	persist(&t, "fast_consumer", 50, ConsumerClass::Pinning);
	persist(&t, "slow_consumer", 20, ConsumerClass::Pinning);

	let watermark = pinning_watermark(&t).unwrap();
	assert_eq!(watermark, CommitVersion(20));

	let result = storage.drop_before(watermark, usize::MAX).unwrap();
	assert_eq!(result.count, Count::new(1));
	assert!(storage.read(CommitVersion(10)).unwrap().is_none());
	assert!(storage.read(CommitVersion(20)).unwrap().is_some(), "the entry AT the floor must be retained");
	assert_eq!(storage.len(), 4);

	persist(&t, "slow_consumer", 50, ConsumerClass::Pinning);
	let watermark = pinning_watermark(&t).unwrap();
	assert_eq!(watermark, CommitVersion(50));

	let result = storage.drop_before(watermark, usize::MAX).unwrap();
	assert_eq!(result.count, Count::new(3));
	assert!(storage.read(CommitVersion(50)).unwrap().is_some());
	assert_eq!(storage.len(), 1);
}

#[test]
fn an_ephemeral_laggard_does_not_prevent_cdc_cleanup() {
	// A subscription parked at version 10 must not keep 10..=40 alive; cleanup runs as if it were
	// absent and the consumer learns of the truncation through the overtaken protocol.
	let storage = MemoryCdcStorage::new();
	let t = TestEngine::new();

	for version in [10u64, 20, 30, 40, 50] {
		storage.write(&make_cdc(version)).unwrap();
	}

	persist(&t, "flow_like", 50, ConsumerClass::Pinning);
	persist(&t, "parked_subscription", 10, ConsumerClass::Ephemeral);

	let watermark = pinning_watermark(&t).unwrap();
	assert_eq!(watermark, CommitVersion(50), "the parked ephemeral consumer must not lower the floor");

	let result = storage.drop_before(watermark, usize::MAX).unwrap();
	assert_eq!(result.count, Count::new(4));
	assert_eq!(storage.len(), 1);
	assert!(storage.read(CommitVersion(50)).unwrap().is_some());
}

#[test]
fn a_new_lagging_pinning_consumer_pulls_the_floor_down() {
	let t = TestEngine::new();
	persist(&t, "consumer1", 500, ConsumerClass::Pinning);
	persist(&t, "consumer2", 510, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(500)));

	persist(&t, "new_consumer", 100, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(100)));
}

#[test]
fn reclassifying_a_consumer_repersists_its_pinning_power() {
	// Every persist rewrites the class, so a downgrade to Ephemeral releases the floor immediately
	// with no separate cleanup step.
	let t = TestEngine::new();
	persist(&t, "flow_like", 200, ConsumerClass::Pinning);
	persist(&t, "migrating", 20, ConsumerClass::Pinning);
	assert_eq!(pinning_watermark(&t), Some(CommitVersion(20)));

	persist(&t, "migrating", 20, ConsumerClass::Ephemeral);
	assert_eq!(
		pinning_watermark(&t),
		Some(CommitVersion(200)),
		"the rewritten class must take effect immediately; the stale Pinning row must not linger"
	);
}

#[test]
fn the_floor_handles_very_large_version_numbers() {
	let t = TestEngine::new();
	persist(&t, "consumer1", u64::MAX - 100, ConsumerClass::Pinning);
	persist(&t, "consumer2", u64::MAX - 200, ConsumerClass::Pinning);
	persist(&t, "consumer3", u64::MAX - 50, ConsumerClass::Pinning);

	assert_eq!(pinning_watermark(&t), Some(CommitVersion(u64::MAX - 200)));
}

#[test]
fn the_floor_finds_the_minimum_among_many_consumers() {
	let t = TestEngine::new();

	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	for i in 0..100 {
		let consumer_id = CdcConsumerId::new(&format!("consumer_{}", i));
		let version = CommitVersion(100 + (i * 10));
		CdcCheckpoint::persist(&mut txn, &consumer_id, version, ConsumerClass::Pinning).unwrap();
	}
	CdcCheckpoint::persist(
		&mut txn,
		&CdcConsumerId::new("minimum_consumer"),
		CommitVersion(50),
		ConsumerClass::Pinning,
	)
	.unwrap();
	txn.commit().unwrap();

	assert_eq!(pinning_watermark(&t), Some(CommitVersion(50)));
}
