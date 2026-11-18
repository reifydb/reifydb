// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{id::ViewId, object::ObjectId},
	key::partition::PartitionKey,
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
use reifydb_transaction::{
	commit::DeferredFlowCommit,
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	wal::{memory_wal, replay},
};
use reifydb_value::{util::cowvec::CowVec, value::partition::Partition};

struct Harness {
	_actor_system: ActorSystem,
	multi: MultiTransaction,
}

impl Harness {
	fn begin(&self) -> DeferredFlowCommit {
		DeferredFlowCommit::new(self.multi.clone(), Interceptors::new().post_commit, None)
			.expect("begin a deferred flow commit")
	}
}

fn harness() -> Harness {
	let multi_store = MultiStore::testing_memory();
	let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
	let spawner = actor_system.spawner();
	let bus = EventBus::new(&spawner);
	let wal = memory_wal(Clock::Mock(MockClock::from_millis(1000))).unwrap();
	let recovery = replay(&wal, multi_store.commit(), CommitVersion(0), CommitVersion(0));
	let multi = MultiTransaction::new(
		multi_store,
		bus,
		spawner.clone(),
		Clock::Mock(MockClock::from_millis(1000)),
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(ProfileConfig),
		wal,
		recovery,
	)
	.unwrap();

	Harness {
		_actor_system: actor_system,
		multi,
	}
}

fn key(name: &str) -> PartitionKey {
	let partition = name.bytes().fold(0u128, |acc, byte| (acc << 8) | u128::from(byte));
	PartitionKey::new(ObjectId::View(ViewId(1)), Partition(partition))
}

fn encoded_bytes(value: &str) -> EncodedBytes {
	EncodedBytes(CowVec::new(value.as_bytes().to_vec()))
}

fn read_back(h: &Harness, k: &PartitionKey) -> Option<Vec<u8>> {
	let mut commit = h.begin();
	commit.get(&[k.clone().into()]).expect("get").pop().flatten().map(|bytes| bytes.to_vec())
}

#[test]
fn each_deferred_flow_commit_commits_in_its_own_version() {
	// Nothing merges submissions, so one must never be able to roll back another's writes.
	let h = harness();

	let mut versions: Vec<CommitVersion> = Vec::new();
	for i in 0..3 {
		let mut commit = h.begin();
		commit.set(vec![(key(&format!("inline-{i}")).into(), encoded_bytes("y"))]).expect("set");
		versions.push(commit.commit().expect("commit").version);
	}
	let deduped: Vec<CommitVersion> = {
		let mut v = versions.clone();
		v.dedup();
		v
	};
	assert_eq!(deduped.len(), 3, "submissions must never be merged: {versions:?}");
	versions.sort();
	assert!(versions.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn a_dropped_deferred_flow_commit_leaves_nothing_behind_and_the_next_one_commits() {
	// A batch that is dropped must leave nothing behind, otherwise one failed slice poisons every later commit.
	let h = harness();

	let k0 = key("poisoned-0");
	let mut dropped = h.begin();
	dropped.set(vec![(k0.clone().into(), encoded_bytes("should-roll-back"))]).expect("set");
	drop(dropped);

	let mut survivor = h.begin();
	survivor.set(vec![(key("survivor").into(), encoded_bytes("ok"))]).expect("set");
	survivor.commit().expect("commit");

	assert_eq!(read_back(&h, &k0), None, "the writes of a dropped batch must leave nothing behind");
	assert_eq!(
		read_back(&h, &key("survivor")),
		Some(b"ok".to_vec()),
		"a later submission must still commit after a failed one"
	);
}
