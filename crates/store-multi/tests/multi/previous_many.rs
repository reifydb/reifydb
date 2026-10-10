// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, SetPre},
	interface::{
		catalog::{flow::OperatorId, id::TableId, storage::StorageId},
		store::{MultiVersionCommit, MultiVersionGetPrevious, MultiVersionRow, classify_key},
	},
	key::{
		any::TaggedKey,
		operator::state::{GroupId, KeyspaceId, OperatorStateKey},
		row::RowKey,
	},
};
use reifydb_store_commit::TierBatch;
use reifydb_store_multi::{store::StandardMultiStore, tier::TierStorage};
use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};

fn row(n: u64) -> TaggedKey {
	RowKey {
		storage: StorageId::Table(TableId(1)),
		row: RowNumber(n),
	}
	.into()
}

fn state(node: u64, payload: &[u8]) -> TaggedKey {
	OperatorStateKey::new(OperatorId(node), GroupId::ROOT, KeyspaceId::CUSTOM_UNMANAGED, payload.to_vec()).into()
}

fn set(key: &TaggedKey, bytes: &str) -> Delta {
	Delta::Set {
		key: key.clone(),
		bytes: EncodedBytes(CowVec::new(bytes.as_bytes().to_vec())),
		pre: SetPre::Unknown,
	}
}

fn persistent_only_set(store: &StandardMultiStore, key: &TaggedKey, version: u64, value: &str) {
	let persistent = store.persistent().expect("persistent tier configured");
	let encoded = key.encode();
	let mut batches: TierBatch = HashMap::new();
	batches.entry(classify_key(&encoded))
		.or_default()
		.push((encoded, Some(CowVec::new(value.as_bytes().to_vec()))));
	persistent.set(CommitVersion(version), batches).unwrap();
}

type Seen = Option<(Vec<u8>, Vec<u8>, u64)>;

fn seen(row: Option<MultiVersionRow<TaggedKey>>) -> Seen {
	row.map(|row| (row.key.encode().to_vec(), row.bytes.0.to_vec(), row.version.0))
}

fn batched(store: &StandardMultiStore, keys: &[TaggedKey], before: u64) -> Vec<Seen> {
	store.get_previous_many(keys, CommitVersion(before)).unwrap().into_iter().map(seen).collect()
}

fn pointwise(store: &StandardMultiStore, keys: &[TaggedKey], before: u64) -> Vec<Seen> {
	keys.iter().map(|key| seen(store.get_previous_version(key, CommitVersion(before)).unwrap())).collect()
}

fn assert_batched_matches_pointwise(store: &StandardMultiStore, keys: &[TaggedKey], tier: &str) {
	for before in 0..=6 {
		assert_eq!(
			batched(store, keys, before),
			pointwise(store, keys, before),
			"{tier}: the batched previous read disagrees with the point read before version {before}"
		);
	}
}

fn version_of(found: &[Seen], i: usize) -> Option<(Vec<u8>, u64)> {
	found[i].as_ref().map(|(_, bytes, version)| (bytes.clone(), *version))
}

#[test]
fn a_batched_previous_read_answers_every_key_exactly_as_the_point_read_does_in_every_tier() {
	// The producer turns the batched answer into the published pre-image and the insert or update
	// classification, so any key where it differs from the point read it replaces changes what CDC
	// consumers see: a tombstone read as a value, a read at the commit's own version, or an answer
	// handed to the wrong position in the batch.
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let a = row(1);
	let b = row(2);
	let c = row(3);
	let s = state(7, b"s");
	let cold = row(4);
	let ghost = row(5);
	let ghost_state = state(8, b"ghost");

	persistent_only_set(&store, &cold, 1, "cold1");
	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![set(&a, "a1"), set(&b, "b1"), set(&s, "s1")]),
		CommitVersion(1),
	)
	.unwrap();
	MultiVersionCommit::commit(
		&store,
		CowVec::new(vec![set(&a, "a2"), Delta::remove_silent(b.clone()), set(&s, "s2")]),
		CommitVersion(2),
	)
	.unwrap();

	let keys =
		vec![a.clone(), b.clone(), c.clone(), s.clone(), cold.clone(), ghost.clone(), ghost_state, a.clone()];
	assert_batched_matches_pointwise(&store, &keys, "commit tier");

	store.flush_pending_blocking();
	MultiVersionCommit::commit(&store, CowVec::new(vec![set(&c, "c3"), set(&a, "a3")]), CommitVersion(3)).unwrap();
	assert_batched_matches_pointwise(&store, &keys, "persistent tier, cold point tier");
	assert_batched_matches_pointwise(&store, &keys, "persistent tier, warm point tier");

	let found = batched(&store, &keys, 3);
	assert_eq!(
		version_of(&found, 0),
		Some((b"a2".to_vec(), 2)),
		"the version below the commit, not the commit's own"
	);
	assert_eq!(version_of(&found, 1), None, "a tombstone is no previous version");
	assert_eq!(version_of(&found, 2), None, "a key first written by this commit has none");
	assert_eq!(version_of(&found, 3), Some((b"s2".to_vec(), 2)));
	assert_eq!(version_of(&found, 4), Some((b"cold1".to_vec(), 1)));
	assert_eq!(version_of(&found, 5), None);
	assert_eq!(version_of(&found, 6), None);
	assert_eq!(found[7], found[0], "a duplicate key gets the same answer");
	assert_eq!(found[0].as_ref().map(|(key, ..)| key.clone()), Some(a.encode().to_vec()));
	assert_eq!(found[3].as_ref().map(|(key, ..)| key.clone()), Some(s.encode().to_vec()));

	let found = batched(&store, &keys, 4);
	assert_eq!(version_of(&found, 0), Some((b"a3".to_vec(), 3)));
	assert_eq!(version_of(&found, 2), Some((b"c3".to_vec(), 3)));

	assert!(batched(&store, &keys, 0).iter().all(Option::is_none), "nothing precedes version zero");
	assert!(batched(&store, &[], 3).is_empty());
}
