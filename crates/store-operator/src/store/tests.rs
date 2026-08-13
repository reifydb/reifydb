// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_value::value::datetime::DateTime;

use super::{OperatorStateCensus, OperatorStore};

const PREFIX: u32 = 9;

fn key(group: u64, keyspace: u8, suffix: u8) -> EncodedKey {
	let mut bytes = group.to_be_bytes().to_vec();
	bytes.push(keyspace);
	bytes.push(suffix);
	EncodedKey::new(bytes)
}

fn real_key(group: GroupId, keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn row(len: usize) -> EncodedOperatorRow {
	EncodedOperatorRow::new(&vec![0u8; len], DateTime::EPOCH)
}

fn prefix(group: u64, keyspace: u8) -> Vec<u8> {
	let mut bytes = group.to_be_bytes().to_vec();
	bytes.push(keyspace);
	bytes
}

#[test]
fn a_group_id_full_of_zero_bytes_still_separates_its_own_census_bucket() {
	let store = OperatorStore::memory();
	store.set(OperatorId(1), key(7, 0x10, 1), row(2));
	store.set(OperatorId(1), key(8, 0x10, 1), row(2));

	let census = store.census(PREFIX);

	assert_eq!(census.len(), 2, "two distinct groups must not merge into one bucket");
	assert_eq!(census[0].prefix, prefix(7, 0x10));
	assert_eq!(census[1].prefix, prefix(8, 0x10));
}

#[test]
fn keyspaces_of_one_group_are_counted_apart() {
	let store = OperatorStore::memory();
	store.set(OperatorId(1), key(7, 0x10, 1), row(2));
	store.set(OperatorId(1), key(7, 0xFE, 1), row(2));

	let census = store.census(PREFIX);

	assert_eq!(census.len(), 2);
	assert_eq!(census[0].prefix, prefix(7, 0x10));
	assert_eq!(census[1].prefix, prefix(7, 0xFE));
}

#[test]
fn keys_and_bytes_accumulate_over_a_bucket() {
	let store = OperatorStore::memory();
	let (small, large) = (row(2), row(3));
	let expected_keys = key(7, 0x10, 1).len() + key(7, 0x10, 2).len();
	let expected_values = small.len() + large.len();
	store.set(OperatorId(1), key(7, 0x10, 1), small);
	store.set(OperatorId(1), key(7, 0x10, 2), large);

	let census = store.census(PREFIX);

	assert_eq!(census.len(), 1, "one keyspace of one group is one bucket");
	assert_eq!(census[0].keys, 2);
	assert_eq!(census[0].key_bytes, expected_keys as u64, "key bytes must sum, not count rows");
	assert_eq!(census[0].value_bytes, expected_values as u64, "payload bytes must stay separate from key bytes");
}

#[test]
fn the_same_group_under_two_operators_stays_apart() {
	let store = OperatorStore::memory();
	store.set(OperatorId(1), key(7, 0x10, 1), row(2));
	store.set(OperatorId(2), key(7, 0x10, 1), row(2));

	let census = store.census(PREFIX);

	assert_eq!(census.len(), 2);
	assert_eq!(census[0].operator, OperatorId(1));
	assert_eq!(census[1].operator, OperatorId(2));
}

#[test]
fn a_removed_key_leaves_the_census() {
	let store = OperatorStore::memory();
	store.set(OperatorId(1), key(7, 0x10, 1), row(2));
	store.remove(OperatorId(1), &key(7, 0x10, 1));

	assert_eq!(store.census(PREFIX), Vec::<OperatorStateCensus>::new());
}

#[test]
fn a_small_group_id_encoded_the_real_way_still_yields_one_bucket_per_keyspace() {
	// A group narrower than the constant prefix lets suffix bytes leak in and shatters one bucket per suffix.
	let store = OperatorStore::memory();
	let group = GroupId(1);
	let keyspace = Keyspace(0x10);
	store.set(OperatorId(1), real_key(group, keyspace, &[0]), row(2));
	store.set(OperatorId(1), real_key(group, keyspace, &[1]), row(2));

	let census = store.census(OperatorStateKey::GROUP_KEYSPACE_PREFIX_LEN);

	assert_eq!(census.len(), 1, "two suffixes of one keyspace must share a bucket");
	assert_eq!(census[0].keys, 2);
	assert_eq!(
		OperatorStateKey::decode_inner(&census[0].prefix),
		Some((group, keyspace, Vec::new())),
		"the prefix must decode to exactly the group and keyspace, with no suffix bleed"
	);
}

#[test]
fn real_keys_of_two_small_groups_do_not_collapse_together() {
	// Adjacent small ids are exactly what a length-prefixed group encoding used to mis-split.
	let store = OperatorStore::memory();
	store.set(OperatorId(1), real_key(GroupId(1), Keyspace(0x10), &[0]), row(2));
	store.set(OperatorId(1), real_key(GroupId(2), Keyspace(0x10), &[0]), row(2));

	let census = store.census(OperatorStateKey::GROUP_KEYSPACE_PREFIX_LEN);

	assert_eq!(census.len(), 2, "distinct groups must not merge");
	let groups: Vec<GroupId> =
		census.iter().map(|c| OperatorStateKey::decode_inner(&c.prefix).unwrap().0).collect();
	assert_eq!(groups, vec![GroupId(2), GroupId(1)], "complemented u64 orders groups descending");
}

#[test]
fn real_keys_split_by_keyspace_within_one_group() {
	// The keyspace byte sits inside the prefix, so one group's data and identity must never be summed together.
	let store = OperatorStore::memory();
	let group = GroupId(1);
	store.set(OperatorId(1), real_key(group, Keyspace(0x10), &[0]), row(2));
	store.set(OperatorId(1), real_key(group, Keyspace(0xFE), &[0]), row(2));

	let census = store.census(OperatorStateKey::GROUP_KEYSPACE_PREFIX_LEN);

	assert_eq!(census.len(), 2);
	let keyspaces: Vec<Keyspace> =
		census.iter().map(|c| OperatorStateKey::decode_inner(&c.prefix).unwrap().1).collect();
	assert_eq!(keyspaces, vec![Keyspace(0xFE), Keyspace(0x10)], "descending keyspace order");
}

#[test]
fn a_long_suffix_never_lengthens_the_census_prefix() {
	// Suffixes longer than the prefix are where a short group encoding leaked most, one bucket per tail byte.
	let store = OperatorStore::memory();
	let group = GroupId(3);
	let keyspace = Keyspace(0x20);
	store.set(OperatorId(1), real_key(group, keyspace, &[9; 24]), row(2));
	store.set(OperatorId(1), real_key(group, keyspace, &[8; 24]), row(2));
	store.set(OperatorId(1), real_key(group, keyspace, &[7; 24]), row(2));

	let census = store.census(OperatorStateKey::GROUP_KEYSPACE_PREFIX_LEN);

	assert_eq!(census.len(), 1);
	assert_eq!(census[0].keys, 3);
	assert_eq!(census[0].prefix.len(), OperatorStateKey::GROUP_KEYSPACE_PREFIX_LEN as usize);
}
