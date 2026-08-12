// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::value::datetime::DateTime;

use super::{OperatorStateCensus, OperatorStore};

const PREFIX: u32 = 9;

fn key(group: u64, keyspace: u8, suffix: u8) -> EncodedKey {
	let mut bytes = group.to_be_bytes().to_vec();
	bytes.push(keyspace);
	bytes.push(suffix);
	EncodedKey::new(bytes)
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
