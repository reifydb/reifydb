// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::operator::{
	keyspace::KEYSPACES,
	state::{GroupId, KeyspaceId, OperatorStateKey},
};

pub fn suffix_width(keyspace: KeyspaceId) -> usize {
	KEYSPACES
		.iter()
		.find(|spec| spec.id == keyspace)
		.expect("a fixture keyspace must appear in the catalogue")
		.suffix_width()
}

pub fn suffix_bytes(keyspace: KeyspaceId, seed: u64) -> Vec<u8> {
	let width = suffix_width(keyspace);
	assert!(
		width >= 8 || seed >> (width * 8) == 0,
		"seed {seed} does not fit the {width} byte suffix of {}, so distinct seeds collapse onto one key",
		keyspace.name()
	);
	let mut bytes = vec![0u8; width];
	let seed = seed.to_be_bytes();
	let take = seed.len().min(width);
	bytes[width - take..].copy_from_slice(&seed[seed.len() - take..]);
	bytes
}

pub fn state_key(group: GroupId, keyspace: KeyspaceId, seed: u64) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix_bytes(keyspace, seed)).as_encoded().clone()
}
