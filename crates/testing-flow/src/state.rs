// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey, state::StateBytes};
use reifydb_core::key::{
	EncodableKey,
	operator_group_state::{Keyspace, OperatorGroupStateKey},
	operator_state::OperatorStateKey,
};

pub const ROW_STAMPED: &[Keyspace] = &[Keyspace::DISTINCT_ENTRY, Keyspace::DISTINCT_LAYOUT];

pub type State = Vec<(EncodedKey, EncodedBytes)>;

pub fn keyspace_of(key: &EncodedKey) -> Option<Keyspace> {
	OperatorStateKey::decode(key)
		.and_then(|state| OperatorGroupStateKey::decode_inner(&state.key))
		.map(|(_, keyspace, _)| keyspace)
}

pub fn body_of(row: &EncodedBytes) -> Vec<u8> {
	match StateBytes::from_bytes(row.clone()) {
		Ok(bytes) => bytes.body().to_vec(),
		Err(_) => row.to_vec(),
	}
}

pub fn assert_identical_bytes(label: &str, a: &State, b: &State) {
	let a: Vec<(Vec<u8>, Vec<u8>)> = a.iter().map(|(k, r)| (k.to_vec(), r.to_vec())).collect();
	let b: Vec<(Vec<u8>, Vec<u8>)> = b.iter().map(|(k, r)| (k.to_vec(), r.to_vec())).collect();
	if a == b {
		return;
	}
	let only_a: Vec<&(Vec<u8>, Vec<u8>)> = a.iter().filter(|entry| !b.contains(entry)).collect();
	let only_b: Vec<&(Vec<u8>, Vec<u8>)> = b.iter().filter(|entry| !a.contains(entry)).collect();
	panic!(
		"{label}: state must be byte-identical (headers included).\n  {} entries only in the first run\n  \
		 {} entries only in the second\n  first difference in keyspace {:?}",
		only_a.len(),
		only_b.len(),
		only_a.first()
			.or(only_b.first())
			.map(|(key, _)| keyspace_of(&EncodedKey::new(key.clone())).map(|k| k.name()))
	);
}

pub fn assert_batch_equivalent(label: &str, a: &State, b: &State) {
	let mut a_strict: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut b_strict: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut a_bodies: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut b_bodies: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

	let classify =
		|state: &State, strict: &mut BTreeMap<Vec<u8>, Vec<u8>>, bodies: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
			for (key, row) in state {
				let Some(keyspace) = keyspace_of(key) else {
					strict.insert(key.to_vec(), row.to_vec());
					continue;
				};
				if ROW_STAMPED.contains(&keyspace) {
					strict.insert(key.to_vec(), row.to_vec());
				} else {
					bodies.insert(key.to_vec(), body_of(row));
				}
			}
		};
	classify(a, &mut a_strict, &mut a_bodies);
	classify(b, &mut b_strict, &mut b_bodies);

	assert_eq!(
		a_strict, b_strict,
		"{label}: row-stamped keyspaces must be byte-identical (headers included) across batch boundaries"
	);
	assert_eq!(
		a_bodies, b_bodies,
		"{label}: every non-allowlisted keyspace must agree on keys and value bodies across batch boundaries"
	);
}
