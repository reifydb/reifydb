// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The byte-identity contract for operator arena state, shared by every suite that asserts a flow
//! ended up in the same place twice: the clock axis and the batch axis of the replay-determinism
//! suite, the in-crate catch-up tests, and the process-lifetime crash test. One definition means
//! a state difference one suite would reject cannot be silently tolerated by another.

use std::collections::BTreeMap;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::EncodedKey,
	state::{StateBytes, decode_state},
};
use reifydb_core::{
	key::{
		EncodableKey,
		operator_group_state::{Keyspace, OperatorGroupStateKey},
		operator_state::OperatorStateKey,
	},
	state::group::GroupRecord,
};

/// Keyspaces whose KEYS embed the arrival-time activity bucket of the dispatch that stamped them.
/// Batch boundaries are exactly what varies between a live run and a replay of the same input, so
/// their keys legitimately differ; the entry COUNT still must not (one live entry per group at
/// quiescence).
pub const ARRIVAL_KEYED: &[Keyspace] = &[Keyspace::ACTIVITY_INDEX, Keyspace::SIDE_ACTIVITY_INDEX];

/// Keyspaces whose value BODIES embed an arrival position or bucket: the group record carries its
/// activity bucket, the side record carries a bucket, and the node watermark IS the last persisted
/// arrival position. Their key sets must still match; for GROUP_RECORD the decoded id-to-group
/// mapping must too, since that mapping is what the allocation-order fix pins.
pub const ARRIVAL_VALUED: &[Keyspace] =
	&[Keyspace::GROUP_RECORD, Keyspace::SIDE_ACTIVITY_RECORD, Keyspace::NODE_WATERMARK];

/// Keyspaces whose row header stamps are carried mutation times derived from row event time, not
/// from the dispatch coordinate: these must be byte-identical INCLUDING headers even across batch
/// boundaries. This pins the distinct flush-stamp mechanism.
pub const ROW_STAMPED: &[Keyspace] = &[Keyspace::DISTINCT_ENTRY, Keyspace::DISTINCT_LAYOUT];

/// One operator arena rendered as the store hands it out: ascending keys, raw rows.
pub type State = Vec<(EncodedKey, EncodedRow)>;

pub fn keyspace_of(key: &EncodedKey) -> Option<Keyspace> {
	OperatorStateKey::decode(key)
		.and_then(|state| OperatorGroupStateKey::decode_inner(&state.key))
		.map(|(_, keyspace, _)| keyspace)
}

pub fn body_of(row: &EncodedRow) -> Vec<u8> {
	match StateBytes::from_row(row.clone()) {
		Ok(bytes) => bytes.body().to_vec(),
		Err(_) => row.to_vec(),
	}
}

/// The strictest contract there is: raw key and raw row bytes, row headers included, across every
/// keyspace with no allowlist. A wall-clock read, a batch-boundary dependency or a replayed
/// version applied twice all surface here.
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
		only_a
			.first()
			.or(only_b.first())
			.map(|(key, _)| keyspace_of(&EncodedKey::new(key.clone())).map(|k| k.name()))
	);
}

/// The re-batching contract: keys and value BODIES must match everywhere, with the named
/// allowlists above carving out exactly the state that is arrival-derived by design and nothing
/// else. This is the strongest statement available whenever the same input is fed through
/// different batch boundaries - a replay-determinism batch axis, or a catch-up replay whose
/// loader chunks never line up with the live run's slices.
pub fn assert_batch_equivalent(label: &str, a: &State, b: &State) {
	let mut a_strict: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut b_strict: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut a_bodies: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut b_bodies: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut a_counts: BTreeMap<u8, usize> = BTreeMap::new();
	let mut b_counts: BTreeMap<u8, usize> = BTreeMap::new();
	let mut a_arrival: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	let mut b_arrival: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

	let classify = |state: &State,
	                strict: &mut BTreeMap<Vec<u8>, Vec<u8>>,
	                bodies: &mut BTreeMap<Vec<u8>, Vec<u8>>,
	                counts: &mut BTreeMap<u8, usize>,
	                arrival: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
		for (key, row) in state {
			let Some(keyspace) = keyspace_of(key) else {
				strict.insert(key.to_vec(), row.to_vec());
				continue;
			};
			if ARRIVAL_KEYED.contains(&keyspace) {
				*counts.entry(keyspace.0).or_insert(0) += 1;
			} else if ARRIVAL_VALUED.contains(&keyspace) {
				let group = if keyspace == Keyspace::GROUP_RECORD {
					decode_state::<GroupRecord>(
						&StateBytes::from_row(row.clone()).expect("a group record decodes"),
					)
					.expect("a group record decodes")
					.group
				} else {
					Vec::new()
				};
				arrival.insert(key.to_vec(), group);
			} else if ROW_STAMPED.contains(&keyspace) {
				strict.insert(key.to_vec(), row.to_vec());
			} else {
				bodies.insert(key.to_vec(), body_of(row));
			}
		}
	};
	classify(a, &mut a_strict, &mut a_bodies, &mut a_counts, &mut a_arrival);
	classify(b, &mut b_strict, &mut b_bodies, &mut b_counts, &mut b_arrival);

	assert_eq!(
		a_strict, b_strict,
		"{label}: row-stamped keyspaces must be byte-identical (headers included) across batch boundaries"
	);
	assert_eq!(
		a_bodies, b_bodies,
		"{label}: every non-allowlisted keyspace must agree on keys and value bodies across batch boundaries"
	);
	assert_eq!(
		a_counts, b_counts,
		"{label}: arrival-keyed index keyspaces must hold one live entry per group either way"
	);
	assert_eq!(
		a_arrival, b_arrival,
		"{label}: arrival-valued keyspaces must agree on their key sets and id-to-group mappings"
	);
}
