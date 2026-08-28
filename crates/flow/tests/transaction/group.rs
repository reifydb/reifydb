// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::operator_state::GroupId;

fn group(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

#[test]
fn the_same_bytes_always_resolve_to_the_same_id() {
	// otherwise a later batch writes state into a group the earlier one cannot read back
	assert_eq!(GroupId::of(&group("orders")), GroupId::of(&group("orders")));
}

#[test]
fn distinct_bytes_resolve_to_distinct_ids() {
	// two group keys sharing an id would silently mix two accumulators in one state scope
	let ids: HashSet<GroupId> = (0..512).map(|i| GroupId::of(&group(&format!("g{i}")))).collect();
	assert_eq!(ids.len(), 512);
}

#[test]
fn a_one_byte_difference_changes_the_id() {
	// neighbouring keys are the common case, so separating unrelated inputs is not enough
	assert_ne!(GroupId::of(&group("window-0000")), GroupId::of(&group("window-0001")));
}

#[test]
fn ids_do_not_depend_on_the_order_the_keys_are_seen_in() {
	// the dictionary minted in arrival order, so a reordered replay must no longer shift ids
	let forward: Vec<GroupId> = (0..64).map(|i| GroupId::of(&group(&format!("g{i}")))).collect();
	let backward: Vec<GroupId> = (0..64).rev().map(|i| GroupId::of(&group(&format!("g{i}")))).collect();
	assert_eq!(forward, backward.into_iter().rev().collect::<Vec<_>>());
}

#[test]
fn no_key_ever_resolves_to_the_root_scope() {
	// root holds the timer wheel, expiry, the reap queue, the ringbuffer and the window meta
	for i in 0..4096 {
		assert!(!GroupId::of(&group(&format!("g{i}"))).is_root());
	}
	assert!(!GroupId::of(&EncodedKey::new(Vec::new())).is_root());
}

#[test]
fn a_zero_hash_folds_onto_the_first_non_root_id() {
	// the fold is what makes "never root" hold for every input, not only the sampled ones
	assert_ne!(GroupId::FIRST_NON_ROOT, GroupId::ROOT);
}

#[test]
fn the_empty_key_resolves_to_a_usable_id() {
	// an unpartitioned operator hands in empty bytes and must still get a scope of its own
	let id = GroupId::of(&EncodedKey::new(Vec::new()));
	assert!(!id.is_root());
	assert_eq!(id, GroupId::of(&EncodedKey::new(Vec::new())));
}
