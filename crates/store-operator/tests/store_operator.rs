// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_codec::{
	encoded::bytes::{EncodedBytes, SHAPE_HEADER_SIZE},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{
		GroupId, Keyspace, OperatorGroupStateKey, group_data_of_inner, keyspace_inner_range,
	},
};
use reifydb_store_operator::{config::OperatorStoreConfig, floor::FloorSpec, store::OperatorStore};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

fn store_with(freeze_bytes: u64, max_frozen: usize) -> OperatorStore {
	OperatorStore::new(OperatorStoreConfig {
		freeze_bytes,
		max_frozen,
	})
}

fn manual_store() -> OperatorStore {
	// Thresholds high enough that freezes and merges only happen when a test asks for them.
	store_with(1 << 40, usize::MAX)
}

fn key(bytes: &[u8]) -> EncodedKey {
	EncodedKey::new(bytes)
}

fn data_key(group: u64, keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
	OperatorGroupStateKey::inner_encoded(GroupId(group), keyspace, suffix).into_encoded()
}

fn value(payload: &[u8]) -> EncodedBytes {
	EncodedBytes(CowVec::new(payload.to_vec()))
}

fn stamped(payload: &[u8], updated_at: u64) -> EncodedBytes {
	// Row header layout: fingerprint(8) | created_at(8) | updated_at(8) | time(8), stamps little-endian,
	// matching what EncodedBytes::updated_at reads back. The floor only ever consults updated_at.
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&updated_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedBytes(CowVec::new(buf))
}

fn all_range() -> EncodedKeyRange {
	EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded)
}

fn scan_all(store: &OperatorStore, operator: OperatorId) -> Vec<(EncodedKey, EncodedBytes)> {
	// Batched continuation with a tiny batch size so every multi-item scan also exercises has_more
	// and the resume-from-last-key path across batch boundaries.
	let mut items: Vec<(EncodedKey, EncodedBytes)> = Vec::new();
	let mut start = Bound::Unbounded;
	loop {
		let batch = store.range_batch(operator, EncodedKeyRange::new(start, Bound::Unbounded), 3);
		let done = !batch.has_more;
		items.extend(batch.items);
		if done {
			return items;
		}
		start = Bound::Excluded(items.last().unwrap().0.clone());
	}
}

const OP: OperatorId = OperatorId(1);

#[test]
fn set_get_round_trips_and_overwrite_wins_in_active() {
	// The most basic contract: a set is readable back, a newer set of the same key replaces the
	// older one, and a key never written stays absent.
	let store = manual_store();

	store.set(OP, key(b"k"), value(b"v1"));
	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"v1")));
	assert!(store.contains(OP, &key(b"k")));

	store.set(OP, key(b"k"), value(b"v2"));
	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"v2")));

	assert_eq!(store.get(OP, &key(b"missing")), None);
	assert!(!store.contains(OP, &key(b"missing")));
}

#[test]
fn remove_round_trips_with_and_without_frozen_batches() {
	// remove must erase in both regimes: with no frozen batches it deletes the live entry outright,
	// with frozen batches it must still hide the key (via a tombstone), and a re-set afterwards wins.
	let store = manual_store();

	store.set(OP, key(b"a"), value(b"v"));
	store.remove(OP, &key(b"a"));
	assert_eq!(store.get(OP, &key(b"a")), None);
	store.set(OP, key(b"a"), value(b"again"));
	assert_eq!(store.get(OP, &key(b"a")), Some(value(b"again")));

	store.freeze(OP);
	store.remove(OP, &key(b"a"));
	assert_eq!(store.get(OP, &key(b"a")), None, "a remove above a frozen set must mask it");
	store.set(OP, key(b"a"), value(b"back"));
	assert_eq!(store.get(OP, &key(b"a")), Some(value(b"back")));
}

#[test]
fn clear_empties_the_arena_and_zeroes_bytes() {
	// clear drops the whole arena content, frozen batches included, and the byte accounting must
	// come back to exactly zero or the store leaks phantom residency into the metrics.
	let store = manual_store();

	store.set(OP, key(b"a"), value(b"1"));
	store.set(OP, key(b"b"), value(b"2"));
	store.freeze(OP);
	store.set(OP, key(b"c"), value(b"3"));
	assert!(store.bytes(OP) > 0);

	store.clear(OP);
	assert_eq!(store.bytes(OP), 0);
	assert_eq!(store.total_bytes(), 0);
	assert_eq!(store.get(OP, &key(b"a")), None);
	assert!(scan_all(&store, OP).is_empty());
}

#[test]
fn newest_wins_across_active_and_frozen_batches() {
	// A key rewritten after a freeze lives in two batches at once; reads must resolve to the active
	// (newest) copy while keys only present in a frozen batch stay visible.
	let store = manual_store();

	store.set(OP, key(b"k"), value(b"old"));
	store.set(OP, key(b"frozen_only"), value(b"kept"));
	store.freeze(OP);
	store.set(OP, key(b"k"), value(b"new"));

	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"new")));
	assert_eq!(store.get(OP, &key(b"frozen_only")), Some(value(b"kept")));

	let items = scan_all(&store, OP);
	assert_eq!(items, vec![(key(b"frozen_only"), value(b"kept")), (key(b"k"), value(b"new"))]);
}

#[test]
fn newest_wins_across_multiple_frozen_batches() {
	// With three generations of the same key in three frozen batches, both point reads and scans
	// must pick the newest frozen generation, not the first one encountered in insertion order.
	let store = manual_store();

	store.set(OP, key(b"k"), value(b"g1"));
	store.freeze(OP);
	store.set(OP, key(b"k"), value(b"g2"));
	store.freeze(OP);
	store.set(OP, key(b"k"), value(b"g3"));
	store.freeze(OP);

	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"g3")));
	assert_eq!(scan_all(&store, OP), vec![(key(b"k"), value(b"g3"))]);
}

#[test]
fn tombstone_masks_older_set_in_reads_and_scans() {
	// A tombstone in a newer batch must hide the frozen value from get, contains, and scans alike;
	// a scan that surfaces the dead key would feed an operator state it already deleted.
	let store = manual_store();

	store.set(OP, key(b"dead"), value(b"v"));
	store.set(OP, key(b"live"), value(b"v"));
	store.freeze(OP);
	store.remove(OP, &key(b"dead"));

	assert_eq!(store.get(OP, &key(b"dead")), None);
	assert!(!store.contains(OP, &key(b"dead")));
	assert_eq!(scan_all(&store, OP), vec![(key(b"live"), value(b"v"))]);
}

#[test]
fn range_tombstone_masks_a_whole_prefix() {
	// remove_range is the O(1) way a whole keyspace dies: every frozen key under the prefix must
	// vanish from reads and scans while neighbouring prefixes stay untouched.
	let store = manual_store();

	store.set(OP, key(b"p/a"), value(b"1"));
	store.set(OP, key(b"p/b"), value(b"2"));
	store.set(OP, key(b"q/a"), value(b"3"));
	store.freeze(OP);
	store.set(OP, key(b"p/c"), value(b"4"));

	store.remove_range(OP, EncodedKeyRange::prefix(b"p/"));

	assert_eq!(store.get(OP, &key(b"p/a")), None);
	assert_eq!(store.get(OP, &key(b"p/b")), None);
	assert_eq!(store.get(OP, &key(b"p/c")), None, "the range must also delete covered keys in the active batch");
	assert_eq!(store.get(OP, &key(b"q/a")), Some(value(b"3")));
	assert_eq!(scan_all(&store, OP), vec![(key(b"q/a"), value(b"3"))]);

	store.set(OP, key(b"p/a"), value(b"reborn"));
	assert_eq!(
		store.get(OP, &key(b"p/a")),
		Some(value(b"reborn")),
		"a set issued after the range tombstone must not be masked by it"
	);
}

#[test]
fn scans_are_ascending_and_stable_across_freeze_boundaries() {
	// Keys are interleaved across three batches so a correct scan must k-way merge; emitting
	// batch-by-batch or in reverse would break the ascending order operators rely on for resume.
	let store = manual_store();

	store.set(OP, key(b"d"), value(b"1"));
	store.set(OP, key(b"a"), value(b"1"));
	store.freeze(OP);
	store.set(OP, key(b"e"), value(b"1"));
	store.set(OP, key(b"b"), value(b"1"));
	store.freeze(OP);
	store.set(OP, key(b"c"), value(b"1"));

	let items = scan_all(&store, OP);
	let keys: Vec<&[u8]> = items.iter().map(|(key, _)| key.as_slice()).collect();
	assert_eq!(keys, vec![b"a".as_slice(), b"b", b"c", b"d", b"e"]);

	let first = store.range_batch(OP, all_range(), 2);
	assert_eq!(first.items.len(), 2);
	assert!(first.has_more, "two of five keys delivered, so has_more must be set");
	let exact = store.range_batch(OP, all_range(), 5);
	assert_eq!(exact.items.len(), 5);
	assert!(!exact.has_more, "the full result fit the batch, so has_more must be clear");
}

#[test]
fn prefix_scan_does_not_leak_adjacent_prefixes() {
	// The prefix end bound is computed by incrementing the last byte; an off-by-one there leaks the
	// lexicographically adjacent prefix into the result.
	let store = manual_store();

	store.set(OP, key(&[1, 2, 2, 9]), value(b"below"));
	store.set(OP, key(&[1, 2, 3]), value(b"exact"));
	store.set(OP, key(&[1, 2, 3, 0]), value(b"inside"));
	store.set(OP, key(&[1, 2, 3, 255]), value(b"inside_high"));
	store.set(OP, key(&[1, 2, 4]), value(b"adjacent"));
	store.set(OP, key(&[1, 2, 4, 0]), value(b"adjacent_child"));

	let batch = store.prefix_batch(OP, &[1, 2, 3], 100);
	let keys: Vec<&[u8]> = batch.items.iter().map(|(key, _)| key.as_slice()).collect();
	assert_eq!(keys, vec![[1, 2, 3].as_slice(), &[1, 2, 3, 0], &[1, 2, 3, 255]]);
}

#[test]
fn merge_drops_superseded_values() {
	// Compaction exists to cancel superseded state instead of reaping it later: after a compact the
	// newest value must remain readable while the accounting shows the older copy is gone.
	let store = manual_store();

	store.set(OP, key(b"k"), value(&[7u8; 512]));
	store.freeze(OP);
	store.set(OP, key(b"k"), value(b"new"));
	store.freeze(OP);

	let before = store.bytes(OP);
	store.compact(OP, &FloorSpec::new());

	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"new")));
	assert!(
		store.bytes(OP) < before,
		"the superseded 512-byte copy must be cancelled by the merge: before={before} after={}",
		store.bytes(OP)
	);
}

#[test]
fn set_tombstone_pair_vanishes_entirely_at_an_oldest_merge() {
	// When a merge reaches the oldest batch nothing older can resurface, so a tombstone that has
	// masked its set must vanish with it; keeping either would leak permanent garbage per deleted key.
	let store = manual_store();

	store.set(OP, key(b"a"), value(b"v"));
	store.freeze(OP);
	store.remove(OP, &key(b"a"));
	assert!(store.bytes(OP) > 0);

	store.compact(OP, &FloorSpec::new());

	assert_eq!(store.get(OP, &key(b"a")), None);
	assert!(scan_all(&store, OP).is_empty());
	assert_eq!(store.bytes(OP), 0, "a fully cancelled arena must account exactly zero bytes");
	assert_eq!(store.total_bytes(), 0);
}

#[test]
fn tombstone_survives_a_merge_that_does_not_reach_the_oldest_batch() {
	// Load-bearing survival rule: a tombstone may only be dropped when no older batch could still
	// hold its key. Here the oldest batch is too large for the size-tiered pick, so the auto merge
	// covers only the two newest batches and the tombstone must survive it - dropping it would
	// resurrect the value from the untouched oldest batch.
	let store = store_with(1 << 40, 2);

	store.set(OP, key(b"a"), value(b"v1"));
	store.set(OP, key(b"bulk"), value(&[0u8; 4096]));
	store.freeze(OP);

	store.set(OP, key(b"b"), value(b"v2"));
	store.set(OP, key(b"d"), value(&[1u8; 1024]));
	store.freeze(OP);

	store.remove(OP, &key(b"a"));
	store.set(OP, key(b"d"), value(b"small"));
	store.freeze(OP);

	let before = store.bytes(OP);
	store.set(OP, key(b"trigger"), value(b"t"));

	assert!(
		store.bytes(OP) < before,
		"the third freeze put the arena over max_frozen, so this write must have merged the two newest batches \
		 and cancelled the superseded 1024-byte copy: before={before} after={}",
		store.bytes(OP)
	);
	assert_eq!(store.get(OP, &key(b"a")), None, "the tombstone must survive the non-oldest merge");
	assert_eq!(store.get(OP, &key(b"b")), Some(value(b"v2")));
	assert_eq!(store.get(OP, &key(b"d")), Some(value(b"small")));
	assert_eq!(store.get(OP, &key(b"bulk")), Some(value(&[0u8; 4096])));
}

#[test]
fn range_tombstone_swallows_covered_entries_and_obeys_the_survival_rule() {
	// A range tombstone must swallow covered entries from the batches it is merged with, survive a
	// non-oldest merge to keep masking the untouched oldest batch, and die at the oldest merge.
	let store = store_with(1 << 40, 2);

	store.set(OP, key(b"p/old"), value(&[0u8; 4096]));
	store.freeze(OP);

	store.set(OP, key(b"p/mid"), value(&[1u8; 1024]));
	store.freeze(OP);

	store.remove_range(OP, EncodedKeyRange::prefix(b"p/"));
	store.set(OP, key(b"q/keep"), value(b"kept"));
	store.freeze(OP);

	let before = store.bytes(OP);
	store.set(OP, key(b"trigger"), value(b"t"));

	assert!(
		store.bytes(OP) < before,
		"the merge of the two newest batches must have swallowed the covered 1024-byte entry: before={before} \
		 after={}",
		store.bytes(OP)
	);
	assert_eq!(store.get(OP, &key(b"p/mid")), None);
	assert_eq!(
		store.get(OP, &key(b"p/old")),
		None,
		"the range tombstone must survive the non-oldest merge and keep masking the oldest batch"
	);
	assert_eq!(store.get(OP, &key(b"q/keep")), Some(value(b"kept")));

	store.compact(OP, &FloorSpec::new());

	assert_eq!(store.get(OP, &key(b"p/old")), None);
	let items = scan_all(&store, OP);
	let keys: Vec<&[u8]> = items.iter().map(|(key, _)| key.as_slice()).collect();
	assert_eq!(keys, vec![b"q/keep".as_slice(), b"trigger"]);

	store.set(OP, key(b"p/reborn"), value(b"alive"));
	assert_eq!(
		store.get(OP, &key(b"p/reborn")),
		Some(value(b"alive")),
		"after the oldest merge the range tombstone is gone and the prefix is writable again"
	);
}

#[test]
fn floor_drops_strictly_below_the_cutoff_and_keeps_the_boundary() {
	// The floor replaces the retention sweep: an entry stamped strictly below its keyspace cutoff
	// dies at compaction, but an entry exactly AT the cutoff must survive - <= would silently widen
	// every retention window by one instant.
	let store = manual_store();

	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"old"), stamped(b"o", 999));
	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"edge"), stamped(b"e", 1000));
	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"fresh"), stamped(b"f", 1001));

	let floor = FloorSpec::new().with(Keyspace::ACCUMULATOR, DateTime::from_nanos(1000));
	store.compact(OP, &floor);

	assert_eq!(store.get(OP, &data_key(1, Keyspace::ACCUMULATOR, b"old")), None);
	assert_eq!(store.get(OP, &data_key(1, Keyspace::ACCUMULATOR, b"edge")), Some(stamped(b"e", 1000)));
	assert_eq!(store.get(OP, &data_key(1, Keyspace::ACCUMULATOR, b"fresh")), Some(stamped(b"f", 1001)));
}

#[test]
fn floor_only_drops_real_group_data_of_the_specified_keyspace() {
	// Cancellation-by-floor must be surgical: a cutoff for keyspace A must not touch keyspace B,
	// and identity keyspaces, node-scoped keys, and undecodable keys are never floor-droppable even
	// when their keyspace byte appears in the spec - those die only by tombstone or supersede.
	let store = manual_store();

	let in_spec = data_key(3, Keyspace::ACCUMULATOR, b"x");
	let other_keyspace = data_key(3, Keyspace::BUFFER, b"x");
	let counter = data_key(3, Keyspace::NODE_COUNTER, b"x");
	let node_scope = data_key(GroupId::NODE_SCOPE.0, Keyspace::ACCUMULATOR, b"x");
	let undecodable = key(&[0xAB]);
	assert_eq!(group_data_of_inner(undecodable.as_slice()), None);

	store.set(OP, in_spec.clone(), stamped(b"v", 10));
	store.set(OP, other_keyspace.clone(), stamped(b"v", 10));
	store.set(OP, counter.clone(), stamped(b"v", 10));
	store.set(OP, node_scope.clone(), stamped(b"v", 10));
	store.set(OP, undecodable.clone(), stamped(b"v", 10));

	let floor = FloorSpec::new()
		.with(Keyspace::ACCUMULATOR, DateTime::from_nanos(1000))
		.with(Keyspace::NODE_COUNTER, DateTime::from_nanos(1000));
	store.compact(OP, &floor);

	assert_eq!(store.get(OP, &in_spec), None, "expired real-group data in the spec must be dropped");
	assert_eq!(store.get(OP, &other_keyspace), Some(stamped(b"v", 10)), "keyspace B must outlive a floor for A");
	assert_eq!(store.get(OP, &counter), Some(stamped(b"v", 10)), "identity keyspaces are never floor-droppable");
	assert_eq!(store.get(OP, &node_scope), Some(stamped(b"v", 10)), "node-scope keys are never floor-droppable");
	assert_eq!(store.get(OP, &undecodable), Some(stamped(b"v", 10)), "undecodable keys are never floor-droppable");
}

#[test]
fn a_data_wide_floor_reaches_every_data_keyspace_but_an_explicit_entry_outranks_it() {
	// An operator cannot enumerate every keyspace its state lives in (a guest invents its own above
	// FIRST_CUSTOM), so the data-wide floor must reach data keyspaces it never heard of, while an
	// explicit per-keyspace cutoff overrides it for the keyspaces that age on their own horizon.
	// Identity keyspaces and node scope stay immune even under a data-wide floor.
	// Mutation falsified against: cutoff() resolving the data default before the explicit entry
	// (JOIN_LEFT row would die), and the data default applying to non-data keyspaces (counter dies).
	let store = manual_store();

	let custom = data_key(3, Keyspace::FIRST_CUSTOM, b"x");
	let accumulator = data_key(3, Keyspace::ACCUMULATOR, b"x");
	let overridden = data_key(3, Keyspace::JOIN_LEFT, b"x");
	let counter = data_key(3, Keyspace::NODE_COUNTER, b"x");
	let node_scope = data_key(GroupId::NODE_SCOPE.0, Keyspace::ACCUMULATOR, b"x");

	for key in [&custom, &accumulator, &overridden, &counter, &node_scope] {
		store.set(OP, key.clone(), stamped(b"v", 500));
	}

	let floor = FloorSpec::data(DateTime::from_nanos(1_000)).with(Keyspace::JOIN_LEFT, DateTime::from_nanos(100));
	let outcome = store.compact(OP, &floor);

	assert_eq!(store.get(OP, &custom), None, "a never-declared custom keyspace must obey the data-wide floor");
	assert_eq!(store.get(OP, &accumulator), None, "a known data keyspace must obey the data-wide floor");
	assert_eq!(
		store.get(OP, &overridden),
		Some(stamped(b"v", 500)),
		"an explicit per-keyspace cutoff must outrank the data-wide floor"
	);
	assert_eq!(store.get(OP, &counter), Some(stamped(b"v", 500)), "identity stays immune to a data-wide floor");
	assert_eq!(store.get(OP, &node_scope), Some(stamped(b"v", 500)), "node scope stays immune");
	assert_eq!(outcome.dropped, 2, "exactly the two floor-expired rows are reported dropped");
	assert!(outcome.reclaimed_bytes > 0, "the dropped rows' bytes must be reported reclaimed");
}

#[test]
fn an_epoch_stamped_row_is_never_floor_dropped() {
	// An unstamped row is byte-identical to one written at the epoch, so the floor refuses both:
	// the writer-stamp contract fails safe as retention (a visible leak) rather than as silent
	// deletion of live state. A legitimate event AT the epoch is retained for the same reason.
	// Mutation falsified against: removing the epoch guard (the row dies under any positive cutoff).
	let store = manual_store();
	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"unstamped"), stamped(b"v", 0));

	store.compact(OP, &FloorSpec::data(DateTime::from_nanos(u64::MAX)));

	assert_eq!(
		store.get(OP, &data_key(1, Keyspace::ACCUMULATOR, b"unstamped")),
		Some(stamped(b"v", 0)),
		"an epoch-stamped row must survive every floor"
	);
}

#[test]
fn compaction_reports_zero_work_when_nothing_expires() {
	// The outcome feeds per-operator reclamation counters that stay quiet at zero; a compaction that
	// merged batches without dropping a floored row must not fabricate activity.
	// Mutation falsified against: counting every merged entry as dropped instead of only
	// floor-expired ones.
	let store = manual_store();
	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"live"), stamped(b"v", 5_000));
	store.freeze(OP);
	store.set(OP, data_key(1, Keyspace::ACCUMULATOR, b"also"), stamped(b"v", 6_000));

	let outcome = store.compact(OP, &FloorSpec::data(DateTime::from_nanos(1_000)));

	assert_eq!(outcome.dropped, 0);
	assert!(outcome.is_noop() || outcome.reclaimed_bytes > 0, "merge overhead may shrink bytes, never grow them");
	assert_eq!(scan_all(&store, OP).len(), 2, "both live rows survive");
}

#[test]
fn the_max_cutoff_spans_explicit_entries_and_the_data_floor() {
	// The frontier column reports the operator's most advanced floor; taking only the explicit
	// entries would report a window operator (data-wide floor only) as permanently none.
	// Mutation falsified against: min instead of max, and ignoring the data default.
	let explicit_newer =
		FloorSpec::data(DateTime::from_nanos(10)).with(Keyspace::JOIN_LEFT, DateTime::from_nanos(99));
	assert_eq!(explicit_newer.max_cutoff(), Some(DateTime::from_nanos(99)));

	let data_newer = FloorSpec::data(DateTime::from_nanos(50)).with(Keyspace::JOIN_LEFT, DateTime::from_nanos(7));
	assert_eq!(data_newer.max_cutoff(), Some(DateTime::from_nanos(50)));

	assert_eq!(FloorSpec::new().max_cutoff(), None, "an empty spec has no frontier to report");
}

#[test]
fn byte_accounting_tracks_every_transition_and_reaches_zero() {
	// The byte counters feed memory metrics, so they must move with every mutation: grow on set,
	// shrink on overwrite-with-smaller, stay put across freeze, and reach exactly zero once
	// everything is cancelled - drift here silently corrupts the residency numbers.
	let store = manual_store();
	assert_eq!(store.bytes(OP), 0);
	assert_eq!(store.total_bytes(), 0);

	store.set(OP, key(b"big"), value(&[0u8; 1024]));
	let after_big = store.bytes(OP);
	assert!(after_big > 1024);

	store.set(OP, key(b"small"), value(b"s"));
	let after_small = store.bytes(OP);
	assert!(after_small > after_big);

	store.set(OP, key(b"big"), value(&[0u8; 8]));
	let after_shrink = store.bytes(OP);
	assert!(after_shrink < after_small, "overwriting with a smaller value must release the difference");

	store.freeze(OP);
	assert_eq!(store.bytes(OP), after_shrink, "freezing moves entries between batches without changing residency");

	store.remove(OP, &key(b"small"));
	assert!(store.bytes(OP) > after_shrink, "a tombstone above a frozen batch occupies bytes until merged");

	store.compact(OP, &FloorSpec::new());
	assert!(store.bytes(OP) < after_shrink, "the merge must cancel the tombstoned pair");

	let other = OperatorId(2);
	store.set(other, key(b"o"), value(b"v"));
	assert_eq!(store.total_bytes(), store.bytes(OP) + store.bytes(other));

	store.clear(OP);
	assert_eq!(store.bytes(OP), 0);
	assert_eq!(store.total_bytes(), store.bytes(other));
}

#[test]
fn upper_round_trips_per_operator() {
	// upper is recovery bookkeeping: it must default to zero, round-trip what the writer set, and
	// stay isolated per operator.
	let store = manual_store();

	assert_eq!(store.upper(OP), CommitVersion(0));
	store.set_upper(OP, CommitVersion(42));
	assert_eq!(store.upper(OP), CommitVersion(42));
	assert_eq!(store.upper(OperatorId(2)), CommitVersion(0));

	store.set_upper(OP, CommitVersion(43));
	assert_eq!(store.upper(OP), CommitVersion(43));
}

#[test]
fn drop_arena_removes_state_and_accounting() {
	// Dropping an arena is how a dropped operator releases everything at once: state, upper, and
	// its contribution to the store total must all go, while other operators stay intact.
	let store = manual_store();
	let other = OperatorId(2);

	store.set(OP, key(b"k"), value(&[0u8; 256]));
	store.set_upper(OP, CommitVersion(7));
	store.set(other, key(b"k"), value(b"other"));
	let other_bytes = store.bytes(other);

	store.drop_arena(OP);

	assert_eq!(store.get(OP, &key(b"k")), None);
	assert_eq!(store.bytes(OP), 0);
	assert_eq!(store.upper(OP), CommitVersion(0));
	assert_eq!(store.total_bytes(), other_bytes);
	assert_eq!(store.get(other, &key(b"k")), Some(value(b"other")));
}

#[test]
fn operators_are_fully_isolated() {
	// Each arena IS its operator: the same inner key must resolve independently per operator, and
	// removes or scans against one operator must never touch another.
	let store = manual_store();
	let op2 = OperatorId(2);

	store.set(OP, key(b"k"), value(b"one"));
	store.set(op2, key(b"k"), value(b"two"));
	assert_eq!(store.get(OP, &key(b"k")), Some(value(b"one")));
	assert_eq!(store.get(op2, &key(b"k")), Some(value(b"two")));

	store.remove(OP, &key(b"k"));
	assert_eq!(store.get(OP, &key(b"k")), None);
	assert_eq!(store.get(op2, &key(b"k")), Some(value(b"two")));

	assert!(scan_all(&store, OP).is_empty());
	assert_eq!(scan_all(&store, op2), vec![(key(b"k"), value(b"two"))]);
}

struct Xorshift(u64);

impl Xorshift {
	fn next(&mut self) -> u64 {
		// Deterministic xorshift64 so the differential run is exactly reproducible from the seed.
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
}

fn model_range_contains(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
	let after_start = match &range.start {
		Bound::Included(start) => key >= start,
		Bound::Excluded(start) => key > start,
		Bound::Unbounded => true,
	};
	let before_end = match &range.end {
		Bound::Included(end) => key <= end,
		Bound::Excluded(end) => key < end,
		Bound::Unbounded => true,
	};
	after_start && before_end
}

fn model_floor_expired(floor: &[(Keyspace, u64)], key: &EncodedKey, row: &EncodedBytes) -> bool {
	// The model applies the documented floor semantics directly on the visible map, using the same
	// core decode helpers as the trusted classification oracle: only real-group data keyspaces with
	// a cutoff in the spec expire, strictly below the cutoff.
	let Some(group) = group_data_of_inner(key.as_slice()) else {
		return false;
	};
	if group.is_node_scope() {
		return false;
	}
	let Some((_, keyspace, _)) = OperatorGroupStateKey::decode_inner(key.as_slice()) else {
		return false;
	};
	let Some((_, cutoff)) = floor.iter().find(|(candidate, _)| *candidate == keyspace) else {
		return false;
	};
	row.updated_at() < DateTime::from_nanos(*cutoff)
}

#[test]
fn randomized_operations_match_a_naive_model() {
	// Differential test: thousands of random set/remove/remove_range/freeze/compact ops against a
	// tiny-threshold store (so freezes and partial auto-merges churn constantly) must leave exactly
	// the visible state of a naive BTreeMap applying the same semantics. Partial merges use an
	// empty floor and may never change visible state; explicit compacts apply a random floor to
	// both sides and the full scan is compared after every one.
	let mut rng = Xorshift(0x9E3779B97F4A7C15);
	let store = store_with(400, 3);
	let mut model: BTreeMap<EncodedKey, EncodedBytes> = BTreeMap::new();

	let keyspaces = [Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::NODE_COUNTER, Keyspace::FIRST_CUSTOM];

	let random_key = |rng: &mut Xorshift| -> EncodedKey {
		match rng.next() % 10 {
			0 => key(&[0xAB]),
			1 => data_key(GroupId::NODE_SCOPE.0, Keyspace::ACCUMULATOR, &[(rng.next() % 8) as u8]),
			_ => {
				let group = 1 + rng.next() % 3;
				let keyspace = keyspaces[(rng.next() % keyspaces.len() as u64) as usize];
				data_key(group, keyspace, &[(rng.next() % 16) as u8])
			}
		}
	};

	let mut compacts = 0usize;
	for _ in 0..4000 {
		match rng.next() % 100 {
			0..50 => {
				// Stamps start at 1: an epoch stamp is the never-floored sentinel (see
				// an_epoch_stamped_row_is_never_floor_dropped), and the naive model does not
				// replicate that carve-out.
				let key = random_key(&mut rng);
				let updated_at = 1 + rng.next() % 127;
				let payload = vec![(rng.next() % 256) as u8; (rng.next() % 24) as usize];
				let row = stamped(&payload, updated_at);
				model.insert(key.clone(), row.clone());
				store.set(OP, key, row);
			}
			50..68 => {
				let key = random_key(&mut rng);
				model.remove(&key);
				store.remove(OP, &key);
			}
			68..78 => {
				let range = if rng.next() % 2 == 0 {
					let group = GroupId(1 + rng.next() % 3);
					let keyspace = keyspaces[(rng.next() % keyspaces.len() as u64) as usize];
					keyspace_inner_range(group, keyspace)
				} else {
					let a = random_key(&mut rng);
					let b = random_key(&mut rng);
					let (low, high) = if a <= b {
						(a, b)
					} else {
						(b, a)
					};
					EncodedKeyRange::new(Bound::Included(low), Bound::Excluded(high))
				};
				model.retain(|key, _| !model_range_contains(&range, key));
				store.remove_range(OP, range);
			}
			78..88 => {
				store.freeze(OP);
			}
			_ => {
				let mut floor_pairs: Vec<(Keyspace, u64)> = Vec::new();
				let mut floor = FloorSpec::new();
				for _ in 0..rng.next() % 3 {
					let keyspace = keyspaces[(rng.next() % keyspaces.len() as u64) as usize];
					let cutoff = rng.next() % 128;
					floor = floor.with(keyspace, DateTime::from_nanos(cutoff));
					floor_pairs.retain(|(candidate, _)| *candidate != keyspace);
					floor_pairs.push((keyspace, cutoff));
				}
				store.compact(OP, &floor);
				model.retain(|key, row| !model_floor_expired(&floor_pairs, key, row));
				compacts += 1;

				let scanned = scan_all(&store, OP);
				let expected: Vec<(EncodedKey, EncodedBytes)> =
					model.iter().map(|(key, row)| (key.clone(), row.clone())).collect();
				assert_eq!(
					scanned, expected,
					"visible state diverged from the model after compact #{compacts}"
				);
			}
		}
	}
	assert!(compacts > 100, "the op mix must actually exercise compaction, got {compacts}");

	store.compact(OP, &FloorSpec::new());
	let scanned = scan_all(&store, OP);
	let expected: Vec<(EncodedKey, EncodedBytes)> =
		model.iter().map(|(key, row)| (key.clone(), row.clone())).collect();
	assert_eq!(scanned, expected, "final visible state diverged from the model");

	store.clear(OP);
	assert_eq!(store.bytes(OP), 0);
	assert_eq!(store.total_bytes(), 0);
	assert!(scan_all(&store, OP).is_empty());
}
