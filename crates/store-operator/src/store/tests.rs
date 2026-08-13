// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread::spawn;

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use rusqlite::params;

use super::{
	ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, ANCHORS_BY_EXPIRY_SQL, OperatorSealAnchor, OperatorSealAnchorCensus,
	OperatorStateCensus, OperatorStore, OperatorWrite,
};

const PREFIX: u32 = 9;

const LEFT: u8 = 0;
const RIGHT: u8 = 1;
const APPEND: u8 = 0xFF;
const PAGE: u64 = 64;

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

fn anchor(side: u8, row_number: u64, millis: u64) -> OperatorSealAnchor {
	OperatorSealAnchor {
		side,
		row_number: RowNumber(row_number),
		expiry: DateTime::from_millis(millis),
	}
}

#[test]
fn an_anchor_round_trips_through_a_point_read() {
	let store = OperatorStore::memory();

	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(5_000));

	assert_eq!(
		store.anchor_get(OperatorId(1), GroupId(7), LEFT, RowNumber(42)),
		Some(DateTime::from_millis(5_000)),
		"an expiry must survive the round trip through SQLite's signed integers"
	);
	assert_eq!(
		store.anchor_get(OperatorId(1), GroupId(7), RIGHT, RowNumber(42)),
		None,
		"the side is part of the identity, so the other side must not answer"
	);
	assert_eq!(store.anchor_get(OperatorId(1), GroupId(8), LEFT, RowNumber(42)), None, "nor must another group");
	assert_eq!(store.anchor_get(OperatorId(2), GroupId(7), LEFT, RowNumber(42)), None, "nor another operator");
}

#[test]
fn an_expiry_beyond_the_signed_boundary_of_a_millisecond_still_orders_after_an_earlier_one() {
	let store = OperatorStore::memory();
	let far = 1_000_000_000_000u64;

	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(far));
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(2), DateTime::from_millis(5_000));

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE),
		vec![anchor(LEFT, 2, 5_000), anchor(LEFT, 1, far)]
	);
}

#[test]
fn re_arming_an_anchor_moves_its_expiry_instead_of_minting_a_second_row() {
	let store = OperatorStore::memory();

	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(9_000));

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE),
		vec![anchor(LEFT, 42, 9_000)],
		"a moved anchor must be one row at the new expiry, never two"
	);
}

#[test]
fn an_append_tuple_never_collides_with_either_join_side() {
	let store = OperatorStore::memory();

	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(0), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(0), DateTime::from_millis(6_000));
	store.anchor_set(OperatorId(1), GroupId(7), APPEND, RowNumber(0), DateTime::from_millis(7_000));

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE),
		vec![anchor(LEFT, 0, 5_000), anchor(RIGHT, 0, 6_000), anchor(APPEND, 0, 7_000)]
	);
	assert_eq!(
		store.anchor_get(OperatorId(1), GroupId(7), APPEND, RowNumber(0)),
		Some(DateTime::from_millis(7_000)),
		"a side above the join tags must read back as itself"
	);
}

#[test]
fn anchors_come_back_earliest_first_and_the_limit_cuts_the_tail() {
	let store = OperatorStore::memory();
	for (row_number, millis) in [(1u64, 9_000u64), (2, 5_000), (3, 7_000)] {
		store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(row_number), DateTime::from_millis(millis));
	}

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), 2),
		vec![anchor(LEFT, 2, 5_000), anchor(LEFT, 3, 7_000)],
		"a capped read must cut in expiry order, earliest first"
	);
	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE).len(),
		3,
		"and the cap must not have consumed what it left behind"
	);
}

#[test]
fn only_the_addressed_group_answers_a_scan() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(1_000));
	store.anchor_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(2_000));

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE),
		vec![anchor(LEFT, 1, 5_000)],
		"an earlier anchor of a neighbouring group or operator must not leak into the scan"
	);
}

#[test]
fn the_due_scan_takes_the_boundary_instant_and_stops_there() {
	let store = OperatorStore::memory();
	for (row_number, millis) in [(1u64, 5_000u64), (2, 6_000), (3, 7_000)] {
		store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(row_number), DateTime::from_millis(millis));
	}

	assert_eq!(
		store.anchors_due(OperatorId(1), GroupId(7), DateTime::from_millis(6_000), PAGE),
		vec![anchor(LEFT, 1, 5_000), anchor(LEFT, 2, 6_000)],
		"the watermark instant itself is due"
	);
	assert_eq!(
		store.anchors_due(OperatorId(1), GroupId(7), DateTime::from_millis(4_999), PAGE),
		Vec::new(),
		"and nothing before the earliest expiry is"
	);
}

#[test]
fn the_due_scan_pages_from_the_earliest_end() {
	let store = OperatorStore::memory();
	for row_number in 1u64..=5 {
		store.anchor_set(
			OperatorId(1),
			GroupId(7),
			LEFT,
			RowNumber(row_number),
			DateTime::from_millis(row_number * 1_000),
		);
	}

	assert_eq!(
		store.anchors_due(OperatorId(1), GroupId(7), DateTime::from_millis(5_000), 2),
		vec![anchor(LEFT, 1, 1_000), anchor(LEFT, 2, 2_000)]
	);
	store.anchor_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(1));
	store.anchor_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(2));
	assert_eq!(
		store.anchors_due(OperatorId(1), GroupId(7), DateTime::from_millis(5_000), 2),
		vec![anchor(LEFT, 3, 3_000), anchor(LEFT, 4, 4_000)],
		"a page must resume at the earliest anchor still standing"
	);
}

#[test]
fn removing_one_anchor_leaves_its_siblings_standing() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(2), DateTime::from_millis(6_000));
	store.anchor_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(1), DateTime::from_millis(7_000));

	store.anchor_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(1));

	assert_eq!(
		store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE),
		vec![anchor(LEFT, 2, 6_000), anchor(RIGHT, 1, 7_000)]
	);
}

#[test]
fn removing_a_group_leaves_its_neighbours_intact() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(2), DateTime::from_millis(6_000));
	store.anchor_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(7_000));
	store.anchor_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(8_000));

	store.anchors_remove_group(OperatorId(1), GroupId(7));

	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(8), PAGE), vec![anchor(LEFT, 1, 7_000)]);
	assert_eq!(
		store.anchors_by_expiry(OperatorId(2), GroupId(7), PAGE),
		vec![anchor(LEFT, 1, 8_000)],
		"the same group under another operator must survive"
	);
}

#[test]
fn dropping_an_operator_takes_every_group_it_owns_and_no_other() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(6_000));
	store.anchor_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(7_000));

	store.anchors_drop_operator(OperatorId(1));

	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(8), PAGE), Vec::new());
	assert_eq!(store.anchors_by_expiry(OperatorId(2), GroupId(7), PAGE), vec![anchor(LEFT, 1, 7_000)]);
}

#[test]
fn the_by_expiry_scan_is_answered_by_a_covering_index() {
	let store = OperatorStore::memory();
	for row_number in 1u64..=8 {
		store.anchor_set(
			OperatorId(1),
			GroupId(7),
			LEFT,
			RowNumber(row_number),
			DateTime::from_millis(row_number * 1_000),
		);
	}

	let guard = store.inner.conn.lock();
	let conn = guard.as_ref().expect("the memory store holds its connection");
	let mut stmt = conn
		.prepare(&format!("EXPLAIN QUERY PLAN {}", ANCHORS_BY_EXPIRY_SQL))
		.expect("the by-expiry statement must parse");
	let mut rows = stmt.query(params![1i64, 7i64, 4i64]).expect("the plan must be readable");
	let mut plan: Vec<String> = Vec::new();
	while let Some(row) = rows.next().expect("the plan must be readable") {
		plan.push(row.get(3).expect("a query plan row carries a detail string"));
	}

	assert_eq!(plan.len(), 1, "the scan must be one step, not a join or a sorter: {:?}", plan);
	assert!(
		plan[0].contains("USING COVERING INDEX operator_seal_anchor_due"),
		"the index must answer the scan on its own, without a lookup into the primary key: {:?}",
		plan
	);
}

#[test]
fn a_long_suffix_never_lengthens_the_census_prefix() {
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

#[test]
fn a_batch_lands_operator_state_and_its_anchors_together() {
	let store = OperatorStore::memory();

	store.apply_batch(&[
		OperatorWrite::Set {
			operator: OperatorId(1),
			key: real_key(GroupId(7), Keyspace(0x1D), &[1]),
			row: row(4),
		},
		OperatorWrite::AnchorSet {
			operator: OperatorId(1),
			group: GroupId(7),
			side: LEFT,
			row_number: RowNumber(1),
			expiry: DateTime::from_millis(5_000),
		},
	]);

	assert!(store.contains(OperatorId(1), &real_key(GroupId(7), Keyspace(0x1D), &[1])));
	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), vec![anchor(LEFT, 1, 5_000)]);
}

#[test]
fn a_batch_applies_a_set_and_a_later_remove_of_the_same_row_in_order() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(1_000));

	store.apply_batch(&[
		OperatorWrite::AnchorSet {
			operator: OperatorId(1),
			group: GroupId(7),
			side: LEFT,
			row_number: RowNumber(1),
			expiry: DateTime::from_millis(9_000),
		},
		OperatorWrite::AnchorRemove {
			operator: OperatorId(1),
			group: GroupId(7),
			side: LEFT,
			row_number: RowNumber(1),
		},
	]);

	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), Vec::new());
}

#[test]
fn an_empty_batch_touches_nothing() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(1_000));

	store.apply_batch(&[]);

	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), vec![anchor(LEFT, 1, 1_000)]);
}

#[test]
fn anchors_are_counted_in_the_byte_accounting_of_their_operator() {
	let store = OperatorStore::memory();
	let empty = store.total_bytes();

	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));

	let one = ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES;
	assert_eq!(store.total_bytes(), empty + one, "one anchor must add its own fixed width");
	assert_eq!(store.bytes(OperatorId(1)), one);
	assert_eq!(store.bytes(OperatorId(2)), 0, "and it must be charged to its own operator only");
}

#[test]
fn dropping_an_operators_state_takes_its_anchors_with_it() {
	let store = OperatorStore::memory();
	store.set(OperatorId(1), real_key(GroupId(7), Keyspace(0x1D), &[1]), row(4));
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(6_000));

	store.drop_operator_state(OperatorId(1));

	assert_eq!(store.anchors_by_expiry(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.bytes(OperatorId(1)), 0, "a dropped operator must leave no bytes behind");
	assert_eq!(
		store.anchors_by_expiry(OperatorId(2), GroupId(7), PAGE),
		vec![anchor(LEFT, 1, 6_000)],
		"and it must not reach into a neighbour's anchors"
	);
}

#[test]
fn the_anchor_census_reports_one_bucket_per_operator_and_group() {
	let store = OperatorStore::memory();
	store.anchor_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.anchor_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(1), DateTime::from_millis(6_000));
	store.anchor_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(7_000));
	store.anchor_set(OperatorId(2), GroupId(7), APPEND, RowNumber(0), DateTime::from_millis(8_000));

	let census = store.anchor_census();

	assert_eq!(
		census,
		vec![
			OperatorSealAnchorCensus {
				operator: OperatorId(1),
				group: GroupId(7),
				keys: 2,
			},
			OperatorSealAnchorCensus {
				operator: OperatorId(1),
				group: GroupId(8),
				keys: 1,
			},
			OperatorSealAnchorCensus {
				operator: OperatorId(2),
				group: GroupId(7),
				keys: 1,
			},
		]
	);
}

#[test]
fn the_in_memory_store_serves_reads_from_its_only_connection() {
	let store = OperatorStore::memory();

	assert_eq!(store.page_cache_metrics().connections_total.as_u64(), 1);
}

#[test]
fn the_sqlite_store_opens_one_reader_per_configured_pool_slot() {
	let (config, _guard) = SqliteConfig::test();
	let pool_size = config.read_pool_size as u64;
	let store = OperatorStore::sqlite(config);

	assert_eq!(store.page_cache_metrics().connections_total.as_u64(), 1 + pool_size);
}

#[test]
fn a_pooled_reader_sees_a_write_the_moment_the_writer_commits() {
	let (config, _guard) = SqliteConfig::test();
	let store = OperatorStore::sqlite(config);
	let probe = key(7, 0x10, 1);

	store.set(OperatorId(1), probe.clone(), row(4));

	assert!(store.contains(OperatorId(1), &probe));
	assert!(store.get(OperatorId(1), &probe).is_some());

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OperatorId(1),
		key: probe.clone(),
	}]);

	assert!(!store.contains(OperatorId(1), &probe), "a committed batch must be visible to the pool too");
}

#[test]
fn concurrent_reads_during_writes_do_not_deadlock() {
	let (config, _guard) = SqliteConfig::test();
	let store = OperatorStore::sqlite(config);
	let probe = key(7, 0x10, 1);
	store.set(OperatorId(1), probe.clone(), row(4));

	let readers: Vec<_> = (0..4)
		.map(|_| {
			let store = store.clone();
			let probe = probe.clone();
			spawn(move || {
				for _ in 0..500 {
					assert!(store.contains(OperatorId(1), &probe));
				}
			})
		})
		.collect();

	for i in 0..200u64 {
		store.set(OperatorId(2), key(8, 0x10, (i % 251) as u8), row(8));
	}

	for reader in readers {
		reader.join().expect("reader thread panicked (deadlock or read error)");
	}
}
