// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::state::{GroupId, KeyspaceId, OperatorStateKey},
		typed::direction::Asc,
	},
	state::typed::SuffixBytes,
};
use reifydb_sqlite::SqliteConfig;
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};
use rusqlite::params;

use crate::{
	tier::{
		persistent::sqlite::{SqliteOperatorStorage as OperatorStore, sql::JOIN_EXPIRIES_BY_TIME_SQL},
		resident::batch::FlushBatch,
	},
	types::{JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES, StoredJoinRowExpiry, StoredJoinRowExpiryCensus},
};

const LEFT: u8 = 0;
const RIGHT: u8 = 1;
const APPEND: u8 = 0xFF;
const PAGE: u64 = 64;

fn real_key(group: GroupId, keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn join_left_key(group: GroupId, row_num: u64) -> EncodedKey {
	real_key(group, KeyspaceId::JOIN_LEFT, &Asc(RowNumber(row_num)).to_suffix_bytes())
}

fn state_batch(writes: &[(OperatorId, GroupId, u64, Option<EncodedPodRow>)]) -> FlushBatch {
	let mut batch = FlushBatch::default();
	for (operator, group, row_num, post) in writes {
		batch.state.record_bytes(
			*operator,
			KeyspaceId::JOIN_LEFT,
			*group,
			&Asc(RowNumber(*row_num)).to_suffix_bytes(),
			post.clone(),
		);
	}
	batch
}

fn row(len: usize) -> EncodedPodRow {
	EncodedPodRow::new(&vec![0u8; len])
}

fn join_expiry(side: u8, row_number: u64, millis: u64) -> StoredJoinRowExpiry {
	StoredJoinRowExpiry {
		side,
		row_number: RowNumber(row_number),
		at: DateTime::from_millis(millis),
	}
}

#[test]
fn a_join_expiry_round_trips_through_a_point_read() {
	let (store, _guard) = OperatorStore::in_memory();

	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(5_000));

	assert_eq!(
		store.join_expiry_get(OperatorId(1), GroupId(7), LEFT, RowNumber(42)),
		Some(DateTime::from_millis(5_000)),
		"an expiry must survive the round trip through SQLite's signed integers"
	);
	assert_eq!(
		store.join_expiry_get(OperatorId(1), GroupId(7), RIGHT, RowNumber(42)),
		None,
		"the side is part of the identity, so the other side must not answer"
	);
	assert_eq!(
		store.join_expiry_get(OperatorId(1), GroupId(8), LEFT, RowNumber(42)),
		None,
		"nor must another group"
	);
	assert_eq!(store.join_expiry_get(OperatorId(2), GroupId(7), LEFT, RowNumber(42)), None, "nor another operator");
}

#[test]
fn an_expiry_beyond_the_signed_boundary_of_a_millisecond_still_orders_after_an_earlier_one() {
	let (store, _guard) = OperatorStore::in_memory();
	let far = 1_000_000_000_000u64;

	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(far));
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(2), DateTime::from_millis(5_000));

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 2, 5_000), join_expiry(LEFT, 1, far)]
	);
}

#[test]
fn re_arming_a_join_expiry_moves_its_expiry_instead_of_minting_a_second_row() {
	let (store, _guard) = OperatorStore::in_memory();

	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(42), DateTime::from_millis(9_000));

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 42, 9_000)],
		"a moved join expiry must be one row at the new expiry, never two"
	);
}

#[test]
fn an_append_tuple_never_collides_with_either_join_side() {
	let (store, _guard) = OperatorStore::in_memory();

	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(0), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(0), DateTime::from_millis(6_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), APPEND, RowNumber(0), DateTime::from_millis(7_000));

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 0, 5_000), join_expiry(RIGHT, 0, 6_000), join_expiry(APPEND, 0, 7_000)]
	);
	assert_eq!(
		store.join_expiry_get(OperatorId(1), GroupId(7), APPEND, RowNumber(0)),
		Some(DateTime::from_millis(7_000)),
		"a side above the join tags must read back as itself"
	);
}

#[test]
fn join_expiries_come_back_earliest_first_and_the_limit_cuts_the_tail() {
	let (store, _guard) = OperatorStore::in_memory();
	for (row_number, millis) in [(1u64, 9_000u64), (2, 5_000), (3, 7_000)] {
		store.join_expiry_set(
			OperatorId(1),
			GroupId(7),
			LEFT,
			RowNumber(row_number),
			DateTime::from_millis(millis),
		);
	}

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), 2),
		vec![join_expiry(LEFT, 2, 5_000), join_expiry(LEFT, 3, 7_000)],
		"a capped read must cut in expiry order, earliest first"
	);
	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE).len(),
		3,
		"and the cap must not have consumed what it left behind"
	);
}

#[test]
fn only_the_addressed_group_answers_a_scan() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(1_000));
	store.join_expiry_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(2_000));

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 1, 5_000)],
		"an earlier join expiry of a neighbouring group or operator must not leak into the scan"
	);
}

#[test]
fn the_due_scan_takes_the_boundary_instant_and_stops_there() {
	let (store, _guard) = OperatorStore::in_memory();
	for (row_number, millis) in [(1u64, 5_000u64), (2, 6_000), (3, 7_000)] {
		store.join_expiry_set(
			OperatorId(1),
			GroupId(7),
			LEFT,
			RowNumber(row_number),
			DateTime::from_millis(millis),
		);
	}

	assert_eq!(
		store.join_expiries_due(OperatorId(1), GroupId(7), DateTime::from_millis(6_000), PAGE),
		vec![join_expiry(LEFT, 1, 5_000), join_expiry(LEFT, 2, 6_000)],
		"the watermark instant itself is due"
	);
	assert_eq!(
		store.join_expiries_due(OperatorId(1), GroupId(7), DateTime::from_millis(4_999), PAGE),
		Vec::new(),
		"and nothing before the earliest expiry is"
	);
}

#[test]
fn the_due_scan_pages_from_the_earliest_end() {
	let (store, _guard) = OperatorStore::in_memory();
	for row_number in 1u64..=5 {
		store.join_expiry_set(
			OperatorId(1),
			GroupId(7),
			LEFT,
			RowNumber(row_number),
			DateTime::from_millis(row_number * 1_000),
		);
	}

	assert_eq!(
		store.join_expiries_due(OperatorId(1), GroupId(7), DateTime::from_millis(5_000), 2),
		vec![join_expiry(LEFT, 1, 1_000), join_expiry(LEFT, 2, 2_000)]
	);
	store.join_expiry_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(1));
	store.join_expiry_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(2));
	assert_eq!(
		store.join_expiries_due(OperatorId(1), GroupId(7), DateTime::from_millis(5_000), 2),
		vec![join_expiry(LEFT, 3, 3_000), join_expiry(LEFT, 4, 4_000)],
		"a page must resume at the earliest join expiry still standing"
	);
}

#[test]
fn removing_one_join_expiry_leaves_its_siblings_standing() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(2), DateTime::from_millis(6_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(1), DateTime::from_millis(7_000));

	store.join_expiry_remove(OperatorId(1), GroupId(7), LEFT, RowNumber(1));

	assert_eq!(
		store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 2, 6_000), join_expiry(RIGHT, 1, 7_000)]
	);
}

#[test]
fn removing_a_group_leaves_its_neighbours_intact() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(2), DateTime::from_millis(6_000));
	store.join_expiry_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(7_000));
	store.join_expiry_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(8_000));

	store.join_expiries_remove_group(OperatorId(1), GroupId(7));

	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(8), PAGE), vec![join_expiry(LEFT, 1, 7_000)]);
	assert_eq!(
		store.join_expiries_by_time(OperatorId(2), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 1, 8_000)],
		"the same group under another operator must survive"
	);
}

#[test]
fn dropping_an_operator_takes_every_group_it_owns_and_no_other() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(6_000));
	store.join_expiry_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(7_000));

	store.join_expiries_drop_operator(OperatorId(1));

	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(8), PAGE), Vec::new());
	assert_eq!(store.join_expiries_by_time(OperatorId(2), GroupId(7), PAGE), vec![join_expiry(LEFT, 1, 7_000)]);
}

#[test]
fn the_by_expiry_scan_is_answered_by_a_covering_index() {
	let (store, _guard) = OperatorStore::in_memory();
	for row_number in 1u64..=8 {
		store.join_expiry_set(
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
		.prepare(&format!("EXPLAIN QUERY PLAN {}", JOIN_EXPIRIES_BY_TIME_SQL))
		.expect("the by-expiry statement must parse");
	let mut rows = stmt.query(params![1i64, 7i64, 4i64]).expect("the plan must be readable");
	let mut plan: Vec<String> = Vec::new();
	while let Some(row) = rows.next().expect("the plan must be readable") {
		plan.push(row.get(3).expect("a query plan row carries a detail string"));
	}

	assert_eq!(plan.len(), 1, "the scan must be one step, not a join or a sorter: {:?}", plan);
	assert!(
		plan[0].contains("USING COVERING INDEX operator_join_expiry_due"),
		"the index must answer the scan on its own, without a lookup into the primary key: {:?}",
		plan
	);
}

#[test]
fn a_batch_lands_operator_state_and_its_join_expiries_together() {
	let (store, _guard) = OperatorStore::in_memory();

	let mut batch = state_batch(&[(OperatorId(1), GroupId(7), 1, Some(row(4)))]);
	batch.join_expiries.insert((OperatorId(1), GroupId(7), LEFT, RowNumber(1)), Some(5_000));
	store.flush_batch(&batch);

	assert!(store.contains(OperatorId(1), &join_left_key(GroupId(7), 1)));
	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), vec![join_expiry(LEFT, 1, 5_000)]);
}

#[test]
fn a_batch_applies_a_set_and_a_later_remove_of_the_same_row_in_order() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(1_000));

	let mut batch = FlushBatch::default();
	batch.join_expiries.insert((OperatorId(1), GroupId(7), LEFT, RowNumber(1)), None);
	store.flush_batch(&batch);

	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), Vec::new());
}

#[test]
fn an_empty_batch_touches_nothing() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(1_000));

	store.flush_batch(&FlushBatch::default());

	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), vec![join_expiry(LEFT, 1, 1_000)]);
}

#[test]
fn join_expiries_are_counted_in_the_byte_accounting_of_their_operator() {
	let (store, _guard) = OperatorStore::in_memory();
	let empty = store.total_bytes();

	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));

	let one = JOIN_EXPIRY_KEY_BYTES + JOIN_EXPIRY_VALUE_BYTES;
	assert_eq!(store.total_bytes(), empty + one, "one join expiry must add its own fixed width");
	assert_eq!(store.bytes(OperatorId(1)), one);
	assert_eq!(store.bytes(OperatorId(2)), ByteSize::ZERO, "and it must be charged to its own operator only");
}

#[test]
fn dropping_an_operators_state_takes_its_join_expiries_with_it() {
	let (store, _guard) = OperatorStore::in_memory();
	store.flush_batch(&state_batch(&[(OperatorId(1), GroupId(7), 1, Some(row(4)))]));
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(2), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(6_000));

	store.drop_operator_state(OperatorId(1));

	assert_eq!(store.join_expiries_by_time(OperatorId(1), GroupId(7), PAGE), Vec::new());
	assert_eq!(store.bytes(OperatorId(1)), ByteSize::ZERO, "a dropped operator must leave no bytes behind");
	assert_eq!(
		store.join_expiries_by_time(OperatorId(2), GroupId(7), PAGE),
		vec![join_expiry(LEFT, 1, 6_000)],
		"and it must not reach into a neighbour's join expiries"
	);
}

#[test]
fn the_join_expiry_census_reports_one_bucket_per_operator() {
	let (store, _guard) = OperatorStore::in_memory();
	store.join_expiry_set(OperatorId(1), GroupId(7), LEFT, RowNumber(1), DateTime::from_millis(5_000));
	store.join_expiry_set(OperatorId(1), GroupId(7), RIGHT, RowNumber(1), DateTime::from_millis(6_000));
	store.join_expiry_set(OperatorId(1), GroupId(8), LEFT, RowNumber(1), DateTime::from_millis(7_000));
	store.join_expiry_set(OperatorId(2), GroupId(7), APPEND, RowNumber(0), DateTime::from_millis(8_000));

	let census = store.join_expiry_census();

	assert_eq!(
		census,
		vec![
			StoredJoinRowExpiryCensus {
				operator: OperatorId(1),
				keys: 3,
			},
			StoredJoinRowExpiryCensus {
				operator: OperatorId(2),
				keys: 1,
			},
		],
		"join expiries of every group must fold into their operator's single bucket"
	);
}

#[test]
fn the_in_memory_store_pools_readers_like_any_file_backed_one() {
	let (config, _config_guard) = SqliteConfig::in_memory();
	let expected = 1 + config.read_pool_size as u64;
	let (store, _guard) = OperatorStore::in_memory();

	assert_eq!(store.page_cache_metrics().connections_total.as_u64(), expected);
}

#[test]
fn the_sqlite_store_opens_one_reader_per_configured_pool_slot() {
	let (config, _guard) = SqliteConfig::test();
	let pool_size = config.read_pool_size as u64;
	let store = OperatorStore::new(config);

	assert_eq!(store.page_cache_metrics().connections_total.as_u64(), 1 + pool_size);
}

#[test]
fn a_pooled_reader_sees_a_write_the_moment_the_writer_commits() {
	let (config, _guard) = SqliteConfig::test();
	let store = OperatorStore::new(config);
	let probe = join_left_key(GroupId(7), 1);

	store.flush_batch(&state_batch(&[(OperatorId(1), GroupId(7), 1, Some(row(4)))]));

	assert!(store.contains(OperatorId(1), &probe));
	assert!(store.get(OperatorId(1), &probe).is_some());

	store.flush_batch(&state_batch(&[(OperatorId(1), GroupId(7), 1, None)]));

	assert!(!store.contains(OperatorId(1), &probe), "a committed batch must be visible to the pool too");
}

#[test]
fn setting_the_checkpoint_threshold_applies_the_wal_autocheckpoint_pragma() {
	fn threshold(store: &OperatorStore) -> u32 {
		let guard = store.inner.conn.lock();
		let conn = guard.as_ref().expect("the fixture keeps the write connection open");
		conn.pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
			.expect("wal_autocheckpoint must be readable back")
	}

	let (config, _guard) = SqliteConfig::test();
	let store = OperatorStore::new(config);

	store.set_checkpoint_threshold(7331);
	assert_eq!(threshold(&store), 7331, "the pragma must carry the requested frame count, not sqlite's default");

	store.set_checkpoint_threshold(11);
	assert_eq!(threshold(&store), 11, "a later call must overwrite the earlier threshold rather than be ignored");
}
