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
use reifydb_value::value::row_number::RowNumber;

use crate::tier::{persistent::sqlite::SqliteOperatorStorage as OperatorStore, resident::batch::FlushBatch};

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
