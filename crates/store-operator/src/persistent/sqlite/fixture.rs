// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			state::{GroupStateKey, OperatorStateKey},
			traits::Keyspace,
		},
		typed::range::KeyRange,
	},
	state::typed::SuffixBytes,
};
use reifydb_store::sqlite::pool::ReadPool;
use rusqlite::Connection;

use crate::{
	bound::parts,
	persistent::sqlite::SqlitePersistent,
	types::{FlushBatch, OperatorBatch, StagedWrite},
};

const SCAN_LIMIT: u64 = 1024;

pub(super) fn open() -> SqlitePersistent {
	let conn = Connection::open_in_memory().expect("an in memory sqlite database must open");
	SqlitePersistent::with_connections(conn, ReadPool::new(Vec::new()))
}

pub(super) fn encode<K: Keyspace>(key: &K::GroupedKey) -> GroupStateKey {
	let (group, suffix) = K::split(key);
	OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes())
}

fn decode<K: Keyspace>(key: &EncodedKey) -> K::GroupedKey {
	let (group, _, suffix) = parts(key);
	K::join(
		group,
		<K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)
			.expect("a stored suffix must decode as its own layout"),
	)
}

fn flush(store: &SqlitePersistent, operator: OperatorId, key: GroupStateKey, write: StagedWrite) {
	let mut batch = FlushBatch::default();
	batch.writes.push((operator, key, write));
	store.flush_batch(&batch);
}

pub(super) fn set_one<K: Keyspace>(store: &SqlitePersistent, operator: OperatorId, key: &K::GroupedKey, bytes: &[u8]) {
	flush(store, operator, encode::<K>(key), StagedWrite::Set(EncodedPodRow::new(bytes)));
}

pub(super) fn remove_one<K: Keyspace>(store: &SqlitePersistent, operator: OperatorId, key: &K::GroupedKey) {
	flush(store, operator, encode::<K>(key), StagedWrite::Remove);
}

pub(super) fn get<K: Keyspace>(store: &SqlitePersistent, operator: OperatorId, key: &K::GroupedKey) -> Option<Vec<u8>> {
	store.get(operator, encode::<K>(key).as_encoded()).map(|row| row.as_slice().to_vec())
}

fn rows<K: Keyspace>(batch: OperatorBatch) -> Vec<(K::GroupedKey, Vec<u8>)> {
	batch.items
		.into_iter()
		.filter(|(key, _)| parts(key.as_encoded()).1 == K::ID)
		.map(|(key, row)| (decode::<K>(key.as_encoded()), row.as_slice().to_vec()))
		.collect()
}

fn encoded<K: Keyspace>(range: &KeyRange<K::GroupedKey>) -> EncodedKeyRange {
	let edge = |bound: Bound<&K::GroupedKey>| match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => Bound::Included(encode::<K>(key).into_encoded()),
		Bound::Excluded(key) => Bound::Excluded(encode::<K>(key).into_encoded()),
	};
	EncodedKeyRange::new(edge(range.start.as_ref()), edge(range.end.as_ref()))
}

pub(super) fn scan<K: Keyspace>(store: &SqlitePersistent, operator: OperatorId) -> Vec<(K::GroupedKey, Vec<u8>)> {
	rows::<K>(store.range_batch(operator, EncodedKeyRange::all(), SCAN_LIMIT))
}

pub(super) fn range<K: Keyspace>(
	store: &SqlitePersistent,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	rows::<K>(store.range_batch(operator, encoded::<K>(range), limit))
}

pub(super) fn last<K: Keyspace>(
	store: &SqlitePersistent,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	rows::<K>(store.last_batch(operator, encoded::<K>(range), limit))
}

pub(super) fn keys_after<K: Keyspace>(
	store: &SqlitePersistent,
	operator: OperatorId,
	after: Option<&K::GroupedKey>,
	limit: u64,
) -> Vec<K::GroupedKey> {
	let cursor = after.map(|key| encode::<K>(key).into_encoded());
	store.state_keys_after(operator, K::ID, cursor.as_ref(), limit).iter().map(decode::<K>).collect()
}

pub(super) fn census<K: Keyspace>(store: &SqlitePersistent) -> Vec<(OperatorId, u64, u64)> {
	store.census()
		.into_iter()
		.filter(|entry| entry.keyspace == K::ID)
		.map(|entry| (entry.operator, entry.keys, entry.value_bytes.as_bytes()))
		.collect()
}

pub(super) fn with_conn<T>(store: &SqlitePersistent, check: impl FnOnce(&Connection) -> T) -> T {
	let guard = store.inner.conn.lock();
	check(guard.as_ref().expect("the store must be open"))
}
