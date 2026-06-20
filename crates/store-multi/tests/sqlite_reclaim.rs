// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
};
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
};
use reifydb_store_multi::tier::{TierStorage, persistent::MultiPersistentTier};
use reifydb_value::util::cowvec::CowVec;

fn kind() -> EntryKind {
	EntryKind::Source(ShapeId::Table(TableId(7001)))
}

fn write(tier: &MultiPersistentTier, version: u64, key: &str, value: &[u8]) {
	let batch = HashMap::from([(
		kind(),
		vec![(EncodedKey::new(key.as_bytes().to_vec()), Some(CowVec::new(value.to_vec())))],
	)]);
	tier.set(CommitVersion(version), batch).unwrap();
}

fn freelist_count(config: &SqliteConfig) -> i64 {
	let db_path = resolve_db_path(config.path.clone(), "persistent.db");
	let conn = connect(&db_path, convert_flags(&config.flags)).expect("probe connection");
	conn.query_row("PRAGMA freelist_count", [], |row| row.get::<_, i64>(0)).expect("freelist_count")
}

// auto_vacuum=INCREMENTAL (set in pragma::apply) parks pages freed by deletes on the freelist instead of
// returning them to the OS; only an explicit incremental_vacuum returns them, which is exactly what
// MultiPersistentTier::reclaim() runs. This test deletes the bulk of a multi-page table, proves the freed
// pages accumulate on the freelist, then proves reclaim() drains it to zero. If reclaim() ever became a
// no-op (or stopped running incremental_vacuum) the persistent file would silently stop shrinking after
// large evictions - the post-reclaim freelist assertion is what catches that.
#[test]
fn reclaim_returns_free_pages_to_the_os() {
	let (config, _guard) = SqliteConfig::test();
	let config = config.read_pool_size(1);
	let tier = MultiPersistentTier::sqlite(config.clone());

	let value = vec![0xABu8; 256];
	for i in 0..2000u64 {
		write(&tier, i + 1, &format!("key-{i:06}"), &value);
	}

	tier.delete_below_version(kind(), CommitVersion(1900), None).unwrap();

	let before = freelist_count(&config);
	assert!(before > 0, "deletes must leave reclaimable free pages on the freelist; got {before}");

	tier.reclaim().unwrap();

	let after = freelist_count(&config);
	assert_eq!(after, 0, "reclaim() must return every free page to the OS; freelist went {before} -> {after}");
}
