// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{keyspace::KEYSPACES, state::KeyspaceId},
};
use reifydb_runtime::sync::rwlock::RwLock;
use rusqlite::Connection;

use crate::{persistent::sqlite::sql::TABLE_NAMES_SQL, store::occupancy::bit};

#[derive(Clone, Copy, Default)]
pub(super) struct TableMask(u64);

impl TableMask {
	pub(super) fn holds(self, keyspace: KeyspaceId) -> bool {
		self.0 & holding(keyspace) != 0
	}

	pub(super) fn held(self) -> Vec<KeyspaceId> {
		let mut ids: Vec<KeyspaceId> =
			KEYSPACES.iter().map(|spec| spec.id).filter(|id| self.holds(*id)).collect();
		ids.sort_unstable();
		ids
	}

	pub(super) fn bits(self) -> u64 {
		self.0
	}

	fn insert(&mut self, keyspace: KeyspaceId) {
		self.0 |= holding(keyspace);
	}
}

pub(super) struct TableRegistry {
	masks: RwLock<HashMap<OperatorId, TableMask>>,
}

impl TableRegistry {
	pub(super) fn load(conn: &Connection) -> Self {
		let mut stmt = conn.prepare(TABLE_NAMES_SQL).expect("operator state tables could not be listed");
		let names: Vec<String> = stmt
			.query_map([], |row| row.get(0))
			.expect("operator state tables could not be listed")
			.collect::<Result<_, _>>()
			.expect("operator state table name could not be read");
		let mut masks: HashMap<OperatorId, TableMask> = HashMap::new();
		for name in names {
			let Some(rest) = name.strip_prefix("operator_") else {
				continue;
			};
			let (operator, keyspace) = parse(rest).unwrap_or_else(|| {
				panic!("operator state table {name} does not name a keyspace and an operator")
			});
			masks.entry(operator).or_default().insert(keyspace);
		}
		Self {
			masks: RwLock::new(masks),
		}
	}

	pub(super) fn mask(&self, operator: OperatorId) -> TableMask {
		self.masks.read().get(&operator).copied().unwrap_or_default()
	}

	pub(super) fn snapshot(&self) -> HashMap<OperatorId, TableMask> {
		self.masks.read().clone()
	}

	pub(super) fn is_empty(&self) -> bool {
		self.masks.read().is_empty()
	}

	pub(super) fn add(&self, created: &[(OperatorId, KeyspaceId)]) {
		if created.is_empty() {
			return;
		}
		let mut masks = self.masks.write();
		for (operator, keyspace) in created {
			masks.entry(*operator).or_default().insert(*keyspace);
		}
	}
}

fn holding(keyspace: KeyspaceId) -> u64 {
	bit(keyspace).expect("every catalogue keyspace must have an occupancy bit")
}

fn parse(rest: &str) -> Option<(OperatorId, KeyspaceId)> {
	let (keyspace, digits) = rest.rsplit_once('_')?;
	let operator: u64 = digits.parse().ok()?;
	if operator.to_string() != digits {
		return None;
	}
	let spec = KEYSPACES.iter().find(|spec| spec.name.to_ascii_lowercase() == keyspace)?;
	Some((OperatorId(operator), spec.id))
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{
				keyspace::{
					expiry::{Expiry, ExpiryKey},
					join::{JoinLeft, JoinLeftKey},
				},
				state::GroupId,
				traits::Keyspace,
			},
			typed::direction::{Asc, Desc},
		},
	};
	use reifydb_runtime::shutdown::Shutdown;
	use reifydb_sqlite::SqliteConfig;
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use crate::persistent::sqlite::{
		SqlitePersistent,
		fixture::{encode, get, keys_after, open, remove_one, scan, set_one, with_conn},
	};

	const OWNER: OperatorId = OperatorId(1);
	const STRANGER: OperatorId = OperatorId(2);

	fn key() -> JoinLeftKey {
		JoinLeftKey {
			group: Desc(GroupId::hashed(Hash128(1))),
			row: Asc(RowNumber(1)),
		}
	}

	#[test]
	fn a_read_for_an_operator_without_a_table_answers_empty() {
		// a missing table must read as absent state; querying it would stop the store on "no such table"
		let store = open();
		set_one::<JoinLeft>(&store, OWNER, &key(), b"mine");
		assert_eq!(get::<JoinLeft>(&store, STRANGER, &key()), None);
		assert!(scan::<JoinLeft>(&store, STRANGER).is_empty());
		assert!(keys_after::<JoinLeft>(&store, STRANGER, None, 10).is_empty());
		assert!(store.get_many(STRANGER, &[encode::<JoinLeft>(&key()).into_encoded()]).is_empty());
		assert!(store.group_page(STRANGER, &[key().group.0], 10, u64::MAX).items.is_empty());
		assert!(store.range_batch(STRANGER, EncodedKeyRange::all(), 10).items.is_empty());
	}

	#[test]
	fn every_keyspace_holding_rows_is_reported_occupied() {
		// the occupancy mask skips keyspaces it is not told about, so a missed bit hides live rows from every
		// sweep
		let store = open();
		set_one::<JoinLeft>(&store, OWNER, &key(), b"left");
		set_one::<Expiry>(
			&store,
			OWNER,
			&ExpiryKey {
				threshold: Desc(7),
				owner: Desc(Hash128(9)),
			},
			b"expiry",
		);
		let occupied = store.occupied_keyspaces(OWNER);
		assert!(occupied.contains(&JoinLeft::ID) && occupied.contains(&Expiry::ID), "{occupied:?}");
	}

	#[test]
	fn an_operator_whose_rows_were_all_removed_is_not_reported_occupied() {
		// an emptied table must not count as occupied, or a dropped operator would still name its keyspaces
		let store = open();
		set_one::<JoinLeft>(&store, OWNER, &key(), b"left");
		remove_one::<JoinLeft>(&store, OWNER, &key());
		assert!(store.occupied_keyspaces(OWNER).is_empty());
	}

	#[test]
	fn a_remove_only_flush_creates_no_table() {
		// a delete against a table that never existed has nothing to delete, so creating one only leaves an
		// empty table
		let store = open();
		remove_one::<JoinLeft>(&store, OWNER, &key());
		assert!(store.occupied_keyspaces(OWNER).is_empty());
		let tables: i64 = with_conn(&store, |conn| {
			conn.query_row(
				r#"SELECT COUNT(*) FROM "sqlite_master" WHERE "name" = 'operator_join_left_1'"#,
				[],
				|row| row.get(0),
			)
			.unwrap()
		});
		assert_eq!(tables, 0);
	}

	#[test]
	fn a_reopened_store_finds_the_tables_created_before_the_restart() {
		// a registry not rebuilt from disk would call every table missing after a restart and serve no state at
		// all
		let (config, _guard) = SqliteConfig::in_memory();
		let first = SqlitePersistent::new(config.clone());
		set_one::<JoinLeft>(&first, OWNER, &key(), b"kept");
		first.shutdown();
		let second = SqlitePersistent::new(config);
		assert_eq!(second.occupied_keyspaces(OWNER), vec![JoinLeft::ID]);
	}

	#[test]
	fn a_reopened_store_with_tables_reads_as_written() {
		// a store that reopened as unwritten would skip sqlite on every read and answer rows it holds as absent
		let (config, _guard) = SqliteConfig::in_memory();
		let first = SqlitePersistent::new(config.clone());
		set_one::<JoinLeft>(&first, OWNER, &key(), b"kept");
		first.shutdown();
		let second = SqlitePersistent::new(config);
		assert_eq!(get::<JoinLeft>(&second, OWNER, &key()).as_deref(), Some(b"kept".as_slice()));
	}

	#[test]
	#[should_panic(expected = "does not name a keyspace and an operator")]
	fn a_stray_operator_table_stops_the_open() {
		// a pre-split table left on disk holds state no registry entry can reach, so opening over it must fail
		// loud
		let (config, _guard) = SqliteConfig::in_memory();
		let first = SqlitePersistent::new(config.clone());
		with_conn(&first, |conn| {
			conn.execute_batch(r#"CREATE TABLE "operator_join_left" ("x" INTEGER)"#).unwrap()
		});
		first.shutdown();
		SqlitePersistent::new(config);
	}
}
