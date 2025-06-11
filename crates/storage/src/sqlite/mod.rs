// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::Stored;
use crate::storage::{
    Apply, Contains, Get, GetHooks, Scan, ScanRange, ScanRangeRev, ScanRev, Storage,
};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::delta::Delta;
use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::{OptionalExtension, params};
use std::ops::{Bound, Deref};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Sqlite(Arc<SqliteInner>);

pub struct SqliteInner {
    pool: Arc<Pool<SqliteConnectionManager>>,
    hooks: Hooks,
}

impl Deref for Sqlite {
    type Target = SqliteInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Sqlite {
    pub fn new(path: &Path) -> Self {
        let db_path = if path.is_dir() { path.join("reify.db") } else { path.to_path_buf() };

        let manager = SqliteConnectionManager::file(db_path).with_flags(
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        );

        let pool = Pool::builder().max_size(4).build(manager).unwrap();
        {
            let conn = pool.get().unwrap();
            conn.pragma_update(None, "journal_mode", &"WAL").unwrap();
            conn.pragma_update(None, "synchronous", &"NORMAL").unwrap();
            conn.pragma_update(None, "temp_store", &"MEMORY").unwrap();

            conn.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS kv (
                     key     BLOB NOT NULL,
                     version INTEGER NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key, version)
                 );
                 COMMIT;",
            )
            .unwrap();
        }

        Self(Arc::new(SqliteInner { pool: Arc::new(pool), hooks: Default::default() }))
    }

    fn get_conn(&self) -> PooledConnection<SqliteConnectionManager> {
        self.pool.get().unwrap()
    }
}

impl Apply for Sqlite {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) {
        let mut conn = self.get_conn();
        let tx = conn.transaction().unwrap();

        for delta in delta {
            match delta {
                Delta::Set { key, row: bytes } => {
                    let version = 1; // FIXME remove this - transaction version needs to be persisted
                    tx.execute(
                        "INSERT OR REPLACE INTO kv (key, version, value) VALUES (?1, ?2, ?3)",
                        params![key.to_vec(), version, bytes.to_vec()],
                    )
                    .unwrap();
                }
                Delta::Remove { key } => {
                    let version = 1; // FIXME remove this - transaction version needs to be persisted
                    tx.execute(
                        "DELETE FROM kv WHERE key = ?1 AND version = ?2",
                        params![key.to_vec(), version],
                    )
                    .unwrap();
                }
            }
        }

        tx.commit().unwrap();
    }
}

impl Get for Sqlite {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Stored> {
        let version = 1; // FIXME remove this - transaction version needs to be persisted

        let conn = self.get_conn();
        conn.query_row(
			"SELECT key, value, version FROM kv WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
			params![key.to_vec(), version],
			|row| {
				Ok(Stored {
					key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
					row: EncodedRow(AsyncCowVec::new(row.get::<_, Vec<u8>>(1)?)),
					version: row.get(2)?,
				})
			},
		)
			.optional()
			.unwrap()
    }
}

impl Contains for Sqlite {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool {
        // FIXME this can be done better than this
        self.get(key, version).is_some()
    }
}

impl Scan for Sqlite {
    type ScanIter<'a> = Box<dyn Iterator<Item = Stored> + 'a>;

    fn scan(&self, version: Version) -> Self::ScanIter<'_> {
        let version = 1; // FIXME remove this - transaction version needs to be persisted

        let conn = self.get_conn();
        let mut stmt = conn
            .prepare("SELECT key, value, version FROM kv WHERE version <= ? ORDER BY key ASC")
            .unwrap();

        let rows = stmt
            .query_map(params![version], |row| {
                Ok(Stored {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(AsyncCowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}

impl ScanRev for Sqlite {
    type ScanIterRev<'a> = Box<dyn Iterator<Item = Stored> + 'a>;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_> {
        let version = 1; // FIXME remove this - transaction version needs to be persisted

        let conn = self.get_conn();
        let mut stmt = conn
            .prepare("SELECT key, value, version FROM kv WHERE version <= ? ORDER BY key DESC")
            .unwrap();

        let rows = stmt
            .query_map(params![version], |row| {
                Ok(Stored {
                    key: EncodedKey(AsyncCowVec::new(row.get(0)?)),
                    row: EncodedRow(AsyncCowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}

impl ScanRange for Sqlite {
    type ScanRangeIter<'a> = Box<dyn Iterator<Item = Stored> + 'a>;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        let version = 1; // FIXME remove this - transaction version needs to be persisted

        let conn = self.get_conn();
        let mut stmt = conn
			.prepare("SELECT key, value, version FROM kv WHERE key >= ?1 AND key <= ?2 AND version <= ?3 ORDER BY key ASC")
			.unwrap();

        let start_bytes = bound_to_bytes(&range.start);
        let end_bytes = bound_to_bytes(&range.end);

        let rows = stmt
            // .query_map(params![], |row| {
            .query_map(params![start_bytes, end_bytes, version], |row| {
                Ok(Stored {
                    key: EncodedKey(AsyncCowVec::new(row.get(0)?)),
                    row: EncodedRow(AsyncCowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}

impl ScanRangeRev for Sqlite {
    type ScanRangeIterRev<'a> = Box<dyn Iterator<Item = Stored> + 'a>;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Self::ScanRangeIterRev<'_> {
        let version = 1; // FIXME remove this - transaction version needs to be persisted

        let conn = self.get_conn();
        let mut stmt = conn
			.prepare("SELECT key, value, version FROM kv WHERE key >= ?1 AND key <= ?2 AND version <= ?3 ORDER BY key DESC")
			.unwrap();

        let start_bytes = bound_to_bytes(&range.start);
        let end_bytes = bound_to_bytes(&range.end);

        let rows = stmt
            .query_map(params![start_bytes, end_bytes, version], |row| {
                Ok(Stored {
                    key: EncodedKey(AsyncCowVec::new(row.get(0)?)),
                    row: EncodedRow(AsyncCowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}

impl GetHooks for Sqlite {
    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }
}

impl Storage for Sqlite {}

fn bound_to_bytes(bound: &Bound<EncodedKey>) -> Vec<u8> {
    match bound {
        Bound::Included(v) | Bound::Excluded(v) => v.to_vec(),
        Bound::Unbounded => Vec::new(), // or handle it differently if needed
    }
}
