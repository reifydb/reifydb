// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod unversioned;
mod versioned;

use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{
    UnversionedRemove, UnversionedSet, UnversionedStorage, VersionedStorage,
};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Sqlite(Arc<SqliteInner>);

pub struct SqliteInner {
    pool: Arc<Pool<SqliteConnectionManager>>,
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
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
            conn.pragma_update(None, "temp_store", "MEMORY").unwrap();

            conn.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS versioned (
                     key     BLOB NOT NULL,
                     version INTEGER NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key, version)
                 );

                 CREATE TABLE IF NOT EXISTS unversioned (
                     key     BLOB NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key)
                 );
                 COMMIT;",
            )
            .unwrap();
        }

        Self(Arc::new(SqliteInner { pool: Arc::new(pool) }))
    }

    fn get_conn(&self) -> PooledConnection<SqliteConnectionManager> {
        self.pool.get().unwrap()
    }
}

impl VersionedStorage for Sqlite {}
impl UnversionedStorage for Sqlite {}
impl UnversionedSet for Sqlite {}
impl UnversionedRemove for Sqlite {}
