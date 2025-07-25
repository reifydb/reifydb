// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod config;
mod unversioned;
mod versioned;

pub use config::*;

use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{
    UnversionedRemove, UnversionedSet, UnversionedStorage, VersionedStorage,
};
use std::ops::Deref;
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
    /// Create a new Sqlite storage with the given configuration
    pub fn new(config: SqliteConfig) -> Self {
        let db_path =
            if config.path.is_dir() { config.path.join("reify.db") } else { config.path.clone() };

        let manager = SqliteConnectionManager::file(db_path)
            .with_flags(Self::convert_open_flags(&config.flags));

        let pool = Pool::builder().max_size(config.max_pool_size).build(manager).unwrap();
        {
            let conn = pool.get().unwrap();
            conn.pragma_update(None, "journal_mode", config.journal_mode.as_str()).unwrap();
            conn.pragma_update(None, "synchronous", config.synchronous_mode.as_str()).unwrap();
            conn.pragma_update(None, "temp_store", config.temp_store.as_str()).unwrap();

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

    fn convert_open_flags(flags: &OpenFlags) -> rusqlite::OpenFlags {
        let mut rusqlite_flags = rusqlite::OpenFlags::empty();

        if flags.read_write {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
        }

        if flags.create {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        }

        if flags.full_mutex {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX;
        }

        if flags.no_mutex {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        }

        if flags.shared_cache {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_SHARED_CACHE;
        }

        if flags.private_cache {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
        }

        if flags.uri {
            rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_URI;
        }

        rusqlite_flags
    }

    fn get_conn(&self) -> PooledConnection<SqliteConnectionManager> {
        self.pool.get().unwrap()
    }
}

impl VersionedStorage for Sqlite {}
impl UnversionedStorage for Sqlite {}
impl UnversionedSet for Sqlite {}
impl UnversionedRemove for Sqlite {}
