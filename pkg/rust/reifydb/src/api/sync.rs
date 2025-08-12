// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Synchronous database creation functions

use crate::{
    Database, MemoryDatabaseOptimistic, MemoryDatabaseSerializable, SqliteDatabaseOptimistic,
    SqliteDatabaseSerializable, SyncBuilder, memory, optimistic, serializable, sqlite,
};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{StandardTransaction, UnversionedTransaction, VersionedTransaction};
use reifydb_storage::sqlite::SqliteConfig;

/// Create an in-memory database with optimistic concurrency control (default)
pub fn memory_optimistic() -> MemoryDatabaseOptimistic {
    let (versioned, unversioned, hooks) = optimistic(memory());
    SyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create an in-memory database with serializable isolation
pub fn memory_serializable() -> MemoryDatabaseSerializable {
    let (versioned, unversioned, hooks) = serializable(memory());
    SyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create a SQLite-backed database with optimistic concurrency control
pub fn sqlite_optimistic(config: SqliteConfig) -> SqliteDatabaseOptimistic {
    let (versioned, unversioned, hooks) = optimistic(sqlite(config));
    SyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create a SQLite-backed database with serializable isolation
pub fn sqlite_serializable(config: SqliteConfig) -> SqliteDatabaseSerializable {
    let (versioned, unversioned, hooks) = serializable(sqlite(config));
    SyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create a custom database with user-provided transaction implementations
pub fn custom<VT, UT>(versioned: VT, unversioned: UT) -> Database<StandardTransaction<VT, UT>>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    SyncBuilder::new(versioned, unversioned, Hooks::new()).build()
}
