// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Synchronous database creation functions
use crate::{
    memory, optimistic, serializable, sqlite,
    Database, MemoryDatabaseOptimistic, MemoryDatabaseSerializable, SqliteDatabaseOptimistic, SqliteDatabaseSerializable, SyncBuilder,
};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    CdcTransaction, StandardTransaction, UnversionedTransaction,
    VersionedTransaction,
};
use reifydb_storage::sqlite::SqliteConfig;

/// Create an in-memory database with optimistic concurrency control (default)
pub fn memory_optimistic() -> MemoryDatabaseOptimistic {
    let (storage, unversioned, cdc, hooks) = memory();
    let (versioned, _, _, _) =
        optimistic((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    SyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create an in-memory database with serializable isolation
pub fn memory_serializable() -> MemoryDatabaseSerializable {
    let (storage, unversioned, cdc, hooks) = memory();
    let (versioned, _, _, _) =
        serializable((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    SyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create a SQLite-backed database with optimistic concurrency control
pub fn sqlite_optimistic(config: SqliteConfig) -> SqliteDatabaseOptimistic {
    let (storage, unversioned, cdc, hooks) = sqlite(config);
    let (versioned, _, _, _) =
        optimistic((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    SyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create a SQLite-backed database with serializable isolation
pub fn sqlite_serializable(config: SqliteConfig) -> SqliteDatabaseSerializable {
    let (storage, unversioned, cdc, hooks) = sqlite(config);
    let (versioned, _, _, _) =
        serializable((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    SyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create a custom database with user-provided transaction implementations
pub fn custom<VT, UT, C>(
    versioned: VT,
    unversioned: UT,
    cdc: C,
) -> Database<StandardTransaction<VT, UT, C>>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    SyncBuilder::new(versioned, unversioned, cdc, Hooks::new()).build()
}
