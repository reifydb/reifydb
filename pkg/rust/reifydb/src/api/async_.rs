// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Asynchronous database creation functions

#![cfg(feature = "async")]

use crate::{
    memory, optimistic, serializable, sqlite,
    AsyncBuilder, Database, MemoryDatabaseOptimistic, MemoryDatabaseSerializable, SqliteDatabaseOptimistic, SqliteDatabaseSerializable,
};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    CdcTransaction, StandardTransaction, UnversionedTransaction,
    VersionedTransaction,
};
use reifydb_storage::sqlite::SqliteConfig;

/// Create an async in-memory database with optimistic concurrency control (default)
pub fn memory_optimistic() -> MemoryDatabaseOptimistic {
    let (storage, unversioned, cdc, hooks) = memory();
    let (versioned, _, _, _) =
        optimistic((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    AsyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create an async in-memory database with serializable isolation
pub fn memory_serializable() -> MemoryDatabaseSerializable {
    let (storage, unversioned, cdc, hooks) = memory();
    let (versioned, _, _, _) =
        serializable((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    AsyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create an async SQLite-backed database with optimistic concurrency control
pub fn sqlite_optimistic(config: SqliteConfig) -> SqliteDatabaseOptimistic {
    let (storage, unversioned, cdc, hooks) = sqlite(config);
    let (versioned, _, _, _) =
        optimistic((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    AsyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create an async SQLite-backed database with serializable isolation
pub fn sqlite_serializable(config: SqliteConfig) -> SqliteDatabaseSerializable {
    let (storage, unversioned, cdc, hooks) = sqlite(config);
    let (versioned, _, _, _) =
        serializable((storage.clone(), unversioned.clone(), cdc.clone(), hooks.clone()));
    AsyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}

/// Create a custom async database with user-provided transaction implementations
pub fn custom<VT, UT, C>(
    versioned: VT,
    unversioned: UT,
    cdc: C,
) -> Database<StandardTransaction<VT, UT, C>>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction + Clone + 'static,
{
    AsyncBuilder::new(versioned, unversioned, cdc, Hooks::new()).build()
}

/// Create a custom async database with user-provided transaction implementations and hooks
pub fn custom_with_hooks<VT, UT, C>(
    versioned: VT,
    unversioned: UT,
    cdc: C,
    hooks: Hooks,
) -> Database<StandardTransaction<VT, UT, C>>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction + Clone + 'static,
{
    AsyncBuilder::new(versioned, unversioned, cdc, hooks).build()
}
