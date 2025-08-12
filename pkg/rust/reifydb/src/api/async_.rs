// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Asynchronous database creation functions

#![cfg(feature = "async")]

use crate::{
    Database, AsyncBuilder,
    MemoryDatabaseOptimistic, MemoryDatabaseSerializable,
    SqliteDatabaseOptimistic, SqliteDatabaseSerializable,
    memory, optimistic, serializable, sqlite,
};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_storage::sqlite::SqliteConfig;

/// Create an async in-memory database with optimistic concurrency control (default)
pub fn memory_optimistic() -> MemoryDatabaseOptimistic {
    let (versioned, unversioned, hooks) = optimistic(memory());
    AsyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create an async in-memory database with serializable isolation
pub fn memory_serializable() -> MemoryDatabaseSerializable {
    let (versioned, unversioned, hooks) = serializable(memory());
    AsyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create an async SQLite-backed database with optimistic concurrency control
pub fn sqlite_optimistic(config: SqliteConfig) -> SqliteDatabaseOptimistic {
    let (versioned, unversioned, hooks) = optimistic(sqlite(config));
    AsyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create an async SQLite-backed database with serializable isolation
pub fn sqlite_serializable(config: SqliteConfig) -> SqliteDatabaseSerializable {
    let (versioned, unversioned, hooks) = serializable(sqlite(config));
    AsyncBuilder::new(versioned, unversioned, hooks).build()
}

/// Create a custom async database with user-provided transaction implementations
pub fn custom<VT, UT>(versioned: VT, unversioned: UT) -> Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    AsyncBuilder::new(versioned, unversioned, Hooks::new()).build()
}

/// Create a custom async database with user-provided transaction implementations and hooks
pub fn custom_with_hooks<VT, UT>(
    versioned: VT,
    unversioned: UT,
    hooks: Hooks
) -> Database<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    AsyncBuilder::new(versioned, unversioned, hooks).build()
}