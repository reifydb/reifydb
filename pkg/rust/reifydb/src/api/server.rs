// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Server database creation functions with network configuration

#![cfg(any(feature = "sub_grpc", feature = "sub_ws"))]

use crate::{
    ServerBuilder, UnversionedMemory, UnversionedSqlite, memory, optimistic, serializable, sqlite,
};
use reifydb_core::interface::StandardTransaction;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::{Sqlite, SqliteConfig};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;

/// Create a server with in-memory storage and optimistic concurrency control
pub fn memory_optimistic() -> ServerBuilder<StandardTransaction<Optimistic<Memory, UnversionedMemory>, UnversionedMemory>>
{
    let (versioned, unversioned, hooks) = optimistic(memory());
    ServerBuilder::new(versioned, unversioned, hooks)
}

/// Create a server with in-memory storage and serializable isolation
pub fn memory_serializable()
-> ServerBuilder<StandardTransaction<Serializable<Memory, UnversionedMemory>, UnversionedMemory>> {
    let (versioned, unversioned, hooks) = serializable(memory());
    ServerBuilder::new(versioned, unversioned, hooks)
}

/// Create a server with SQLite storage and optimistic concurrency control
pub fn sqlite_optimistic(
    config: SqliteConfig,
) -> ServerBuilder<StandardTransaction<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite>> {
    let (versioned, unversioned, hooks) = optimistic(sqlite(config));
    ServerBuilder::new(versioned, unversioned, hooks)
}

/// Create a server with SQLite storage and serializable isolation
pub fn sqlite_serializable(
    config: SqliteConfig,
) -> ServerBuilder<StandardTransaction<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite>> {
    let (versioned, unversioned, hooks) = serializable(sqlite(config));
    ServerBuilder::new(versioned, unversioned, hooks)
}
