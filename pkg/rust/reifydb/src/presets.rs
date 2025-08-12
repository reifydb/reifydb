// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured database types for common use cases
//!
//! These type aliases provide non-generic database types that are ready to use
//! without having to specify the transaction types.

use crate::Database;
use reifydb_core::interface::StandardTransaction;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::Sqlite;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb_transaction::svl::SingleVersionLock;

pub type UnversionedMemory = SingleVersionLock<Memory>;
pub type UnversionedSqlite = SingleVersionLock<Sqlite>;

/// In-memory database with serializable isolation
pub type MemoryDatabaseSerializable =
    Database<StandardTransaction<Serializable<Memory, UnversionedMemory>, UnversionedMemory>>;

/// In-memory database with optimistic concurrency control
pub type MemoryDatabaseOptimistic =
    Database<StandardTransaction<Optimistic<Memory, UnversionedMemory>, UnversionedMemory>>;

/// SQLite-backed database with serializable isolations
pub type SqliteDatabaseSerializable =
    Database<StandardTransaction<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite>>;

/// SQLite-backed database with optimistic concurrency control
pub type SqliteDatabaseOptimistic =
    Database<StandardTransaction<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite>>;
