// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured database types for common use cases
//!
//! These type aliases provide non-generic database types that are ready to use
//! without having to specify the transaction types.

use crate::Database;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::Sqlite;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb_transaction::svl::SingleVersionLock;

pub type UnversionedMemory = SingleVersionLock<Memory>;
pub type UnversionedSqlite = SingleVersionLock<Sqlite>;

/// In-memory database with serializable isolation
pub type MemoryDatabaseSerializable =
    Database<Serializable<Memory, UnversionedMemory>, UnversionedMemory>;

/// In-memory database with optimistic concurrency control
pub type MemoryDatabaseOptimistic =
    Database<Optimistic<Memory, UnversionedMemory>, UnversionedMemory>;

/// SQLite-backed database with serializable isolations
pub type SqliteDatabaseSerializable =
    Database<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite>;

/// SQLite-backed database with optimistic concurrency control
pub type SqliteDatabaseOptimistic =
    Database<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite>;
