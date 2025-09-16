// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured post types for common use cases
//!
//! These type aliases provide non-generic post types that are ready to use
//! without having to specify the transaction types.

use reifydb_engine::{EngineTransaction, StandardCdcTransaction};
use reifydb_storage::{memory::Memory, sqlite::Sqlite};
use reifydb_transaction::{
	mvcc::transaction::{optimistic::Optimistic, serializable::Serializable},
	svl::SingleVersionLock,
};

use crate::Database;

pub type UnversionedMemory = SingleVersionLock<Memory>;
pub type UnversionedSqlite = SingleVersionLock<Sqlite>;

/// CDC transaction type for Memory storage
pub type MemoryCdc = StandardCdcTransaction<Memory>;

/// CDC transaction type for SQLite storage
pub type SqliteCdc = StandardCdcTransaction<Sqlite>;

/// In-memory with serializable isolation
pub type MemorySerializableTransaction =
	EngineTransaction<Serializable<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc>;

/// In-memory post with serializable isolation
pub type MemoryDatabaseSerializable = Database<Serializable<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc>;

/// In-memory with optimistic concurrency control
pub type MemoryOptimisticTransaction =
	EngineTransaction<Optimistic<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc>;

/// In-memory post with optimistic concurrency control
pub type MemoryDatabaseOptimistic = Database<Optimistic<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc>;

/// SQLite with serializable isolation
pub type SqliteSerializableTransaction =
	EngineTransaction<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc>;

/// SQLite-backed with serializable isolations
pub type SqliteDatabaseSerializable = Database<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc>;

/// SQLite with optimistic concurrency control
pub type SqliteOptimisticTransaction =
	EngineTransaction<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc>;

/// SQLite-backed post with optimistic concurrency control
pub type SqliteDatabaseOptimistic = Database<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc>;
