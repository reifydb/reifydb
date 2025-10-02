// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured post types for common use cases
//!
//! These type aliases provide non-generic post types that are ready to use
//! without having to specify the transaction types.

use reifydb_engine::{EngineTransaction, StandardCdcTransaction};
use reifydb_store_transaction::backend::{memory::Memory, sqlite::Sqlite};
use reifydb_transaction::{
	mvcc::transaction::{optimistic::Optimistic, serializable::Serializable},
	svl::SingleVersionLock,
};

use crate::Database;

pub type SingleVersionMemory = SingleVersionLock<Memory>;
pub type SingleVersionSqlite = SingleVersionLock<Sqlite>;

/// CDC transaction type for Memory storage
pub type MemoryCdc = StandardCdcTransaction<Memory>;

/// CDC transaction type for SQLite storage
pub type SqliteCdc = StandardCdcTransaction<Sqlite>;

/// In-memory with serializable isolation
pub type MemorySerializableTransaction =
	EngineTransaction<Serializable<Memory, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>;

/// In-memory post with serializable isolation
pub type MemoryDatabaseSerializable =
	Database<Serializable<Memory, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>;

/// In-memory with optimistic concurrency control
pub type MemoryOptimisticTransaction =
	EngineTransaction<Optimistic<Memory, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>;

/// In-memory post with optimistic concurrency control
pub type MemoryDatabaseOptimistic = Database<Optimistic<Memory, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>;

/// SQLite with serializable isolation
pub type SqliteSerializableTransaction =
	EngineTransaction<Serializable<Sqlite, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>;

/// SQLite-backed with serializable isolations
pub type SqliteDatabaseSerializable =
	Database<Serializable<Sqlite, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>;

/// SQLite with optimistic concurrency control
pub type SqliteOptimisticTransaction =
	EngineTransaction<Optimistic<Sqlite, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>;

/// SQLite-backed post with optimistic concurrency control
pub type SqliteDatabaseOptimistic = Database<Optimistic<Sqlite, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>;
