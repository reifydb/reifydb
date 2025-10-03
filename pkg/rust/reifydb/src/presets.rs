// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured post types for common use cases
//!
//! These type aliases provide non-generic post types that are ready to use
//! without having to specify the transaction types.

use reifydb_engine::{EngineTransaction, TransactionCdc};
use reifydb_transaction::{
	multi::transaction::{optimistic::TransactionOptimistic, serializable::TransactionSerializable},
	single::TransactionSvl,
};

use crate::Database;

pub type SingleVersionMemory = TransactionSvl;
pub type SingleVersionSqlite = TransactionSvl;

/// CDC transaction type for Memory storage
pub type MemoryCdc = TransactionCdc;

/// CDC transaction type for SQLite storage
pub type SqliteCdc = TransactionCdc;

/// In-memory with serializable isolation
pub type MemorySerializableTransaction = EngineTransaction<TransactionSerializable, SingleVersionMemory, MemoryCdc>;

/// In-memory post with serializable isolation
pub type MemoryDatabaseSerializable = Database;

/// In-memory with optimistic concurrency control
pub type MemoryOptimisticTransaction = EngineTransaction<TransactionOptimistic, SingleVersionMemory, MemoryCdc>;

/// In-memory post with optimistic concurrency control
pub type MemoryDatabaseOptimistic = Database;

/// SQLite with serializable isolation
pub type SqliteSerializableTransaction = EngineTransaction<TransactionSerializable, SingleVersionSqlite, SqliteCdc>;

/// SQLite-backed with serializable isolations
pub type SqliteDatabaseSerializable = Database;

/// SQLite with optimistic concurrency control
pub type SqliteOptimisticTransaction = EngineTransaction<TransactionOptimistic, SingleVersionSqlite, SqliteCdc>;

/// SQLite-backed post with optimistic concurrency control
pub type SqliteDatabaseOptimistic = Database;
