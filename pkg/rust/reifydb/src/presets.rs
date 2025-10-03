// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured post types for common use cases
//!
//! These type aliases provide non-generic post types that are ready to use
//! without having to specify the transaction types.

use reifydb_engine::{EngineTransaction, StandardCdcTransaction};
use reifydb_store_transaction::StandardTransactionStore;
use reifydb_transaction::{
	mvcc::transaction::{optimistic::OptimisticTransaction, serializable::SerializableTransaction},
	svl::SingleVersionLock,
};

use crate::Database;

pub type SingleVersionMemory = SingleVersionLock<StandardTransactionStore>;
pub type SingleVersionSqlite = SingleVersionLock<StandardTransactionStore>;

/// CDC transaction type for Memory storage
pub type MemoryCdc = StandardCdcTransaction<StandardTransactionStore>;

/// CDC transaction type for SQLite storage
pub type SqliteCdc = StandardCdcTransaction<StandardTransactionStore>;

/// In-memory with serializable isolation
pub type MemorySerializableTransaction = EngineTransaction<
	SerializableTransaction<StandardTransactionStore, SingleVersionMemory>,
	SingleVersionMemory,
	MemoryCdc,
>;

/// In-memory post with serializable isolation
pub type MemoryDatabaseSerializable = Database<
	SerializableTransaction<StandardTransactionStore, SingleVersionMemory>,
	SingleVersionMemory,
	MemoryCdc,
>;

/// In-memory with optimistic concurrency control
pub type MemoryOptimisticTransaction = EngineTransaction<
	OptimisticTransaction<StandardTransactionStore, SingleVersionMemory>,
	SingleVersionMemory,
	MemoryCdc,
>;

/// In-memory post with optimistic concurrency control
pub type MemoryDatabaseOptimistic =
	Database<OptimisticTransaction<StandardTransactionStore, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>;

/// SQLite with serializable isolation
pub type SqliteSerializableTransaction = EngineTransaction<
	SerializableTransaction<StandardTransactionStore, SingleVersionSqlite>,
	SingleVersionSqlite,
	SqliteCdc,
>;

/// SQLite-backed with serializable isolations
pub type SqliteDatabaseSerializable = Database<
	SerializableTransaction<StandardTransactionStore, SingleVersionSqlite>,
	SingleVersionSqlite,
	SqliteCdc,
>;

/// SQLite with optimistic concurrency control
pub type SqliteOptimisticTransaction = EngineTransaction<
	OptimisticTransaction<StandardTransactionStore, SingleVersionSqlite>,
	SingleVersionSqlite,
	SqliteCdc,
>;

/// SQLite-backed post with optimistic concurrency control
pub type SqliteDatabaseOptimistic =
	Database<OptimisticTransaction<StandardTransactionStore, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>;
