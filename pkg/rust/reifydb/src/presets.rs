// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pre-configured database types for common use cases
//!
//! These type aliases provide non-generic database types that are ready to use
//! without having to specify the transaction types.

use reifydb_core::interface::{StandardCdcTransaction, StandardTransaction};
use reifydb_storage::{memory::Memory, sqlite::Sqlite};
use reifydb_transaction::{
	mvcc::transaction::{
		optimistic::Optimistic, serializable::Serializable,
	},
	svl::SingleVersionLock,
};

use crate::Database;

pub type UnversionedMemory = SingleVersionLock<Memory>;
pub type UnversionedSqlite = SingleVersionLock<Sqlite>;

/// CDC transaction type for Memory storage
pub type MemoryCdc = StandardCdcTransaction<Memory>;

/// CDC transaction type for SQLite storage
pub type SqliteCdc = StandardCdcTransaction<Sqlite>;

/// In-memory database with serializable isolation
pub type MemoryDatabaseSerializable = Database<
	StandardTransaction<
		Serializable<Memory, UnversionedMemory>,
		UnversionedMemory,
		MemoryCdc,
	>,
>;

/// In-memory database with optimistic concurrency control
pub type MemoryDatabaseOptimistic = Database<
	StandardTransaction<
		Optimistic<Memory, UnversionedMemory>,
		UnversionedMemory,
		MemoryCdc,
	>,
>;

/// SQLite-backed database with serializable isolations
pub type SqliteDatabaseSerializable = Database<
	StandardTransaction<
		Serializable<Sqlite, UnversionedSqlite>,
		UnversionedSqlite,
		SqliteCdc,
	>,
>;

/// SQLite-backed database with optimistic concurrency control
pub type SqliteDatabaseOptimistic = Database<
	StandardTransaction<
		Optimistic<Sqlite, UnversionedSqlite>,
		UnversionedSqlite,
		SqliteCdc,
	>,
>;
