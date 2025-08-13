// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Server database creation functions with network configuration

#![cfg(any(feature = "sub_grpc", feature = "sub_ws"))]

use reifydb_core::interface::StandardTransaction;
use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_transaction::mvcc::transaction::{
	optimistic::Optimistic, serializable::Serializable,
};

use crate::{
	MemoryCdc, ServerBuilder, SqliteCdc, UnversionedMemory,
	UnversionedSqlite, memory, optimistic, serializable, sqlite,
};

/// Create a server with in-memory storage and optimistic concurrency control
pub fn memory_optimistic() -> ServerBuilder<
	StandardTransaction<
		Optimistic<Memory, UnversionedMemory>,
		UnversionedMemory,
		MemoryCdc,
	>,
> {
	let (storage, unversioned, cdc, hooks) = memory();
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, hooks)
}

/// Create a server with in-memory storage and serializable isolation
pub fn memory_serializable() -> ServerBuilder<
	StandardTransaction<
		Serializable<Memory, UnversionedMemory>,
		UnversionedMemory,
		MemoryCdc,
	>,
> {
	let (storage, unversioned, cdc, hooks) = memory();
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, hooks)
}

/// Create a server with SQLite storage and optimistic concurrency control
pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> ServerBuilder<
	StandardTransaction<
		Optimistic<Sqlite, UnversionedSqlite>,
		UnversionedSqlite,
		SqliteCdc,
	>,
> {
	let (storage, unversioned, cdc, hooks) = sqlite(config);
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, hooks)
}

/// Create a server with SQLite storage and serializable isolation
pub fn sqlite_serializable(
	config: SqliteConfig,
) -> ServerBuilder<
	StandardTransaction<
		Serializable<Sqlite, UnversionedSqlite>,
		UnversionedSqlite,
		SqliteCdc,
	>,
> {
	let (storage, unversioned, cdc, hooks) = sqlite(config);
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, hooks)
}
