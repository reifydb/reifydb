// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_transaction::mvcc::transaction::{optimistic::Optimistic, serializable::Serializable};

use crate::{
	memory, optimistic, serializable, sqlite, EmbeddedBuilder, MemoryCdc, SqliteCdc, UnversionedMemory,
	UnversionedSqlite,
};

pub fn memory_optimistic() -> EmbeddedBuilder<Optimistic<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = optimistic((storage.clone(), unversioned.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn memory_serializable() -> EmbeddedBuilder<Serializable<Memory, UnversionedMemory>, UnversionedMemory, MemoryCdc> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = serializable((storage.clone(), unversioned.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> EmbeddedBuilder<Optimistic<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = optimistic((storage.clone(), unversioned.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> EmbeddedBuilder<Serializable<Sqlite, UnversionedSqlite>, UnversionedSqlite, SqliteCdc> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = serializable((storage.clone(), unversioned.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}
