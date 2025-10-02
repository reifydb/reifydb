// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "sub_server")]

use reifydb_store_transaction::backend::{
	memory::MemoryBackend,
	sqlite::{SqliteBackend, SqliteConfig},
};
use reifydb_transaction::mvcc::transaction::{optimistic::Optimistic, serializable::Serializable};

use crate::{
	MemoryCdc, ServerBuilder, SingleVersionMemory, SingleVersionSqlite, SqliteCdc, memory, optimistic,
	serializable, sqlite,
};

pub fn memory_optimistic()
-> ServerBuilder<Optimistic<MemoryBackend, SingleVersionMemory>, SingleVersionMemory, MemoryCdc> {
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn memory_serializable()
-> ServerBuilder<Serializable<MemoryBackend, SingleVersionMemory>, SingleVersionMemory, MemoryCdc> {
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> ServerBuilder<Optimistic<SqliteBackend, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc> {
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> ServerBuilder<Serializable<SqliteBackend, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc> {
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}
