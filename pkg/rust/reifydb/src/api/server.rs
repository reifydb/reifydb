// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "sub_server")]

use reifydb_storage::sqlite::SqliteConfig;

use crate::{
	MemoryOptimisticTransaction, MemorySerializableTransaction,
	ServerBuilder, SqliteOptimisticTransaction,
	SqliteSerializableTransaction, memory, optimistic, serializable,
	sqlite,
};

pub fn memory_optimistic() -> ServerBuilder<MemoryOptimisticTransaction> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn memory_serializable() -> ServerBuilder<MemorySerializableTransaction> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> ServerBuilder<SqliteOptimisticTransaction> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> ServerBuilder<SqliteSerializableTransaction> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	ServerBuilder::new(versioned, unversioned, cdc, eventbus)
}
