// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "async")]

use reifydb_storage::sqlite::SqliteConfig;

use crate::{
	AsyncBuilder, MemoryOptimisticTransaction,
	MemorySerializableTransaction, SqliteOptimisticTransaction,
	SqliteSerializableTransaction, memory, optimistic, serializable,
	sqlite,
};

pub fn memory_optimistic() -> AsyncBuilder<MemoryOptimisticTransaction> {
	let (storage, unversioned, cdc, hooks) = memory();
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	AsyncBuilder::new(versioned, unversioned, cdc, hooks)
}

pub fn memory_serializable() -> AsyncBuilder<MemorySerializableTransaction> {
	let (storage, unversioned, cdc, hooks) = memory();
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	AsyncBuilder::new(versioned, unversioned, cdc, hooks)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> AsyncBuilder<SqliteOptimisticTransaction> {
	let (storage, unversioned, cdc, hooks) = sqlite(config);
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	AsyncBuilder::new(versioned, unversioned, cdc, hooks)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> AsyncBuilder<SqliteSerializableTransaction> {
	let (storage, unversioned, cdc, hooks) = sqlite(config);
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		hooks.clone(),
	));
	AsyncBuilder::new(versioned, unversioned, cdc, hooks)
}
