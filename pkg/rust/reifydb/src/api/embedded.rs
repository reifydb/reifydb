// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_storage::sqlite::SqliteConfig;

use crate::{
	EmbeddedBuilder, MemoryOptimisticTransaction,
	MemorySerializableTransaction, SqliteOptimisticTransaction,
	SqliteSerializableTransaction, memory, optimistic, serializable,
	sqlite,
};

pub fn memory_optimistic() -> EmbeddedBuilder<MemoryOptimisticTransaction> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn memory_serializable() -> EmbeddedBuilder<MemorySerializableTransaction> {
	let (storage, unversioned, cdc, eventbus) = memory();
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> EmbeddedBuilder<SqliteOptimisticTransaction> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = optimistic((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> EmbeddedBuilder<SqliteSerializableTransaction> {
	let (storage, unversioned, cdc, eventbus) = sqlite(config);
	let (versioned, _, _, _) = serializable((
		storage.clone(),
		unversioned.clone(),
		cdc.clone(),
		eventbus.clone(),
	));
	EmbeddedBuilder::new(versioned, unversioned, cdc, eventbus)
}
