// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "sub_server")]

use reifydb_store_transaction::{StandardTransactionStore, backend::sqlite::SqliteConfig};
use reifydb_transaction::multi::transaction::{
	optimistic::TransactionOptimistic, serializable::SerializableTransaction,
};

use crate::{
	MemoryCdc, ServerBuilder, SingleVersionMemory, SingleVersionSqlite, SqliteCdc, memory, optimistic,
	serializable, sqlite,
};

pub fn memory_optimistic()
-> ServerBuilder<TransactionOptimistic<StandardTransactionStore, SingleVersionMemory>, SingleVersionMemory, MemoryCdc> {
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn memory_serializable()
-> ServerBuilder<SerializableTransaction<StandardTransactionStore, SingleVersionMemory>, SingleVersionMemory, MemoryCdc>
{
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_optimistic(
	config: SqliteConfig,
) -> ServerBuilder<TransactionOptimistic<StandardTransactionStore, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>
{
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_serializable(
	config: SqliteConfig,
) -> ServerBuilder<SerializableTransaction<StandardTransactionStore, SingleVersionSqlite>, SingleVersionSqlite, SqliteCdc>
{
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	ServerBuilder::new(multi, single, cdc, eventbus)
}
