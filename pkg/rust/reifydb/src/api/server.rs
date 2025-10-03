// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "sub_server")]

use reifydb_store_transaction::backend::sqlite::SqliteConfig;

use crate::{ServerBuilder, memory, optimistic, serializable, sqlite};

pub fn memory_optimistic() -> ServerBuilder {
	let (store, single, cdc, bus) = memory();
	let (multi, single, cdc, bus) = optimistic((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}

pub fn memory_serializable() -> ServerBuilder {
	let (store, single, cdc, bus) = memory();
	let (multi, single, cdc, bus) = serializable((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}

pub fn sqlite_optimistic(config: SqliteConfig) -> ServerBuilder {
	let (store, single, cdc, bus) = sqlite(config);
	let (multi, single, cdc, bus) = optimistic((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}

pub fn sqlite_serializable(config: SqliteConfig) -> ServerBuilder {
	let (store, single, cdc, bus) = sqlite(config);
	let (multi, single, cdc, bus) = serializable((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}
