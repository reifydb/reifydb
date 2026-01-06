// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_store_transaction::hot::sqlite::SqliteConfig;

use crate::{ServerBuilder, memory as memory_store, sqlite as sqlite_store, transaction};

pub fn memory() -> ServerBuilder {
	let (store, single, cdc, bus) = memory_store();
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}

pub fn sqlite(config: SqliteConfig) -> ServerBuilder {
	let (store, single, cdc, bus) = sqlite_store(config);
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus));
	ServerBuilder::new(multi, single, cdc, bus)
}
