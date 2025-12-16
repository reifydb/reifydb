// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_store_transaction::backend::sqlite::SqliteConfig;

use crate::{EmbeddedBuilder, memory as memory_store, sqlite as sqlite_store, transaction};

pub fn memory() -> EmbeddedBuilder {
	let (store, single, cdc, bus) = memory_store();
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus));
	EmbeddedBuilder::new(multi, single, cdc, bus)
}

pub fn sqlite(config: SqliteConfig) -> EmbeddedBuilder {
	let (store, single, cdc, bus) = sqlite_store(config);
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus));
	EmbeddedBuilder::new(multi, single, cdc, bus)
}
