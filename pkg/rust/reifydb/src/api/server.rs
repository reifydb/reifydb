// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_store_transaction::backend::sqlite::SqliteConfig;

use crate::{ServerBuilder, memory as memory_store, sqlite as sqlite_store, transaction};

pub async fn memory() -> crate::Result<ServerBuilder> {
	let (store, single, cdc, bus) = memory_store();
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(ServerBuilder::new(multi, single, cdc, bus))
}

pub async fn sqlite(config: SqliteConfig) -> crate::Result<ServerBuilder> {
	let (store, single, cdc, bus) = sqlite_store(config);
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(ServerBuilder::new(multi, single, cdc, bus))
}
