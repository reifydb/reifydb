// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_store_transaction::backend::sqlite::SqliteConfig;

use crate::{ServerBuilder, memory as memory_store, sqlite as sqlite_store, transaction};

pub async fn memory() -> crate::Result<ServerBuilder> {
	let (store, single, cdc, bus) = memory_store().await;
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(ServerBuilder::new(multi, single, cdc, bus))
}

pub async fn sqlite(config: SqliteConfig) -> crate::Result<ServerBuilder> {
	let (store, single, cdc, bus) = sqlite_store(config).await;
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(ServerBuilder::new(multi, single, cdc, bus))
}
