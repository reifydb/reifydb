// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_store_transaction::hot::sqlite::SqliteConfig;

use crate::{EmbeddedBuilder, memory as memory_store, sqlite as sqlite_store, transaction};

pub async fn memory() -> crate::Result<EmbeddedBuilder> {
	let (store, single, cdc, bus) = memory_store().await;
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(EmbeddedBuilder::new(multi, single, cdc, bus))
}

pub async fn sqlite(config: SqliteConfig) -> crate::Result<EmbeddedBuilder> {
	let (store, single, cdc, bus) = sqlite_store(config).await;
	let (multi, single, cdc, bus) = transaction((store, single, cdc, bus)).await?;
	Ok(EmbeddedBuilder::new(multi, single, cdc, bus))
}
