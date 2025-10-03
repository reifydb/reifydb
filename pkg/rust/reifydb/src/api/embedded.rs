// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_store_transaction::backend::sqlite::SqliteConfig;

use crate::{EmbeddedBuilder, memory, optimistic, serializable, sqlite};

pub fn memory_optimistic() -> EmbeddedBuilder {
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(multi, single, cdc, eventbus)
}

pub fn memory_serializable() -> EmbeddedBuilder {
	let (storage, single, cdc, eventbus) = memory();
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_optimistic(config: SqliteConfig) -> EmbeddedBuilder {
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = optimistic((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(multi, single, cdc, eventbus)
}

pub fn sqlite_serializable(config: SqliteConfig) -> EmbeddedBuilder {
	let (storage, single, cdc, eventbus) = sqlite(config);
	let (multi, _, _, _) = serializable((storage.clone(), single.clone(), cdc.clone(), eventbus.clone()));
	EmbeddedBuilder::new(multi, single, cdc, eventbus)
}
