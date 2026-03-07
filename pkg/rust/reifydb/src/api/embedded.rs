// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_store_multi::hot::sqlite::config::SqliteConfig;

use crate::{EmbeddedBuilder, api::StorageFactory};

/// Create an embedded database with in-memory storage.
pub fn memory() -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Memory)
}

/// Create an embedded database with SQLite storage.
pub fn sqlite(config: SqliteConfig) -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Sqlite(config))
}
