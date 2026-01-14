// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_store_multi::sqlite::SqliteConfig;

use crate::EmbeddedBuilder;
use crate::api::StorageFactory;

/// Create an embedded database with in-memory storage.
pub fn memory() -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Memory)
}

/// Create an embedded database with SQLite storage.
pub fn sqlite(config: SqliteConfig) -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Sqlite(config))
}
