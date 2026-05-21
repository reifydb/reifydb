// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_sqlite::SqliteConfig;

use crate::{EmbeddedBuilder, api::StorageFactory};

/// Create an embedded database with in-memory storage.
pub fn memory() -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Memory)
}

/// Create an embedded database with SQLite storage.
pub fn sqlite(config: SqliteConfig) -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Sqlite(config))
}

/// Create an embedded database with SQLite storage and no in-memory buffer.
pub fn sqlite_without_buffer(config: SqliteConfig) -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::SqliteWithoutBuffer(config))
}
