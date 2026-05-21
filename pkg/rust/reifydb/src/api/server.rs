// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_sqlite::SqliteConfig;

use crate::{ServerBuilder, api::StorageFactory};

/// Create a server with in-memory storage.
pub fn memory() -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Memory)
}

/// Create a server with SQLite storage.
pub fn sqlite(config: SqliteConfig) -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Sqlite(config))
}

/// Create a server with SQLite storage and no in-memory buffer.
pub fn sqlite_without_buffer(config: SqliteConfig) -> ServerBuilder {
	ServerBuilder::new(StorageFactory::SqliteWithoutBuffer(config))
}
