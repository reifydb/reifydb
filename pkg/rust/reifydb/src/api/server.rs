// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_store_multi::hot::sqlite::config::SqliteConfig;

use crate::{ServerBuilder, api::StorageFactory};

/// Create a server with in-memory storage.
pub fn memory() -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Memory)
}

/// Create a server with SQLite storage.
pub fn sqlite(config: SqliteConfig) -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Sqlite(config))
}
