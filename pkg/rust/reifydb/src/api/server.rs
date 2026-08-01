// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sqlite::SqliteConfig;

use crate::{ServerBuilder, api::StorageFactory};

pub fn memory() -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Memory)
}

pub fn sqlite(config: SqliteConfig) -> ServerBuilder {
	ServerBuilder::new(StorageFactory::Sqlite(config))
}
