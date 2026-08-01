// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sqlite::SqliteConfig;

use crate::{EmbeddedBuilder, api::StorageFactory};

pub fn memory() -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Memory)
}

pub fn sqlite(config: SqliteConfig) -> EmbeddedBuilder {
	EmbeddedBuilder::new(StorageFactory::Sqlite(config))
}
