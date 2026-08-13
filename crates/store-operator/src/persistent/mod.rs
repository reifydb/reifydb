// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod sqlite;

use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};

use crate::persistent::sqlite::storage::SqliteOperatorStorage;

#[derive(Clone)]
pub enum OperatorPersistentTier {
	Sqlite(SqliteOperatorStorage),
}

impl OperatorPersistentTier {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqliteOperatorStorage::new(config))
	}

	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SqliteOperatorStorage::in_memory();
		(Self::Sqlite(storage), guard)
	}
}
