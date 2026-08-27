// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod start;

use reifydb_engine::{engine::StandardEngine, queue::hydrate::hydrate_queues};

use crate::{
	MigrationStatement, Result,
	boot::start::{apply_migrations, configure_store, ensure_storage_version},
};

pub struct Bootloader {
	engine: StandardEngine,
}

impl Bootloader {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}
}

impl Bootloader {
	pub fn load(&self) -> Result<()> {
		ensure_storage_version(&self.engine.single_owned())?;
		configure_store(&self.engine)?;
		hydrate_queues(&self.engine)?;
		Ok(())
	}

	pub fn apply_migrations(&self, migrations: &[MigrationStatement]) -> Result<()> {
		apply_migrations(&self.engine, migrations)
	}
}
