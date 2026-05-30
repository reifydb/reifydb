// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

mod start;

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSpawner;

use crate::{
	MigrationStatement, Result,
	boot::start::{apply_migrations, ensure_storage_version, spawn_actors},
};

pub struct Bootloader {
	engine: StandardEngine,
	spawner: ActorSpawner,
}

impl Bootloader {
	pub fn new(engine: StandardEngine, spawner: ActorSpawner) -> Self {
		Self {
			engine: engine.clone(),
			spawner,
		}
	}
}

impl Bootloader {
	pub fn load(&self) -> Result<()> {
		ensure_storage_version(&self.engine.single_owned())?;
		spawn_actors(&self.engine, &self.spawner)?;
		Ok(())
	}

	pub fn apply_migrations(&self, migrations: &[MigrationStatement]) -> Result<()> {
		apply_migrations(&self.engine, migrations)
	}
}
