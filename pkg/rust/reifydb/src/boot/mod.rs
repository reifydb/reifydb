// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod start;

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSystem;

use crate::{
	Result,
	boot::start::{ensure_storage_version, spawn_actors},
};

pub struct Bootloader {
	engine: StandardEngine,
	actor_system: ActorSystem,
}

impl Bootloader {
	pub fn new(engine: StandardEngine, actor_system: ActorSystem) -> Self {
		Self {
			engine: engine.clone(),
			actor_system,
		}
	}
}

impl Bootloader {
	pub fn load(&self) -> Result<()> {
		ensure_storage_version(&self.engine.single_owned())?;
		spawn_actors(&self.engine, &self.actor_system)?;
		Ok(())
	}
}
