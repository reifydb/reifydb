// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod start;

use reifydb_core::interface::WithEventBus;
use reifydb_engine::StandardEngine;

use crate::boot::{create::CreateEventListener, start::StartEventListener};

pub struct Bootloader {
	engine: StandardEngine,
}

impl Bootloader {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine: engine.clone(),
		}
	}
}

impl Bootloader {
	pub fn load(&self) -> crate::Result<()> {
		let engine = self.engine.clone();
		let eventbus = engine.event_bus();

		eventbus.register(StartEventListener::new(engine.single_owned()));
		eventbus.register(CreateEventListener::new(engine.clone()));

		Ok(())
	}
}
