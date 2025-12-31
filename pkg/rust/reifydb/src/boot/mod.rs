// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
	pub async fn load(&self) -> crate::Result<()> {
		let engine = self.engine.clone();
		let eventbus = engine.event_bus();

		eventbus.register(StartEventListener::new(engine.single_owned())).await;
		eventbus.register(CreateEventListener::new(engine.clone())).await;

		Ok(())
	}
}
