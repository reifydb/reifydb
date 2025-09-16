// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod start;

use reifydb_core::interface::{Transaction, WithEventBus};
use reifydb_engine::StandardEngine;

use crate::boot::{create::CreateEventListener, start::StartEventListener};

pub struct Bootloader<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> Bootloader<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self {
			engine: engine.clone(),
		}
	}
}

impl<T: Transaction> Bootloader<T> {
	pub fn load(&self) -> crate::Result<()> {
		let engine = self.engine.clone();
		let eventbus = engine.event_bus();

		eventbus.register(StartEventListener::new(engine.unversioned_owned()));
		eventbus.register(CreateEventListener::new(engine.clone()));

		Ok(())
	}
}
