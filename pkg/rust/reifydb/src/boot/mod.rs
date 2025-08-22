// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod start;

use reifydb_core::interface::{WithHooks, Transaction};
use reifydb_engine::StandardEngine;

use crate::boot::{create::CreateCallback, start::StartCallback};

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
		let hooks = engine.hooks();

		hooks.register(StartCallback::new(engine.unversioned_owned()));
		hooks.register(CreateCallback::new(engine.clone()));

		Ok(())
	}
}
