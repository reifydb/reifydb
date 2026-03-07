// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod start;

use reifydb_engine::engine::StandardEngine;

use crate::boot::start::ensure_storage_version;

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
		ensure_storage_version(&self.engine.single_owned())?;
		Ok(())
	}
}
