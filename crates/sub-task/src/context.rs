// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_engine::engine::StandardEngine;

#[derive(Clone)]
pub struct TaskContext {
	engine: StandardEngine,
}

impl TaskContext {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}
}
