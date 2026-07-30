// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::Result;

use crate::operator::reclaim::{Reclaimed, StateFootprint};

pub trait Subject {
	fn apply(&mut self, change: Change) -> Result<Change>;

	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>>;

	fn reclaim(&mut self, _at_ms: u64) -> Result<Reclaimed> {
		Ok(Reclaimed::default())
	}

	fn footprint(&mut self) -> Result<Option<StateFootprint>> {
		Ok(None)
	}
}
