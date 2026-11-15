// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::error::ValueError;

use crate::operator::reclaim::StateFootprint;

pub trait Subject {
	fn apply(&mut self, change: Change) -> Result<Change, ValueError>;

	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>, ValueError>;

	fn footprint(&mut self) -> Result<Option<StateFootprint>, ValueError> {
		Ok(None)
	}
}
