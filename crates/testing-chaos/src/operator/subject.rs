// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::Result;

use crate::operator::reclaim::{Reclaimed, StateFootprint};

pub trait Subject {
	fn apply(&mut self, change: Change) -> Result<Change>;

	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>>;

	/// Panics rather than reporting an empty sweep, because the two are indistinguishable to a caller
	/// and only one of them is a passing test.
	///
	/// A subject with no reclaim substrate that answered `Reclaimed::default()` would report exactly
	/// what a working subject reports when nothing is due, so a suite driving reclamation against it
	/// would go green while exercising nothing. That is the same silent-skip failure the harness
	/// warns about for a missing activity grid, one layer up. `drive` only reaches this when a
	/// scenario sets `reclaim_pct`, so a subject that never reclaims is never asked to.
	fn reclaim(&mut self, _at_ms: u64) -> Result<Reclaimed> {
		panic!("this subject cannot reclaim, but the scenario asked it to; implement Subject::reclaim or \
			 leave Scenario::reclaim_pct at zero")
	}

	fn footprint(&mut self) -> Result<Option<StateFootprint>> {
		Ok(None)
	}
}
