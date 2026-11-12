// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod open;

use reifydb_value::value::duration::Duration;

use crate::operator::state::seal::rule::SealRule;

pub struct SessionKind {
	gap: Duration,
}

impl SessionKind {
	pub fn with_gap(gap: Duration) -> Self {
		Self {
			gap,
		}
	}

	pub fn seal_rule(&self, lateness: Duration) -> SealRule {
		SealRule::session(self.gap, lateness)
	}
}
