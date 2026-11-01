// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod open;

use reifydb_value::value::duration::Duration;

use crate::operator::state::seal::rule::SealRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAssignment {
	Opened(u64),
	Extended(u64),
	Rotated {
		closed: u64,
		opened: u64,
	},
	Refused,
}

impl SessionAssignment {
	pub fn session_id(self) -> Option<u64> {
		match self {
			SessionAssignment::Opened(session_id) | SessionAssignment::Extended(session_id) => {
				Some(session_id)
			}
			SessionAssignment::Rotated {
				opened,
				..
			} => Some(opened),
			SessionAssignment::Refused => None,
		}
	}

	pub fn closed(self) -> Option<u64> {
		match self {
			SessionAssignment::Rotated {
				closed,
				..
			} => Some(closed),
			_ => None,
		}
	}
}

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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_refused_row_reports_no_session_and_a_rotation_reports_the_new_one() {
		// The caller routes on session_id() alone, so a Rotated that answered its closed id would
		// file the row that caused the rotation into the session it just ended.
		assert_eq!(SessionAssignment::Refused.session_id(), None);
		assert_eq!(
			SessionAssignment::Rotated {
				closed: 3,
				opened: 4
			}
			.session_id(),
			Some(4)
		);
		assert_eq!(SessionAssignment::Opened(2).session_id(), Some(2));
		assert_eq!(SessionAssignment::Extended(2).session_id(), Some(2));
	}
}
