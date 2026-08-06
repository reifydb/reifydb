// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::{coord::EventCoord, policy::SealPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionTracker {
	pub session_id: u64,
	pub last: DateTime,
	pub start: DateTime,
	opened: bool,
}

impl SessionTracker {
	pub fn resumed(session_id: u64, last: DateTime, start: DateTime) -> Self {
		Self {
			session_id,
			last,
			start,
			opened: true,
		}
	}

	fn is_unopened(&self) -> bool {
		!self.opened
	}

	fn adopt(&mut self, coord: DateTime) {
		self.last = coord;
		self.start = coord;
		self.opened = true;
	}

	fn extend(&mut self, coord: DateTime) {
		self.last = self.last.max(coord);
		self.start = self.start.min(coord);
	}
}

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

	pub fn seal_policy(&self, grace: Duration) -> SealPolicy {
		SealPolicy::session(self.gap, grace)
	}

	pub fn assign(&self, tracker: &mut SessionTracker, coord: EventCoord) -> SessionAssignment {
		let coord = coord.at();

		if tracker.is_unopened() {
			tracker.adopt(coord);
			return SessionAssignment::Opened(tracker.session_id);
		}
		if coord > tracker.last && coord - tracker.last > self.gap {
			let closed = tracker.session_id;
			tracker.session_id += 1;
			tracker.adopt(coord);
			return SessionAssignment::Rotated {
				closed,
				opened: tracker.session_id,
			};
		}
		if coord < tracker.start && tracker.start - coord > self.gap {
			return SessionAssignment::Refused;
		}
		tracker.extend(coord);
		SessionAssignment::Extended(tracker.session_id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::at_millis;

	use super::*;
	use crate::factory::event_coord_at_millis;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn kind() -> SessionKind {
		SessionKind::with_gap(ms(1_000))
	}

	#[test]
	fn a_quiet_period_longer_than_the_gap_rotates_to_a_new_session() {
		// Activity separated by more than the gap is two sessions. The rotation must report the id
		// it closed as well as the one it opened, or the closed session's accumulator stays live
		// forever with nothing left to seal it.
		let mut tracker = SessionTracker::default();

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(5_000)), SessionAssignment::Opened(0));
		assert_eq!(
			kind().assign(&mut tracker, event_coord_at_millis(6_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1
			}
		);
		assert_eq!(tracker.session_id, 1);
	}

	#[test]
	fn a_quiet_period_of_exactly_the_gap_stays_in_the_same_session() {
		// The gap boundary is inclusive: sessions are defined by the absence of activity for longer
		// than the gap, so splitting at exactly the gap ends a session that never went quiet.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, event_coord_at_millis(5_000));

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(6_000)), SessionAssignment::Extended(0));
	}

	#[test]
	fn a_late_row_inside_the_gap_extends_the_session_backwards() {
		// Sessions grow at both ends. If the start does not move back to cover a late row, the
		// session's span no longer contains all its rows and the seal timer is armed too late.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, event_coord_at_millis(5_000));

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(4_500)), SessionAssignment::Extended(0));
		assert_eq!(tracker.start, at_millis(4_500));
		assert_eq!(tracker.last, at_millis(5_000), "reaching backwards must not drag the high end down");
	}

	#[test]
	fn a_row_far_before_the_session_start_is_refused_rather_than_misfiled() {
		// A row more than a gap before the session start belongs to an earlier, already-sealed
		// session. Admitting it amends a published aggregate; opening a new session for it
		// interleaves two sessions on one tracker.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, event_coord_at_millis(5_000));

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(3_999)), SessionAssignment::Refused);
		assert_eq!(tracker.start, at_millis(5_000), "a refused row must leave the tracker untouched");
		assert_eq!(tracker.last, at_millis(5_000));
	}

	#[test]
	fn a_fresh_tracker_adopts_its_first_coordinate_without_closing_anything() {
		// The first row of a new group has no session to rotate out of, so it must open rather than
		// rotate - a Rotated would tell the caller to seal session id 0, whose accumulator does not
		// exist, and the seal would run against empty state.
		let mut tracker = SessionTracker::default();

		let assignment = kind().assign(&mut tracker, event_coord_at_millis(9_000));

		assert_eq!(assignment, SessionAssignment::Opened(0));
		assert_eq!(assignment.closed(), None);
		assert_eq!(tracker, SessionTracker::resumed(0, at_millis(9_000), at_millis(9_000)));
	}

	#[test]
	fn a_session_opened_at_the_epoch_still_rotates_across_its_gap() {
		// Openness used to be encoded as `last == 0`, which is also a real coordinate, so a session
		// opened at the epoch read as unopened forever and every row folded into one aggregate. It
		// is now carried explicitly, which makes the epoch an ordinary coordinate.
		let mut tracker = SessionTracker::default();

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(0)), SessionAssignment::Opened(0));
		assert_eq!(tracker.last, at_millis(0), "the tracker must keep the epoch coordinate it adopted");
		assert_eq!(
			kind().assign(&mut tracker, event_coord_at_millis(1_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1
			}
		);
	}

	#[test]
	fn a_tracker_that_has_adopted_the_epoch_is_distinguishable_from_a_fresh_one() {
		// The store tells openness apart by whether a SessionState row exists, and load_session maps
		// that onto these two values. Comparing equal would erase the distinction and bring the
		// epoch collision back through the persistence path.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, event_coord_at_millis(0));

		assert_ne!(tracker, SessionTracker::default());
		assert_eq!(tracker, SessionTracker::resumed(0, at_millis(0), at_millis(0)));
	}

	#[test]
	fn a_resumed_tracker_continues_the_session_it_was_persisted_with() {
		// A session outlives the batch that opened it, so the tracker is reloaded per batch.
		// Resuming into a fresh tracker restarts the ids at 0 and aliases every group's second
		// session onto its first.
		let mut tracker = SessionTracker::resumed(7, at_millis(5_000), at_millis(4_000));

		assert_eq!(kind().assign(&mut tracker, event_coord_at_millis(5_500)), SessionAssignment::Extended(7));
		assert_eq!(tracker, SessionTracker::resumed(7, at_millis(5_500), at_millis(4_000)));
	}

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

	#[test]
	fn a_zero_gap_puts_every_distinct_instant_in_its_own_session() {
		// A zero gap means any quiet period ends the session, so consecutive instants rotate but a
		// repeated instant does not. Zero is coherent here, unlike a sliding slide: it is the
		// degenerate "group by instant" case and there is no division to blow up.
		let kind = SessionKind::with_gap(ms(0));
		let mut tracker = SessionTracker::default();

		assert_eq!(kind.assign(&mut tracker, event_coord_at_millis(5_000)), SessionAssignment::Opened(0));
		assert_eq!(kind.assign(&mut tracker, event_coord_at_millis(5_000)), SessionAssignment::Extended(0));
		assert_eq!(
			kind.assign(&mut tracker, event_coord_at_millis(5_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1
			}
		);
	}
}
