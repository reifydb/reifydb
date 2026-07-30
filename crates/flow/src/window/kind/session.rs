// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::{coord::EventCoord, policy::SealPolicy, span::WindowCoord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionTracker {
	pub session_id: u64,
	pub last: u64,
	pub start: u64,
}

impl SessionTracker {
	pub fn resumed(session_id: u64, last: u64, start: u64) -> Self {
		Self {
			session_id,
			last,
			start,
		}
	}

	fn is_unopened(&self) -> bool {
		self.last == 0
	}

	fn adopt(&mut self, coord: u64) {
		self.last = coord;
		self.start = coord;
	}

	fn extend(&mut self, coord: u64) {
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

	pub fn gap_millis(&self) -> u64 {
		<DateTime as WindowCoord>::span_millis(self.gap).unwrap_or(0)
	}

	pub fn seal_policy(&self, grace: Duration) -> SealPolicy {
		SealPolicy::session(self.gap, grace)
	}

	pub fn assign(&self, tracker: &mut SessionTracker, coord: EventCoord) -> SessionAssignment {
		let coord = coord.at().to_order();
		let gap = self.gap_millis();

		if tracker.is_unopened() {
			tracker.adopt(coord);
			return SessionAssignment::Opened(tracker.session_id);
		}
		if coord > tracker.last && coord - tracker.last > gap {
			let closed = tracker.session_id;
			tracker.session_id += 1;
			tracker.adopt(coord);
			return SessionAssignment::Rotated {
				closed,
				opened: tracker.session_id,
			};
		}
		if coord < tracker.start && tracker.start - coord > gap {
			return SessionAssignment::Refused;
		}
		tracker.extend(coord);
		SessionAssignment::Extended(tracker.session_id)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn at(millis: u64) -> EventCoord {
		EventCoord::of(&DateTime::from_millis(millis))
	}

	fn kind() -> SessionKind {
		SessionKind::with_gap(ms(1_000))
	}

	#[test]
	fn a_quiet_period_longer_than_the_gap_rotates_to_a_new_session() {
		// This IS the definition of a session window - activity separated by more than
		// the gap is two sessions, not one. The rotation has to report the id it closed as well
		// as the one it opened, because the caller has to go and seal the closed session's
		// accumulator; returning only the new id leaves the old session's state live forever with
		// nothing left to close it.
		let mut tracker = SessionTracker::default();

		assert_eq!(kind().assign(&mut tracker, at(5_000)), SessionAssignment::Opened(0));
		assert_eq!(
			kind().assign(&mut tracker, at(6_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1
			}
		);
		assert_eq!(tracker.session_id, 1);
	}

	#[test]
	fn a_quiet_period_of_exactly_the_gap_stays_in_the_same_session() {
		// The gap boundary is inclusive - a row landing exactly one gap after the last
		// one still belongs to the session. Sessions are defined by the ABSENCE of activity for
		// longer than the gap, so splitting at exactly the gap would end a session that never
		// actually went quiet.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, at(5_000));

		assert_eq!(kind().assign(&mut tracker, at(6_000)), SessionAssignment::Extended(0));
	}

	#[test]
	fn a_late_row_inside_the_gap_extends_the_session_backwards() {
		// Sessions grow at BOTH ends. A row that arrives out of order but lands within
		// the gap of the session's start belongs to it, and the start has to move back to cover
		// it - otherwise the session's own span no longer contains all its rows, and the seal
		// timer is armed from a start that is too late.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, at(5_000));

		assert_eq!(kind().assign(&mut tracker, at(4_500)), SessionAssignment::Extended(0));
		assert_eq!(tracker.start, 4_500);
		assert_eq!(tracker.last, 5_000, "reaching backwards must not drag the high end down");
	}

	#[test]
	fn a_row_far_before_the_session_start_is_refused_rather_than_misfiled() {
		// A row more than a gap BEFORE the session start belongs to an earlier session
		// that has already been sealed and emitted. Admitting it would silently amend a published
		// aggregate; opening a new session for it would interleave two sessions on one tracker.
		// Refusing is the only answer that leaves both correct, and the caller counts it as a
		// sealed drop.
		let mut tracker = SessionTracker::default();

		kind().assign(&mut tracker, at(5_000));

		assert_eq!(kind().assign(&mut tracker, at(3_999)), SessionAssignment::Refused);
		assert_eq!(tracker.start, 5_000, "a refused row must leave the tracker untouched");
		assert_eq!(tracker.last, 5_000);
	}

	#[test]
	fn a_fresh_tracker_adopts_its_first_coordinate_without_closing_anything() {
		// The first row of a brand-new group has no session to rotate out of, so it must
		// open rather than rotate - a Rotated here would tell the caller to seal session id 0,
		// which has no accumulator, and the seal would run against empty state.
		// "unopened" is encoded as `last == 0`, a sentinel that collides with a real
		// coordinate at the epoch. It is baked into the persisted SessionState encoding,
		// so replacing it is a state-format change.
		let mut tracker = SessionTracker::default();

		let assignment = kind().assign(&mut tracker, at(9_000));

		assert_eq!(assignment, SessionAssignment::Opened(0));
		assert_eq!(assignment.closed(), None);
		assert_eq!(tracker, SessionTracker::resumed(0, 9_000, 9_000));
	}

	#[test]
	fn a_resumed_tracker_continues_the_session_it_was_persisted_with() {
		// A session outlives the batch that opened it, so the tracker is reloaded from the store
		// on the next batch. Resuming into a fresh tracker instead would restart the ids at 0 and
		// alias every group's second session onto its first.
		let mut tracker = SessionTracker::resumed(7, 5_000, 4_000);

		assert_eq!(kind().assign(&mut tracker, at(5_500)), SessionAssignment::Extended(7));
		assert_eq!(tracker, SessionTracker::resumed(7, 5_500, 4_000));
	}

	#[test]
	fn a_refused_row_reports_no_session_and_a_rotation_reports_the_new_one() {
		// The caller routes on session_id() alone, so the accessor has to agree with the variant
		// it came from. A Rotated that answered its CLOSED id would file the row that caused the
		// rotation into the session it just ended.
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
		// A zero gap means any quiet period at all ends the session, so consecutive
		// instants rotate but a repeated instant does not. Unlike the sliding slide, zero is a
		// coherent value here and must not be refused - it is the degenerate "group by instant"
		// case, and there is no division to blow up.
		let kind = SessionKind::with_gap(ms(0));
		let mut tracker = SessionTracker::default();

		assert_eq!(kind.assign(&mut tracker, at(5_000)), SessionAssignment::Opened(0));
		assert_eq!(kind.assign(&mut tracker, at(5_000)), SessionAssignment::Extended(0));
		assert_eq!(
			kind.assign(&mut tracker, at(5_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1
			}
		);
	}
}
