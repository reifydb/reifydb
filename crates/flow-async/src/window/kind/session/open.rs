// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::take;

use reifydb_core::internal_err;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::state::seal::coord::Coord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Member<S> {
	pub at: S,
	pub row: RowNumber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenSession<S> {
	pub id: u64,
	pub start: S,
	pub last: S,
	pub members: Vec<Member<S>>,
}

impl<S: Coord> OpenSession<S> {
	fn of(id: u64, members: Vec<Member<S>>) -> Self {
		let start = members.first().expect("an open session holds at least one member").at;
		let last = members.last().expect("an open session holds at least one member").at;
		Self {
			id,
			start,
			last,
			members,
		}
	}

	fn reach(&self, at: S, gap: S::Span) -> bool {
		if at < self.start {
			return self.start.span_since(at) < gap;
		}
		if at > self.last {
			return at.span_since(self.last) < gap;
		}
		true
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionIds {
	StartTime,
	Counter {
		next: u64,
	},
}

impl SessionIds {
	fn mint<S: Coord>(&mut self, start: S, taken: &[OpenSession<S>]) -> u64 {
		match self {
			SessionIds::StartTime => {
				let mut id = start.to_order();
				while taken.iter().any(|session| session.id == id) {
					id += 1;
				}
				id
			}
			SessionIds::Counter {
				next,
			} => {
				let id = *next;
				*next += 1;
				id
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Insertion<S> {
	Opened(u64),
	Extended(u64),
	Merged {
		survivor: u64,
		absorbed: Vec<OpenSession<S>>,
	},
	Refused,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Removal<S> {
	Shrunk(u64),
	Split {
		kept: u64,
		moved: OpenSession<S>,
	},
	Emptied(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenSessions<S: Coord> {
	gap: S::Span,
	ids: SessionIds,
	sessions: Vec<OpenSession<S>>,
	sealed_last: Option<S>,
}

impl<S: Coord> OpenSessions<S> {
	pub fn new(gap: S::Span, ids: SessionIds) -> Self {
		Self::resumed(gap, ids, Vec::new(), None)
	}

	pub fn resumed(gap: S::Span, ids: SessionIds, sessions: Vec<OpenSession<S>>, sealed_last: Option<S>) -> Self {
		Self {
			gap,
			ids,
			sessions,
			sealed_last,
		}
	}

	pub fn sessions(&self) -> &[OpenSession<S>] {
		&self.sessions
	}

	pub fn sealed_last(&self) -> Option<S> {
		self.sealed_last
	}

	pub fn ids(&self) -> SessionIds {
		self.ids
	}

	pub fn insert(&mut self, at: S, row: RowNumber, horizon: S) -> Insertion<S> {
		let reached: Vec<usize> =
			(0..self.sessions.len()).filter(|&index| self.sessions[index].reach(at, self.gap)).collect();
		if self.touches_sealed(at) || (reached.is_empty() && at < horizon) {
			return Insertion::Refused;
		}
		let member = Member {
			at,
			row,
		};
		match reached.as_slice() {
			[] => {
				let id = self.ids.mint(at, &self.sessions);
				self.place(OpenSession::of(id, vec![member]));
				Insertion::Opened(id)
			}
			[index] => {
				let session = &mut self.sessions[*index];
				insert_sorted(&mut session.members, member);
				*session = OpenSession::of(session.id, take(&mut session.members));
				Insertion::Extended(session.id)
			}
			[first, rest @ ..] => {
				let mut absorbed: Vec<OpenSession<S>> = Vec::with_capacity(rest.len());
				for &index in rest.iter().rev() {
					absorbed.push(self.sessions.remove(index));
				}
				absorbed.reverse();
				let survivor = &mut self.sessions[*first];
				let mut members = take(&mut survivor.members);
				for session in &absorbed {
					members.extend(session.members.iter().copied());
				}
				members.push(member);
				members.sort_by_key(|member| member.at);
				*survivor = OpenSession::of(survivor.id, members);
				Insertion::Merged {
					survivor: survivor.id,
					absorbed,
				}
			}
		}
	}

	pub fn remove(&mut self, id: u64, row: RowNumber) -> Result<Option<Removal<S>>> {
		let Some(index) = self.sessions.iter().position(|session| session.id == id) else {
			return Ok(None);
		};
		let Some(position) = self.sessions[index].members.iter().position(|member| member.row == row) else {
			return internal_err!(
				"open session {} has no member row {:?}; the row index and the session members disagree",
				id,
				row
			);
		};
		self.sessions[index].members.remove(position);
		if self.sessions[index].members.is_empty() {
			self.sessions.remove(index);
			return Ok(Some(Removal::Emptied(id)));
		}
		let members = take(&mut self.sessions[index].members);
		let split = (1..members.len()).find(|&at| members[at].at.span_since(members[at - 1].at) >= self.gap);
		match split {
			None => {
				self.sessions[index] = OpenSession::of(id, members);
				Ok(Some(Removal::Shrunk(id)))
			}
			Some(at) => {
				let mut kept = members;
				let tail = kept.split_off(at);
				self.sessions[index] = OpenSession::of(id, kept);
				let moved = OpenSession::of(self.ids.mint(tail[0].at, &self.sessions), tail);
				self.place(moved.clone());
				Ok(Some(Removal::Split {
					kept: id,
					moved,
				}))
			}
		}
	}

	pub fn close(&mut self, horizon: S) -> Vec<OpenSession<S>> {
		let (closed, open): (Vec<_>, Vec<_>) =
			take(&mut self.sessions).into_iter().partition(|session| session.last < horizon);
		self.sessions = open;
		if let Some(last) = closed.iter().map(|session| session.last).max() {
			self.sealed_last = Some(self.sealed_last.map_or(last, |sealed| sealed.max(last)));
		}
		closed
	}

	fn touches_sealed(&self, at: S) -> bool {
		match self.sealed_last {
			Some(sealed) if at >= sealed => at.span_since(sealed) < self.gap,
			Some(_) => true,
			None => false,
		}
	}

	fn place(&mut self, session: OpenSession<S>) {
		let index = self.sessions.partition_point(|existing| existing.start <= session.start);
		self.sessions.insert(index, session);
	}
}

fn insert_sorted<S: Coord>(members: &mut Vec<Member<S>>, member: Member<S>) {
	let index = members.partition_point(|existing| existing.at <= member.at);
	members.insert(index, member);
}

#[cfg(test)]
mod tests {
	use reifydb_value::{
		factory::time::at_millis,
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::{Insertion, Member, OpenSession, OpenSessions, Removal, SessionIds};

	fn sessions() -> OpenSessions<DateTime> {
		OpenSessions::new(Duration::from_milliseconds_const(1_000), SessionIds::StartTime)
	}

	fn id(millis: i64) -> u64 {
		at_millis(millis).to_order()
	}

	fn member(millis: i64, row: u64) -> Member<DateTime> {
		Member {
			at: at_millis(millis),
			row: RowNumber(row),
		}
	}

	fn far_past() -> DateTime {
		at_millis(0)
	}

	fn seeded(points: &[(i64, u64)]) -> OpenSessions<DateTime> {
		let mut open = sessions();
		for &(millis, row) in points {
			open.insert(at_millis(millis), RowNumber(row), far_past());
		}
		open
	}

	fn bounds(open: &OpenSessions<DateTime>) -> Vec<(u64, DateTime, DateTime, usize)> {
		open.sessions().iter().map(|s| (s.id, s.start, s.last, s.members.len())).collect()
	}

	#[test]
	fn removing_the_bridge_row_splits_the_session_and_the_back_half_gets_a_new_id() {
		// Without the split a deleted bridge keeps two quiet-separated runs in one session, unlike a fresh run.
		let mut open = seeded(&[(10_000, 1), (10_800, 2), (11_600, 3)]);
		assert_eq!(bounds(&open), vec![(id(10_000), at_millis(10_000), at_millis(11_600), 3)]);

		let removal = open.remove(id(10_000), RowNumber(2)).unwrap();

		assert_eq!(
			removal,
			Some(Removal::Split {
				kept: id(10_000),
				moved: OpenSession {
					id: id(11_600),
					start: at_millis(11_600),
					last: at_millis(11_600),
					members: vec![member(11_600, 3)],
				},
			})
		);
		assert_eq!(
			bounds(&open),
			vec![
				(id(10_000), at_millis(10_000), at_millis(10_000), 1),
				(id(11_600), at_millis(11_600), at_millis(11_600), 1),
			]
		);
	}

	#[test]
	fn removing_an_end_row_shrinks_the_bounds_so_a_later_row_one_gap_away_opens_a_new_session() {
		// A last that never shrinks lets a row 0.85s after a removed member join a session it is 1.05s from.
		let mut open = seeded(&[(11_000, 1), (11_200, 2)]);

		assert_eq!(open.remove(id(11_000), RowNumber(2)).unwrap(), Some(Removal::Shrunk(id(11_000))));
		assert_eq!(bounds(&open), vec![(id(11_000), at_millis(11_000), at_millis(11_000), 1)]);
		assert_eq!(open.insert(at_millis(12_050), RowNumber(2), far_past()), Insertion::Opened(id(12_050)));
	}

	#[test]
	fn removing_the_only_member_empties_the_session_and_drops_it() {
		// An emptied session left in the set would still capture rows that should open a fresh session.
		let mut open = seeded(&[(10_000, 1)]);

		assert_eq!(open.remove(id(10_000), RowNumber(1)).unwrap(), Some(Removal::Emptied(id(10_000))));
		assert!(open.sessions().is_empty());
	}

	#[test]
	fn a_row_exactly_one_gap_after_the_last_member_opens_a_new_session() {
		// The gap is exclusive, so a distance of exactly one gap must separate rather than extend.
		let open = seeded(&[(11_100, 1), (12_100, 2)]);

		assert_eq!(
			bounds(&open),
			vec![
				(id(11_100), at_millis(11_100), at_millis(11_100), 1),
				(id(12_100), at_millis(12_100), at_millis(12_100), 1)
			]
		);
	}

	#[test]
	fn a_row_within_the_gap_of_two_sessions_merges_them_and_the_earlier_one_survives() {
		// Keeping both sessions after a bridge disagrees with a fresh run; keeping the later id breaks id ==
		// start.
		let mut open = seeded(&[(11_100, 1), (12_100, 2)]);

		let insertion = open.insert(at_millis(11_650), RowNumber(3), far_past());

		assert_eq!(
			insertion,
			Insertion::Merged {
				survivor: id(11_100),
				absorbed: vec![OpenSession {
					id: id(12_100),
					start: at_millis(12_100),
					last: at_millis(12_100),
					members: vec![member(12_100, 2)],
				}],
			}
		);
		assert_eq!(
			open.sessions(),
			&[OpenSession {
				id: id(11_100),
				start: at_millis(11_100),
				last: at_millis(12_100),
				members: vec![member(11_100, 1), member(11_650, 3), member(12_100, 2)],
			}]
		);
	}

	#[test]
	fn a_row_that_reaches_only_one_session_extends_it_at_either_end() {
		// Extending the wrong end, or opening instead of extending, splits one burst of activity into two
		// sessions.
		let mut open = seeded(&[(10_000, 1)]);

		assert_eq!(open.insert(at_millis(9_500), RowNumber(2), far_past()), Insertion::Extended(id(10_000)));
		assert_eq!(open.insert(at_millis(10_900), RowNumber(3), far_past()), Insertion::Extended(id(10_000)));
		assert_eq!(bounds(&open), vec![(id(10_000), at_millis(9_500), at_millis(10_900), 3)]);
	}

	#[test]
	fn a_bridge_into_a_closed_session_is_refused_and_leaves_the_open_session_untouched() {
		// Closed means final: admitting the bridge would either amend a published session or misfile the row.
		let mut open = seeded(&[(10_000, 1), (11_300, 2)]);

		let closed = open.close(at_millis(11_100));

		assert_eq!(closed.iter().map(|s| s.id).collect::<Vec<_>>(), vec![id(10_000)]);
		assert_eq!(open.sealed_last(), Some(at_millis(10_000)));
		assert_eq!(open.insert(at_millis(10_600), RowNumber(3), at_millis(11_100)), Insertion::Refused);
		assert_eq!(bounds(&open), vec![(id(11_300), at_millis(11_300), at_millis(11_300), 1)]);
	}

	#[test]
	fn a_split_off_front_half_behind_the_horizon_closes_and_then_refuses_rows_near_it() {
		// A front half that stayed open past its close time would take rows a fresh run already finalised.
		let mut open = seeded(&[(10_100, 1), (10_900, 2), (11_700, 3)]);

		open.remove(id(10_100), RowNumber(2)).unwrap();
		let closed = open.close(at_millis(11_100));

		assert_eq!(closed.iter().map(|s| s.id).collect::<Vec<_>>(), vec![id(10_100)]);
		assert_eq!(open.insert(at_millis(10_500), RowNumber(4), at_millis(11_100)), Insertion::Refused);
		assert_eq!(bounds(&open), vec![(id(11_700), at_millis(11_700), at_millis(11_700), 1)]);
	}

	#[test]
	fn a_row_behind_the_horizon_is_refused_even_with_no_closed_session_yet() {
		// Opening a session behind the horizon creates a window that is already past its close time.
		let mut open = sessions();

		assert_eq!(open.insert(at_millis(9_000), RowNumber(1), at_millis(9_500)), Insertion::Refused);
		assert!(open.sessions().is_empty());
	}

	#[test]
	fn a_row_far_after_the_newest_closed_session_opens_normally() {
		// Refusing every row after a close would freeze the group once its first session sealed.
		let mut open = seeded(&[(10_000, 1)]);
		open.close(at_millis(10_500));

		assert_eq!(
			open.insert(at_millis(11_000), RowNumber(2), at_millis(10_500)),
			Insertion::Opened(id(11_000))
		);
	}

	#[test]
	fn removing_a_member_of_a_closed_session_changes_nothing() {
		// Closed means final, so a late retraction must not reach a published session.
		let mut open = seeded(&[(10_000, 1), (12_000, 2)]);
		open.close(at_millis(11_500));

		assert_eq!(open.remove(id(10_000), RowNumber(1)).unwrap(), None);
		assert_eq!(bounds(&open), vec![(id(12_000), at_millis(12_000), at_millis(12_000), 1)]);
	}

	#[test]
	fn counter_ids_number_opened_and_split_sessions_in_order() {
		// The sdk path numbers sessions by counter, so a split must mint the next number, not a start time.
		let mut open = OpenSessions::new(
			Duration::from_milliseconds_const(1_000),
			SessionIds::Counter {
				next: 0,
			},
		);
		assert_eq!(open.insert(at_millis(10_000), RowNumber(1), far_past()), Insertion::Opened(0));
		open.insert(at_millis(10_800), RowNumber(2), far_past());
		open.insert(at_millis(11_600), RowNumber(3), far_past());

		let removal = open.remove(0, RowNumber(2)).unwrap();

		assert!(matches!(removal, Some(Removal::Split { kept: 0, moved }) if moved.id == 1));
		assert_eq!(open.insert(at_millis(20_000), RowNumber(4), far_past()), Insertion::Opened(2));
	}

	#[test]
	fn removing_a_row_the_open_session_does_not_hold_is_an_error_not_a_no_op() {
		// A silent no-op here would leave a stale member in the session after its row index went out of sync.
		let mut open = seeded(&[(10_000, 1)]);

		assert!(open.remove(id(10_000), RowNumber(99)).is_err());
		assert_eq!(bounds(&open), vec![(id(10_000), at_millis(10_000), at_millis(10_000), 1)]);
	}

	#[test]
	fn a_row_within_the_gap_after_a_closed_session_is_refused_even_ahead_of_the_horizon() {
		// Without sealed_last this row opens a session a fresh run would have merged into the closed one.
		let mut open = seeded(&[(10_000, 1)]);
		open.close(at_millis(10_500));

		assert_eq!(open.insert(at_millis(10_600), RowNumber(2), at_millis(10_500)), Insertion::Refused);
		assert!(open.sessions().is_empty());
	}

	#[test]
	fn a_late_row_behind_the_horizon_extends_an_open_session_it_reaches() {
		// The session closes on its last row, so refusing by the row's own time drops a row a fresh run keeps.
		let mut open = seeded(&[(11_300, 1)]);

		assert_eq!(
			open.insert(at_millis(11_050), RowNumber(2), at_millis(11_099)),
			Insertion::Extended(id(11_300))
		);
		assert_eq!(bounds(&open), vec![(id(11_300), at_millis(11_050), at_millis(11_300), 2)]);
	}

	#[test]
	fn a_session_opened_at_the_epoch_still_splits_off_a_row_one_gap_later() {
		// Time zero is an ordinary instant; treating it as unopened folds every later row into one session.
		let mut open = seeded(&[(0, 1)]);

		assert_eq!(open.insert(at_millis(1_000), RowNumber(2), far_past()), Insertion::Opened(id(1_000)));
		assert_eq!(
			bounds(&open),
			vec![
				(id(0), at_millis(0), at_millis(0), 1),
				(id(1_000), at_millis(1_000), at_millis(1_000), 1)
			]
		);
	}

	#[test]
	fn a_zero_gap_puts_every_distinct_instant_in_its_own_session() {
		// A zero gap is the group-by-instant case: a repeated instant joins, the next instant must not.
		let mut open = OpenSessions::new(Duration::from_milliseconds_const(0), SessionIds::StartTime);

		assert_eq!(open.insert(at_millis(5_000), RowNumber(1), far_past()), Insertion::Opened(id(5_000)));
		assert_eq!(open.insert(at_millis(5_000), RowNumber(2), far_past()), Insertion::Extended(id(5_000)));
		assert_eq!(
			open.insert(
				at_millis(5_000).saturating_add(Duration::from_nanoseconds_const(1)),
				RowNumber(3),
				far_past()
			),
			Insertion::Opened(
				at_millis(5_000).saturating_add(Duration::from_nanoseconds_const(1)).to_order()
			)
		);
	}

	#[test]
	fn a_split_whose_back_half_starts_at_the_kept_id_gets_a_distinct_id() {
		// The kept half keeps the opener's id, so minting the back half from its start can collide.
		let mut open = seeded(&[(4_639, 2), (4_000, 8), (3_400, 9)]);
		assert_eq!(bounds(&open), vec![(id(4_639), at_millis(3_400), at_millis(4_639), 3)]);

		let Some(Removal::Split {
			kept,
			moved,
		}) = open.remove(id(4_639), RowNumber(8)).unwrap()
		else {
			panic!("removing the middle row must split the session");
		};

		assert_eq!(kept, id(4_639));
		assert_ne!(moved.id, kept, "two open sessions must never share an id");
		assert_eq!(open.sessions().len(), 2);
	}
}
