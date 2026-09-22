// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::state::{OperatorState, StateCodec},
};
use reifydb_core::{key::operator::state::GroupId, state::timer::StateStore};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::{
	operator::{
		state::seal::coord::Coord,
		state_access::{get_classified, put, remove},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			BufferKey, GroupMeta, KeyspaceFamily, RunningKey,
			config::WindowEngineConfig,
			tumbling::{TumblingEngine, TumblingIndexEntry},
		},
		kind::session::SessionAssignment,
		meta::SessionState,
		span::WindowAnchor,
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestSession<S> {
	pub session_id: u64,
	pub start: S,
	pub last: S,
	opened: bool,
}

impl<S: Coord> GuestSession<S> {
	fn unopened() -> Self {
		Self {
			session_id: 0,
			start: S::from_order(0),
			last: S::from_order(0),
			opened: false,
		}
	}

	fn adopt(&mut self, coord: S) {
		self.start = coord;
		self.last = coord;
		self.opened = true;
	}

	fn extend(&mut self, coord: S) {
		self.start = self.start.min(coord);
		self.last = self.last.max(coord);
	}
}

fn session_state_key(group: GroupId) -> RunningKey {
	RunningKey::new(KeyspaceFamily::Guest, group, EncodedKey::new(Vec::new()))
}

fn row_index_key(session: GroupId, row: RowNumber) -> BufferKey {
	BufferKey::of_row(KeyspaceFamily::Guest, session, row)
}

pub struct SessionEngine<G, S: Coord, Accumulator> {
	tumbling: TumblingEngine<G, S, Accumulator>,
	gap: S::Span,
	refused: u64,
}

impl<G, S, Accumulator> SessionEngine<G, S, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
	G: StateCodec,
	GroupMeta<S>: OperatorState,
	TumblingIndexEntry<G, S>: OperatorState,
{
	pub fn new(config: WindowEngineConfig, gap: S::Span) -> Self {
		Self {
			tumbling: TumblingEngine::new(config),
			gap,
			refused: 0,
		}
	}

	pub fn gap(&self) -> S::Span {
		self.gap
	}

	pub fn assign(&mut self, tracker: &mut GuestSession<S>, coord: S) -> SessionAssignment {
		if !tracker.opened {
			tracker.adopt(coord);
			return SessionAssignment::Opened(tracker.session_id);
		}
		if coord > tracker.last && coord.span_since(tracker.last) > self.gap {
			let closed = tracker.session_id;
			tracker.session_id += 1;
			tracker.adopt(coord);
			return SessionAssignment::Rotated {
				closed,
				opened: tracker.session_id,
			};
		}
		if self.refuses(tracker, coord) {
			self.refused += 1;
			return SessionAssignment::Refused;
		}
		tracker.extend(coord);
		SessionAssignment::Extended(tracker.session_id)
	}

	pub fn refuses(&self, tracker: &GuestSession<S>, coord: S) -> bool {
		coord < tracker.start && tracker.start.span_since(coord) > self.gap
	}

	pub fn take_refused(&mut self) -> u64 {
		std::mem::take(&mut self.refused)
	}

	pub fn tumbling_mut(&mut self) -> &mut TumblingEngine<G, S, Accumulator> {
		&mut self.tumbling
	}

	pub fn load_tracker(&self, store: &mut dyn StateStore, partition: GroupId) -> Result<GuestSession<S>> {
		let Some(state) = get_classified::<_, SessionState>(store, &session_state_key(partition))? else {
			return Ok(GuestSession::unopened());
		};
		Ok(GuestSession {
			session_id: state.session_id,
			start: S::from_order(state.session_start),
			last: S::from_order(state.last_event_time),
			opened: true,
		})
	}

	pub fn save_tracker(
		&self,
		store: &mut dyn StateStore,
		partition: GroupId,
		tracker: &GuestSession<S>,
	) -> Result<()> {
		put(
			store,
			&session_state_key(partition),
			SessionState {
				session_id: tracker.session_id,
				last_event_time: tracker.last.to_order(),
				session_start: tracker.start.to_order(),
			},
		)
	}

	pub fn load_record(&self, store: &mut dyn StateStore, session: GroupId) -> Result<Option<(S, S)>> {
		Ok(get_classified::<_, SessionState>(store, &session_state_key(session))?
			.map(|state| (S::from_order(state.session_start), S::from_order(state.last_event_time))))
	}

	pub fn save_record(&self, store: &mut dyn StateStore, session: GroupId, start: S, last: S) -> Result<()> {
		put(
			store,
			&session_state_key(session),
			SessionState {
				session_id: 0,
				last_event_time: last.to_order(),
				session_start: start.to_order(),
			},
		)
	}

	pub fn index_row(&self, store: &mut dyn StateStore, id: u64, session: GroupId, row: RowNumber) -> Result<()> {
		put(store, &row_index_key(session, row), id)
	}

	pub fn holds_row(&self, store: &mut dyn StateStore, session: GroupId, row: RowNumber) -> Result<bool> {
		Ok(get_classified::<_, u64>(store, &row_index_key(session, row))?.is_some())
	}

	pub fn unindex_row(&self, store: &mut dyn StateStore, session: GroupId, row: RowNumber) -> Result<()> {
		remove(store, &row_index_key(session, row))
	}

	#[allow(clippy::too_many_arguments)]
	pub fn reindex_session(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		id: u64,
		session: GroupId,
		slot_key: &EncodedKey,
		prior_last: Option<S>,
		last: S,
	) -> Result<()> {
		self.tumbling.reindex_window(
			store,
			group,
			S::from_order(id),
			session,
			slot_key,
			prior_last.map(Coord::to_order),
			Some(last.to_order()),
		)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator::state::GroupId;
	use reifydb_value::{
		factory::time::at_millis,
		value::{duration::Duration, row_number::RowNumber},
	};

	use super::{GuestSession, SessionEngine};
	use crate::{
		operator::state::mock::MockStore,
		window::{
			accumulator::mock::SumAccumulator, engine::config::WindowEngineConfig,
			kind::session::SessionAssignment,
		},
	};

	fn group_id(name: &str) -> GroupId {
		GroupId::of(&EncodedKey::new(name.as_bytes().to_vec()))
	}

	type Engine = SessionEngine<u32, reifydb_value::value::datetime::DateTime, SumAccumulator>;

	fn engine(gap_ms: i64) -> Engine {
		SessionEngine::new(WindowEngineConfig::builder().build(), Duration::from_milliseconds_const(gap_ms))
	}

	#[test]
	fn a_session_assignment_opens_extends_rotates_and_refuses_like_the_native_rule() {
		// A guest rule that drifts from the native one files the same rows into different sessions than RQL.
		let mut engine = engine(1_000);
		let mut tracker = GuestSession::unopened();

		assert_eq!(engine.assign(&mut tracker, at_millis(5_000)), SessionAssignment::Opened(0));
		assert_eq!(engine.assign(&mut tracker, at_millis(6_000)), SessionAssignment::Extended(0));
		assert_eq!(
			engine.assign(&mut tracker, at_millis(7_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1,
			}
		);
		assert_eq!(engine.assign(&mut tracker, at_millis(5_000)), SessionAssignment::Refused);
		assert_eq!((tracker.start, tracker.last), (at_millis(7_001), at_millis(7_001)));
		assert_eq!(engine.assign(&mut tracker, at_millis(6_500)), SessionAssignment::Extended(1));
		assert_eq!(
			(tracker.start, tracker.last),
			(at_millis(6_500), at_millis(7_001)),
			"reaching backwards must not drag the high end down"
		);

		let mut epoch = GuestSession::unopened();
		assert_eq!(engine.assign(&mut epoch, at_millis(0)), SessionAssignment::Opened(0));
		assert_eq!(
			engine.assign(&mut epoch, at_millis(1_001)),
			SessionAssignment::Rotated {
				closed: 0,
				opened: 1,
			},
			"a session opened at the epoch must still rotate across its gap"
		);
	}

	#[test]
	fn a_refused_row_is_counted_once_and_the_count_resets_per_batch() {
		// An uncounted refusal loses a row with no trace; a count that never resets reports old drops again.
		let mut engine = engine(10);
		let mut tracker = GuestSession::unopened();
		engine.assign(&mut tracker, at_millis(100));

		assert_eq!(engine.assign(&mut tracker, at_millis(89)), SessionAssignment::Refused);
		assert_eq!(engine.assign(&mut tracker, at_millis(90)), SessionAssignment::Extended(0));
		assert_eq!(engine.take_refused(), 1);
		assert_eq!(engine.take_refused(), 0);
	}

	#[test]
	fn a_saved_tracker_resumes_its_session_and_an_absent_one_opens_fresh() {
		// A tracker that loses its id or swaps start and last files the next row into the wrong session.
		let mut engine = engine(10);
		let mut store = MockStore::default();
		let partition = group_id("BTC");

		let mut fresh = engine.load_tracker(&mut store, partition).unwrap();
		assert_eq!(engine.assign(&mut fresh, at_millis(0)), SessionAssignment::Opened(0));

		let mut tracker = engine.load_tracker(&mut store, partition).unwrap();
		engine.assign(&mut tracker, at_millis(100));
		engine.assign(&mut tracker, at_millis(111));
		engine.assign(&mut tracker, at_millis(105));
		engine.save_tracker(&mut store, partition, &tracker).unwrap();

		let resumed = engine.load_tracker(&mut store, partition).unwrap();
		assert_eq!(resumed, tracker);
		assert_eq!((resumed.session_id, resumed.start, resumed.last), (1, at_millis(105), at_millis(111)));
		assert_eq!(
			engine.load_tracker(&mut store, group_id("ETH")).unwrap(),
			GuestSession::unopened(),
			"another group must not see this group's tracker"
		);
	}

	#[test]
	fn a_session_record_and_its_row_index_live_under_that_session_only() {
		// A record or index entry visible from another session sends a remove to the wrong session.
		let engine = engine(10);
		let mut store = MockStore::default();
		let (first, second) = (group_id("s0"), group_id("s1"));

		assert_eq!(engine.load_record(&mut store, first).unwrap(), None);
		engine.save_record(&mut store, first, at_millis(95), at_millis(107)).unwrap();
		assert_eq!(engine.load_record(&mut store, first).unwrap(), Some((at_millis(95), at_millis(107))));
		assert_eq!(engine.load_record(&mut store, second).unwrap(), None);

		engine.index_row(&mut store, 0, first, RowNumber(2)).unwrap();
		assert!(engine.holds_row(&mut store, first, RowNumber(2)).unwrap());
		assert!(!engine.holds_row(&mut store, second, RowNumber(2)).unwrap());
		assert!(!engine.holds_row(&mut store, first, RowNumber(3)).unwrap());
		engine.unindex_row(&mut store, first, RowNumber(2)).unwrap();
		assert!(!engine.holds_row(&mut store, first, RowNumber(2)).unwrap());
	}

	#[test]
	fn extending_a_session_keeps_one_expiry_row_at_its_new_last() {
		// A stale anchor seals the session at an old last, and a second row trips the one-row-per-window check.
		let mut engine = engine(10);
		let mut store = MockStore::default();
		let (session, slot) = (group_id("s0"), EncodedKey::new(Vec::new()));

		engine.reindex_session(&mut store, &1u32, 0, session, &slot, None, at_millis(100)).unwrap();
		engine.reindex_session(&mut store, &1u32, 0, session, &slot, Some(at_millis(100)), at_millis(105))
			.unwrap();
		engine.reindex_session(&mut store, &1u32, 0, session, &slot, Some(at_millis(105)), at_millis(107))
			.unwrap();

		assert!(
			engine.tumbling_mut().expire(&mut store, at_millis(106).to_order()).unwrap().is_empty(),
			"no anchor may remain below 107"
		);
		let due = engine.tumbling_mut().expire(&mut store, at_millis(107).to_order()).unwrap();
		assert_eq!(due.len(), 1, "exactly one expiry row per session");
		assert_eq!(due[0].group_id, session);
	}
}
