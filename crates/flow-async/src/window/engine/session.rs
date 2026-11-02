// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash, mem::size_of, slice::from_ref};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::state::{OperatorState, StateCodec, decode_body, encode},
};
use reifydb_core::{
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	operator::{
		state::seal::coord::Coord,
		state_access::{get_classified, remove, set},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			BufferKey, GroupMeta, KeyspaceFamily, RunningKey,
			config::WindowEngineConfig,
			tumbling::{TumblingEngine, TumblingIndexEntry},
		},
		kind::session::open::{Member, OpenSession, OpenSessions, SessionIds},
		meta::SessionState,
		span::WindowAnchor,
	},
};

#[operator_state]
#[derive(Clone, Default)]
pub struct GuestOpenSessionsState {
	pub next_id: u64,
	pub sessions: Vec<(u64, Vec<(u64, u64)>)>,
	pub sealed_last: Option<u64>,
}

impl HeapSize for GuestOpenSessionsState {
	fn heap_size(&self) -> usize {
		self.sessions.iter().map(|(_, members)| size_of::<u64>() + members.len() * 2 * size_of::<u64>()).sum()
	}
}

fn session_state_key(group: GroupId) -> RunningKey {
	RunningKey::new(KeyspaceFamily::Guest, group, EncodedKey::new(Vec::new()))
}

fn record_of<S: Coord>(state: SessionState) -> (S, S) {
	(S::from_order(state.session_start), S::from_order(state.last_event_time))
}

fn member_key(session: GroupId, row: RowNumber) -> GroupStateKey {
	(&BufferKey::of_row(KeyspaceFamily::Guest, session, row)).into_group_state_key()
}

fn open_sessions_of<S: Coord>(gap: S::Span, state: Option<GuestOpenSessionsState>) -> OpenSessions<S> {
	let Some(state) = state else {
		return OpenSessions::new(
			gap,
			SessionIds::Counter {
				next: 0,
			},
		);
	};
	let sessions = state
		.sessions
		.into_iter()
		.map(|(id, members)| {
			let members: Vec<Member<S>> = members
				.into_iter()
				.map(|(at, row)| Member {
					at: S::from_order(at),
					row: RowNumber(row),
				})
				.collect();
			OpenSession {
				id,
				start: members[0].at,
				last: members[members.len() - 1].at,
				members,
			}
		})
		.collect();
	OpenSessions::resumed(
		gap,
		SessionIds::Counter {
			next: state.next_id,
		},
		sessions,
		state.sealed_last.map(S::from_order),
	)
}

fn state_of<S: Coord>(open: &OpenSessions<S>) -> GuestOpenSessionsState {
	let SessionIds::Counter {
		next,
	} = open.ids()
	else {
		panic!("guest sessions are numbered by counter, so their ids must carry a next value");
	};
	GuestOpenSessionsState {
		next_id: next,
		sessions: open
			.sessions()
			.iter()
			.map(|session| {
				(session.id, session.members.iter().map(|m| (m.at.to_order(), m.row.0)).collect())
			})
			.collect(),
		sealed_last: open.sealed_last().map(Coord::to_order),
	}
}

pub struct SessionEngine<G, S: Coord, Accumulator> {
	tumbling: TumblingEngine<G, S, Accumulator>,
	gap: S::Span,
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
		}
	}

	pub fn gap(&self) -> S::Span {
		self.gap
	}

	pub fn tumbling_mut(&mut self) -> &mut TumblingEngine<G, S, Accumulator> {
		&mut self.tumbling
	}

	pub fn load_open(&self, store: &mut dyn StateStore, partitions: &[GroupId]) -> Result<Vec<OpenSessions<S>>> {
		let keys: Vec<RunningKey> = partitions.iter().map(|partition| session_state_key(*partition)).collect();
		Ok(get_classified::<_, GuestOpenSessionsState>(store, &keys)?
			.into_iter()
			.map(|state| open_sessions_of(self.gap, state))
			.collect())
	}

	pub fn save_open(
		&self,
		store: &mut dyn StateStore,
		saves: &[(GroupId, &OpenSessions<S>)],
		drops: &[GroupId],
	) -> Result<()> {
		if !saves.is_empty() {
			set(
				store,
				saves.iter()
					.map(|(partition, open)| (session_state_key(*partition), state_of(open)))
					.collect(),
			)?;
		}
		if !drops.is_empty() {
			let keys: Vec<RunningKey> =
				drops.iter().map(|partition| session_state_key(*partition)).collect();
			remove(store, &keys)?;
		}
		Ok(())
	}

	pub fn load_record(&self, store: &mut dyn StateStore, session: GroupId) -> Result<Option<(S, S)>> {
		Ok(get_classified::<_, SessionState>(store, from_ref(&session_state_key(session)))?
			.remove(0)
			.map(record_of))
	}

	pub fn load_records(
		&self,
		store: &mut dyn StateStore,
		sessions: &[GroupId],
	) -> Result<Vec<Option<(DateTime, S, S)>>> {
		let keys: Vec<RunningKey> = sessions.iter().map(|session| session_state_key(*session)).collect();
		Ok(get_classified::<_, SessionState>(store, &keys)?
			.into_iter()
			.map(|state| {
				state.map(|state| {
					let first = state.first_time;
					let (start, last) = record_of(state);
					(first, start, last)
				})
			})
			.collect())
	}

	pub fn save_records(
		&self,
		store: &mut dyn StateStore,
		records: &[(GroupId, DateTime, S, S)],
		drops: &[GroupId],
	) -> Result<()> {
		if !records.is_empty() {
			set(
				store,
				records.iter()
					.map(|(session, first, start, last)| {
						(
							session_state_key(*session),
							SessionState {
								session_id: 0,
								last_event_time: last.to_order(),
								session_start: start.to_order(),
								first_time: *first,
							},
						)
					})
					.collect(),
			)?;
		}
		if !drops.is_empty() {
			let keys: Vec<RunningKey> = drops.iter().map(|session| session_state_key(*session)).collect();
			remove(store, &keys)?;
		}
		Ok(())
	}

	pub fn store_member<C: StateCodec>(
		&self,
		store: &mut dyn StateStore,
		session: GroupId,
		row: RowNumber,
		contribution: &C,
	) -> Result<()> {
		store.state_set(vec![(member_key(session, row), encode(contribution)?)])
	}

	pub fn load_member<C: StateCodec>(
		&self,
		store: &mut dyn StateStore,
		session: GroupId,
		row: RowNumber,
	) -> Result<C> {
		match store.state_get(from_ref(&member_key(session, row)))?.pop().flatten() {
			Some(encoded) => Ok(decode_body::<C>(&encoded)?),
			None => internal_err!(
				"session member {:?} of {:?} has no stored contribution; the open sessions and the member \
				 rows disagree",
				row,
				session
			),
		}
	}

	pub fn drop_member(&self, store: &mut dyn StateStore, session: GroupId, row: RowNumber) -> Result<()> {
		store.state_remove(from_ref(&member_key(session, row)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator::state::GroupId;
	use reifydb_value::{
		factory::time::at_millis,
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::SessionEngine;
	use crate::{
		operator::state::{mock::MockStore, seal::coord::Coord},
		window::{
			accumulator::mock::SumAccumulator,
			engine::{WindowReindex, config::WindowEngineConfig},
			kind::session::open::{Insertion, SessionIds},
		},
	};

	fn group_id(name: &str) -> GroupId {
		GroupId::of(&EncodedKey::new(name.as_bytes()))
	}

	type Engine = SessionEngine<u32, DateTime, SumAccumulator>;

	fn engine(gap_ms: i64) -> Engine {
		SessionEngine::new(WindowEngineConfig::builder().build(), Duration::from_milliseconds_const(gap_ms))
	}

	#[test]
	fn saved_open_sessions_resume_exactly_and_stay_invisible_to_other_groups() {
		// A state losing its counter, members or seal line files the next row into the wrong session.
		let engine = engine(10);
		let mut store = MockStore::default();
		let (partition, other) = (group_id("BTC"), group_id("ETH"));

		let mut open = engine.load_open(&mut store, &[partition]).unwrap().remove(0);
		assert_eq!(open.insert(at_millis(100), RowNumber(1), at_millis(0)), Insertion::Opened(0));
		assert_eq!(open.insert(at_millis(105), RowNumber(2), at_millis(0)), Insertion::Extended(0));
		assert_eq!(open.insert(at_millis(130), RowNumber(3), at_millis(0)), Insertion::Opened(1));
		open.close(at_millis(110));
		engine.save_open(&mut store, &[(partition, &open)], &[]).unwrap();

		let resumed = engine.load_open(&mut store, &[partition, other]).unwrap();
		assert_eq!(resumed[0], open);
		assert_eq!(
			resumed[0].ids(),
			SessionIds::Counter {
				next: 2
			}
		);
		assert_eq!(resumed[0].sealed_last(), Some(at_millis(105)));
		assert!(resumed[1].sessions().is_empty(), "another group must not see this group's sessions");

		engine.save_open(&mut store, &[], &[partition]).unwrap();
		assert!(engine.load_open(&mut store, &[partition]).unwrap()[0].sessions().is_empty());
	}

	#[test]
	fn a_session_record_and_its_members_live_under_that_session_only() {
		// A record or member visible from another session sends a remove to the wrong session.
		let engine = engine(10);
		let mut store = MockStore::default();
		let (first, second) = (group_id("s0"), group_id("s1"));

		assert_eq!(engine.load_record(&mut store, first).unwrap(), None);
		engine.save_records(&mut store, &[(first, at_millis(99), at_millis(95), at_millis(107))], &[]).unwrap();
		assert_eq!(engine.load_record(&mut store, first).unwrap(), Some((at_millis(95), at_millis(107))));
		assert_eq!(
			engine.load_records(&mut store, &[first, second]).unwrap(),
			vec![Some((at_millis(99), at_millis(95), at_millis(107))), None]
		);
		engine.save_records(&mut store, &[], &[first]).unwrap();
		assert_eq!(engine.load_record(&mut store, first).unwrap(), None);

		engine.store_member(&mut store, first, RowNumber(2), &41i64).unwrap();
		assert_eq!(engine.load_member::<i64>(&mut store, first, RowNumber(2)).unwrap(), 41);
		assert!(engine.load_member::<i64>(&mut store, second, RowNumber(2)).is_err());
		engine.drop_member(&mut store, first, RowNumber(2)).unwrap();
		assert!(engine.load_member::<i64>(&mut store, first, RowNumber(2)).is_err());
	}

	#[test]
	fn extending_a_session_keeps_one_expiry_row_at_its_new_last() {
		// A stale anchor seals the session at an old last, and a second row trips the one-row-per-window check.
		let mut engine = engine(10);
		let mut store = MockStore::default();
		let (session, slot) = (group_id("s0"), EncodedKey::new(Vec::new()));

		for (prior, last) in [(None, 100), (Some(100), 105), (Some(105), 107)] {
			engine.tumbling_mut()
				.reindex_windows(
					&mut store,
					&[WindowReindex {
						group: &1u32,
						window_start: DateTime::from_order(0),
						id: session,
						slot_key: &slot,
						prior: prior.map(|ms| at_millis(ms).to_order()),
						new: Some(at_millis(last).to_order()),
					}],
				)
				.unwrap();
		}

		assert!(
			engine.tumbling_mut().expire(&mut store, at_millis(106).to_order()).unwrap().is_empty(),
			"no anchor may remain below 107"
		);
		let due = engine.tumbling_mut().expire(&mut store, at_millis(107).to_order()).unwrap();
		assert_eq!(due.len(), 1, "exactly one expiry row per session");
		assert_eq!(due[0].group_id, session);
	}
}
