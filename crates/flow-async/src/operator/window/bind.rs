// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use reifydb_core::{common::WindowKind, key::operator::state::GroupId, state::timer::StateStore};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{duration::Duration, row_number::RowNumber},
};

use super::operator::WindowOperator;
use crate::{
	operator::{
		host::HostContext,
		state::seal::{coord::Coord, ledger::FiredAt, rule::SealedThrough},
	},
	window::{
		coord::DateTimeCoord,
		kind::session::open::{Member, OpenSession, OpenSessions, SessionIds},
		meta::OpenSessionsState,
	},
};

impl WindowOperator {
	pub(super) fn session_gap(&self) -> Duration {
		match &self.kind {
			WindowKind::Session {
				gap,
				..
			} => *gap,
			_ => Duration::default(),
		}
	}

	pub(super) fn load_open_sessions(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
	) -> Result<OpenSessions<DateTimeCoord>> {
		Ok(self.load_open_sessions_of(store, from_ref(&group_hash))?.remove(0))
	}

	pub(super) fn load_open_sessions_of(
		&mut self,
		store: &mut dyn StateStore,
		group_hashes: &[Hash128],
	) -> Result<Vec<OpenSessions<DateTimeCoord>>> {
		let gap = self.session_gap();
		let groups: Vec<GroupId> = group_hashes.iter().map(|hash| self.partition_group(*hash)).collect();
		let states = self.meta_slot().load_open_sessions_of(store, &groups)?;
		Ok(states.into_iter().map(|state| open_sessions_of(gap, state)).collect())
	}

	pub(super) fn save_open_sessions(
		&mut self,
		store: &mut dyn StateStore,
		groups: &[(Hash128, &OpenSessions<DateTimeCoord>)],
	) -> Result<()> {
		let states = groups
			.iter()
			.map(|(hash, open)| {
				let state = OpenSessionsState {
					sessions: open
						.sessions()
						.iter()
						.map(|session| {
							(
								session.id,
								session.members
									.iter()
									.map(|m| (m.at.to_order(), m.row.0))
									.collect(),
							)
						})
						.collect(),
					sealed_last: open.sealed_last().map(|at| at.to_order()),
				};
				(self.partition_group(*hash), state)
			})
			.collect();
		self.meta_slot().save_open_sessions(store, states)
	}

	pub(super) fn partition_group(&self, partition: Hash128) -> GroupId {
		GroupId::hashed(partition)
	}

	pub(super) fn seal_ledger(&mut self, host: &mut dyn HostContext) -> Result<SealedThrough> {
		Ok(SealedThrough::from_order(self.meta_slot().seal_ledger(host)?))
	}

	pub(super) fn advance_seal_ledger(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<()> {
		self.meta_slot().advance_seal_ledger(host, fired.at().to_order())
	}
}

fn open_sessions_of(gap: Duration, state: Option<OpenSessionsState>) -> OpenSessions<DateTimeCoord> {
	let Some(state) = state else {
		return OpenSessions::new(gap, SessionIds::StartTime);
	};
	let sessions = state
		.sessions
		.into_iter()
		.map(|(id, members)| {
			let members: Vec<Member<DateTimeCoord>> = members
				.into_iter()
				.map(|(at, row)| Member {
					at: <DateTimeCoord as Coord>::from_order(at),
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
		SessionIds::StartTime,
		sessions,
		state.sealed_last.map(<DateTimeCoord as Coord>::from_order),
	)
}
