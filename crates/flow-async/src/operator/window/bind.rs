// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	key::operator::state::GroupId,
	state::timer::StateStore,
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use super::operator::WindowOperator;
use crate::{
	operator::{
		host::HostContext,
		state::seal::{
			coord::Coord,
			gate::SealGate,
			ledger::FiredAt,
			rule::{SealRule, SealedThrough},
		},
	},
	window::{
		coord::{EventCoord, OrdinalCoord, RowSpan},
		kind::{
			ordinal_window_span,
			session::{SessionKind, SessionTracker},
			sliding::{SlidingOverRows, SlidingOverTime},
			tumbling::TumblingOverRows,
		},
		mint::Mint,
		span::WindowSpan,
	},
};

pub(super) struct SlidingBound {
	over_time: Option<SlidingOverTime>,
	over_rows: Option<SlidingOverRows>,
	count_based: bool,
}

impl SlidingBound {
	pub(super) fn anchors(&self, timestamp_or_row_index: u64) -> impl Iterator<Item = u64> + use<> {
		let over_time = self.over_time.as_ref().map(|kind| {
			kind.anchors(EventCoord::of(&<DateTime as Coord>::from_order(timestamp_or_row_index)))
		});
		let over_rows = match over_time {
			Some(_) => None,
			None => self
				.over_rows
				.as_ref()
				.map(|kind| kind.anchors(OrdinalCoord::from_arrival_counter(timestamp_or_row_index))),
		};
		let neither = (over_time.is_none() && over_rows.is_none()).then_some(0);
		over_time.into_iter().flatten().chain(over_rows.into_iter().flatten()).chain(neither)
	}

	pub(super) fn span(&self, anchor: u64) -> WindowSpan<DateTime> {
		if self.count_based {
			return ordinal_window_span(anchor);
		}
		self.over_time.as_ref().map_or_else(
			|| TumblingOverRows::holding(RowSpan::of(1)).span(OrdinalCoord::from_arrival_counter(anchor)),
			|kind| kind.span(anchor),
		)
	}
}

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

	pub(super) fn session_kind(&self) -> SessionKind {
		SessionKind::with_gap(self.session_gap())
	}

	pub(super) fn session_rule(&self) -> SealRule {
		self.session_kind().seal_rule(self.lateness().unwrap_or_else(Duration::zero))
	}

	pub(super) fn load_session_tracker(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
	) -> Result<SessionTracker> {
		let group = self.partition_group(group_hash);
		self.meta_slot().load_session(store, group)
	}

	pub(super) fn stored_session_trackers(
		&mut self,
		host: &mut dyn HostContext,
		group_hashes: &[Hash128],
	) -> Result<Vec<Option<SessionTracker>>> {
		let groups: Vec<GroupId> = group_hashes.iter().map(|hash| self.partition_group(*hash)).collect();
		self.meta_slot().stored_sessions(host, &groups)
	}

	pub(super) fn drop_session_trackers(
		&mut self,
		host: &mut dyn HostContext,
		group_hashes: &[Hash128],
	) -> Result<()> {
		let groups: Vec<GroupId> = group_hashes.iter().map(|hash| self.partition_group(*hash)).collect();
		self.meta_slot().drop_sessions(host, &groups)
	}

	pub(super) fn save_session_tracker(
		&mut self,
		host: &mut dyn HostContext,
		group_hash: Hash128,
		tracker: &SessionTracker,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		self.meta_slot().save_session(host, group, tracker)
	}

	fn sliding_over_time(&self) -> Option<SlidingOverTime> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Duration(size),
				slide: WindowSize::Duration(slide),
				..
			} => SlidingOverTime::by_duration(*size, *slide),
			_ => None,
		}
	}

	fn sliding_over_rows(&self) -> Option<SlidingOverRows> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Count(size),
				slide: WindowSize::Count(slide),
				..
			} => SlidingOverRows::by_count(RowSpan::of(*size), RowSpan::of(*slide)),
			_ => None,
		}
	}

	pub(super) fn sliding_bound(&self) -> SlidingBound {
		SlidingBound {
			over_time: self.sliding_over_time(),
			over_rows: self.sliding_over_rows(),
			count_based: self.is_count_based(),
		}
	}

	pub(super) fn partition_group(&self, partition: Hash128) -> GroupId {
		GroupId::hashed(partition)
	}

	pub fn store_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).record_membership(store, group, row_number, window_id)
	}

	pub(super) fn store_row_indexes(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		row_number: RowNumber,
		window_ids: &[u64],
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).record_memberships(store, group, row_number, window_ids)
	}

	pub(super) fn withdraw_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).withdraw_membership(store, group, row_number, window_id)
	}

	pub(super) fn lookup_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).membership(store, group, row_number)
	}

	pub(super) fn drop_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).drop_membership(store, group, row_number)
	}

	pub(super) fn store_session_member(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		session_id: u64,
		row_number: RowNumber,
	) -> Result<()> {
		self.meta_slot().store_session_member(store, GroupId::window(group_hash, session_id), row_number)
	}

	pub(super) fn drop_session_member(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
		session_id: u64,
		row_number: RowNumber,
	) -> Result<()> {
		self.meta_slot().drop_session_member(store, GroupId::window(group_hash, session_id), row_number)
	}

	pub fn get_and_increment_global_count(
		&mut self,
		store: &mut dyn StateStore,
		group_hash: Hash128,
	) -> Result<OrdinalCoord> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).ordinal(store, group)
	}

	pub(super) fn seal_ledger(&mut self, host: &mut dyn HostContext) -> Result<SealedThrough> {
		Ok(SealedThrough::from_order(self.meta_slot().seal_ledger(host)?))
	}

	pub(super) fn advance_seal_ledger(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<()> {
		self.meta_slot().advance_seal_ledger(host, fired.at().to_order())
	}

	pub(super) fn seal_gate(&mut self, host: &mut dyn HostContext, rule: SealRule) -> Result<SealGate> {
		let watermark = host.flow_watermark()?;
		let ledger = self.seal_ledger(host)?;
		Ok(SealGate::new(rule, Some(ledger), watermark))
	}
}
