// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	key::operator::state::GroupId,
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
		host: &mut dyn HostContext,
		group_hash: Hash128,
	) -> Result<SessionTracker> {
		let group = self.partition_group(group_hash);
		self.meta_slot().load_session(host, group)
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

	pub fn sliding_window_anchors(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		if let Some(kind) = self.sliding_over_time() {
			let instant = <DateTime as Coord>::from_order(timestamp_or_row_index);
			return kind.anchors(EventCoord::of(&instant));
		}
		if let Some(kind) = self.sliding_over_rows() {
			return kind.anchors(OrdinalCoord::from_arrival_counter(timestamp_or_row_index));
		}
		vec![0]
	}

	pub(super) fn sliding_window_span(&self, anchor: u64) -> WindowSpan<DateTime> {
		if self.is_count_based() {
			return ordinal_window_span(anchor);
		}
		self.sliding_over_time().map_or_else(
			|| TumblingOverRows::holding(RowSpan::of(1)).span(OrdinalCoord::from_arrival_counter(anchor)),
			|kind| kind.span(anchor),
		)
	}

	pub(super) fn partition_group(&self, partition: Hash128) -> GroupId {
		GroupId::hashed(partition)
	}

	pub fn store_row_index(
		&mut self,
		host: &mut dyn HostContext,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).record_membership(host, group, row_number, window_id)
	}

	pub(super) fn lookup_row_index(
		&mut self,
		host: &mut dyn HostContext,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).membership(host, group, row_number)
	}

	pub(super) fn drop_row_index(
		&mut self,
		host: &mut dyn HostContext,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<()> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).drop_membership(host, group, row_number)
	}

	pub fn get_and_increment_global_count(
		&mut self,
		host: &mut dyn HostContext,
		group_hash: Hash128,
	) -> Result<OrdinalCoord> {
		let group = self.partition_group(group_hash);
		Mint::new(self.meta_slot()).ordinal(host, group)
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
