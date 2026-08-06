// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	key::operator_group_state::GroupId,
};
use reifydb_flow::{
	transaction::FlowTransaction,
	window::{
		coord::{EventCoord, OrdinalCoord, RowSpan},
		driver::{gate::SealGate, mint::Mint},
		kind::{
			ordinal_window_span,
			session::{SessionKind, SessionTracker},
			sliding::{SlidingOverRows, SlidingOverTime},
			tumbling::TumblingOverRows,
		},
		ledger::FiredAt,
		policy::{SealPolicy, SealedThrough},
		span::{WindowCoord, WindowSpan},
	},
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use super::operator::WindowOperator;
use crate::operator::{aggregation::engine::partition_group_key, store::OperatorStateStore};

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

	pub(super) fn session_policy(&self) -> SealPolicy {
		self.session_kind().seal_policy(self.grace())
	}

	pub(super) fn load_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
	) -> Result<SessionTracker> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		self.meta_slot().load_session(&mut store, group)
	}

	pub(super) fn save_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		tracker: &SessionTracker,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		self.meta_slot().save_session(&mut store, group, tracker)
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
			let instant = <DateTime as WindowCoord>::from_order(timestamp_or_row_index);
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

	pub(super) fn partition_group(&self, txn: &mut FlowTransaction, partition: Hash128) -> Result<GroupId> {
		let (group, _) = txn.intern_group(self.core.operator, &partition_group_key(partition))?;
		Ok(group)
	}

	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		Mint::new(self.meta_slot()).record_membership(&mut store, group, row_number, window_id)
	}

	pub(super) fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		Mint::new(self.meta_slot()).membership(&mut store, group, row_number)
	}

	pub(super) fn drop_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		Mint::new(self.meta_slot()).drop_membership(&mut store, group, row_number)
	}

	pub fn get_and_increment_global_count(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
	) -> Result<OrdinalCoord> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		Mint::new(self.meta_slot()).ordinal(&mut store, group)
	}

	pub(super) fn seal_ledger(&self, txn: &mut FlowTransaction) -> Result<SealedThrough> {
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		Ok(SealedThrough::from_order(self.meta_slot().seal_ledger(&mut store)?))
	}

	pub(super) fn advance_seal_ledger(&self, txn: &mut FlowTransaction, fired: FiredAt) -> Result<()> {
		let mut store = OperatorStateStore::new(txn, self.core.operator);
		self.meta_slot().advance_seal_ledger(&mut store, fired.at().to_order())
	}

	pub(super) fn seal_gate(&self, txn: &mut FlowTransaction, policy: SealPolicy) -> Result<SealGate> {
		let watermark = txn.flow_watermark();
		let ledger = self.seal_ledger(txn)?;
		Ok(SealGate::new(policy, Some(ledger), watermark))
	}
}
