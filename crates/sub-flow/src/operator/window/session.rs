// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::WindowKind;
use reifydb_flow::{
	transaction::FlowTransaction,
	window::{policy::SealPolicy, span::WindowCoord},
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration},
};

use super::operator::WindowOperator;
use crate::operator::store::OperatorStateStore;

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

	pub(super) fn session_gap_ms(&self) -> u64 {
		<DateTime as WindowCoord>::span_millis(self.session_gap()).unwrap_or(0)
	}

	pub(super) fn session_policy(&self) -> SealPolicy {
		SealPolicy::session(self.session_gap(), self.grace())
	}

	pub(super) fn load_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
	) -> Result<(u64, u64, u64)> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().load_session(&mut store, group)
	}

	pub(super) fn save_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		session_id: u64,
		last_event_time: u64,
		session_start: u64,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().save_session(&mut store, group, session_id, last_event_time, session_start)
	}
}
