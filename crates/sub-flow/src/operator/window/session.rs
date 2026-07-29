// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::WindowKind;
use reifydb_flow::{
	transaction::FlowTransaction,
	window::{
		kind::session::{SessionKind, SessionTracker},
		policy::SealPolicy,
	},
};
use reifydb_value::{Result, util::hash::Hash128, value::duration::Duration};

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
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().load_session(&mut store, group)
	}

	pub(super) fn save_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		tracker: &SessionTracker,
	) -> Result<()> {
		let group = self.partition_group(txn, group_hash)?;
		let mut store = OperatorStateStore::new(txn, self.core.node);
		self.aux_slot().save_session(&mut store, group, tracker)
	}
}
