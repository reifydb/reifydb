// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::encoded::row::{EncodedRow, SHAPE_HEADER_SIZE};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::{
		Key,
		kind::KeyKind,
		operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey},
	},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_flow::transaction::{
	ChangeCoordinate, DeferredParams, FlowTransaction,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	util::cowvec::CowVec,
	value::{datetime::DateTime, identity::IdentityId},
};

pub const OPERATOR_ID: OperatorId = OperatorId(1);

pub fn make_row(payload: &str, created_at: u64, updated_at: u64) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	EncodedRow(CowVec::new(buf))
}

pub fn key(s: &str) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::FIRST_CUSTOM, s.as_bytes())
}

pub fn engine() -> TestEngine {
	TestEngine::new()
}

pub fn payload(stored: &EncodedRow) -> &[u8] {
	&stored.0[SHAPE_HEADER_SIZE..]
}

pub struct FlowTxnBuilder<'a> {
	engine: &'a TestEngine,
	version: CommitVersion,
	clock: Clock,
	catalog: Catalog,
}

impl<'a> FlowTxnBuilder<'a> {
	pub fn at(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn clock_millis(mut self, millis: u64) -> Self {
		self.clock = Clock::Mock(MockClock::from_millis(millis));
		self
	}

	pub fn catalog(mut self, catalog: Catalog) -> Self {
		self.catalog = catalog;
		self
	}

	pub fn deferred(self) -> FlowTransaction {
		let version = self.version;
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query: self.engine.multi().begin_query().unwrap(),
			state_query: self.engine.multi().begin_query().unwrap(),
			single: self.engine.inner().single().clone(),
			catalog: self.catalog,
			interceptors: Interceptors::new(),
			clock: self.clock,
			substrate: FlowSubstrate {
				operators: self.engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
			state_budget: OperatorStateBudgetHandle::default(),
		});
		txn.set_change_coordinate(default_coordinate(version));
		txn
	}

	pub fn ephemeral(self) -> FlowTransaction {
		let query = self.engine.multi().begin_query().unwrap();
		let version = self.version;
		let mut txn = FlowTransaction::ephemeral(
			version,
			query,
			self.engine.inner().single().clone(),
			self.catalog,
			HashMap::new(),
			self.clock,
			OperatorStateBudgetHandle::default(),
		);
		txn.set_change_coordinate(default_coordinate(version));
		txn
	}
}

fn default_coordinate(version: CommitVersion) -> ChangeCoordinate {
	ChangeCoordinate {
		at: Some(DateTime::from_millis(0)),
		version,
	}
}

pub trait FlowTxn {
	fn flow_txn(&self) -> FlowTxnBuilder<'_>;

	fn commit_pending(&self, txn: &mut FlowTransaction);
}

impl FlowTxn for TestEngine {
	fn flow_txn(&self) -> FlowTxnBuilder<'_> {
		FlowTxnBuilder {
			engine: self,
			version: CommitVersion(1),
			clock: self.clock().clone(),
			catalog: Catalog::testing(),
		}
	}

	fn commit_pending(&self, txn: &mut FlowTransaction) {
		let pending = txn.take_pending();
		let mut cmd = self.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (key, pw) in pending.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::OperatorState)) {
				continue;
			}
			match pw {
				PendingWrite::Set(v) => cmd.set(key, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(key).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(key).unwrap(),
			};
		}
		let version = cmd.commit_unchecked().unwrap();
		apply_operator_state(&self.inner().operator_state(), version, &pending);
	}
}
