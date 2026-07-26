// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::encoded::row::{EncodedRow, SHAPE_HEADER_SIZE};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_flow::transaction::{FlowTransaction, TransactionalParams, allocators::FlowAllocators};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{util::cowvec::CowVec, value::identity::IdentityId};

pub const NODE_ID: FlowNodeId = FlowNodeId(1);

pub fn make_row(payload: &str, created_at: u64, updated_at: u64) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	EncodedRow(CowVec::new(buf))
}

pub fn key(s: &str) -> StateKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::FIRST_CUSTOM, s.as_bytes())
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
		let parent = self.engine.begin_admin(IdentityId::system()).unwrap();
		FlowTransaction::deferred(&parent, self.version, self.catalog, Interceptors::new(), self.clock)
	}

	pub fn transactional(self) -> FlowTransaction {
		let query = self.engine.multi().begin_query().unwrap();
		let state_query = self.engine.multi().begin_query().unwrap();
		FlowTransaction::transactional(TransactionalParams {
			version: self.version,
			pending: Pending::new(),
			base_pending: Pending::new(),
			query,
			state_query,
			single: self.engine.inner().single().clone(),
			catalog: self.catalog,
			interceptors: Interceptors::new(),
			clock: self.clock,
			view_overlay: Arc::new(Vec::new()),
			allocators: FlowAllocators::new(),
			state_budget: OperatorStateBudgetHandle::default(),
		})
	}

	pub fn ephemeral(self) -> FlowTransaction {
		let query = self.engine.multi().begin_query().unwrap();
		FlowTransaction::ephemeral(
			self.version,
			query,
			self.engine.inner().single().clone(),
			self.catalog,
			HashMap::new(),
			self.clock,
			OperatorStateBudgetHandle::default(),
		)
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
		cmd.commit_unchecked().unwrap();
	}
}
