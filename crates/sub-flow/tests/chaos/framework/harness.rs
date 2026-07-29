// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::Pending, common::CommitVersion, interface::change::Change,
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_flow::{
	operator::Operator,
	transaction::{ChangeCoordinate, DeferredParams, FlowTransaction, substrate::FlowSubstrate, timer::Timer},
};
use reifydb_runtime::context::{
	RuntimeContext,
	clock::{Clock, MockClock},
};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{Result, value::datetime::DateTime};

pub struct Harness<O: Operator> {
	engine: TestEngine,
	operator: O,
	clock: MockClock,
	version: u64,
	pending: Pending,
	substrate: FlowSubstrate,
}

impl<O: Operator> Harness<O> {
	pub fn new(build: impl FnOnce(RuntimeContext) -> O) -> Self {
		let engine = TestEngine::new();
		let clock = engine.mock_clock();
		let runtime = RuntimeContext::new(
			Clock::Mock(clock.clone()),
			engine.inner().rng().clone(),
			engine.inner().version_epoch().clone(),
		);
		Self {
			engine,
			operator: build(runtime),
			clock,
			version: 1,
			pending: Pending::new(),
			substrate: FlowSubstrate::new(),
		}
	}

	pub fn operator(&self) -> &O {
		&self.operator
	}

	pub fn now(&self) -> DateTime {
		self.clock.now()
	}

	pub fn advance_millis(&mut self, millis: u64) {
		self.clock.advance_millis(millis);
	}

	pub fn set_millis(&mut self, millis: u64) {
		self.clock.set_millis(millis);
	}

	fn begin(&mut self) -> FlowTransaction {
		let query = self.engine.multi().begin_query().expect("begin_query");
		let state_query = self.engine.multi().begin_query().expect("begin_query");
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: CommitVersion(self.version),
			pending: mem::take(&mut self.pending),
			base_pending: Arc::new(Pending::new()),
			query,
			state_query,
			single: self.engine.inner().single().clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(self.clock.clone()),
			substrate: self.substrate.clone(),
			state_budget: OperatorStateBudgetHandle::default(),
		});
		txn.set_change_coordinate(ChangeCoordinate {
			at: self.clock.now(),
			version: CommitVersion(self.version),
		});
		txn
	}

	fn end(&mut self, mut txn: FlowTransaction) {
		self.pending = txn.take_pending();
		self.version += 1;
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let mut txn = self.begin();
		let out = self.operator.apply(&mut txn, change)?;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(out)
	}

	pub fn on_timer(&mut self, timer: Timer) -> Result<Option<Change>> {
		let mut txn = self.begin();
		let out = self.operator.on_timer(&mut txn, timer)?;
		txn.flush_operator_states()?;
		self.end(txn);
		Ok(out)
	}
}
