// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{actors::pending::PendingLayers, common::CommitVersion};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::datetime::DateTime;

use crate::transaction::{
	ChangeCoordinate, DeferredParams, deferred::DeferredTransaction, interface::FlowTransaction,
	substrate::FlowSubstrate,
};

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

	pub fn deferred(self) -> DeferredTransaction {
		let version = self.version;
		let mut txn = DeferredTransaction::from_parts(DeferredParams {
			version,
			pending: PendingLayers::empty(),
			query: self.engine.multi().begin_query().unwrap(),
			state_query: self.engine.multi().begin_query().unwrap(),
			catalog: self.catalog,
			interceptors: Interceptors::new(),
			clock: self.clock,
			substrate: FlowSubstrate {
				operators: self.engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
		});
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
}
