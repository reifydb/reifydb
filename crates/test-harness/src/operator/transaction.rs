// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	actors::pending::Pending,
	commit::tracker::WriteTracker,
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::KEYSPACES,
		state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
	},
};
use reifydb_flow_async::transaction::{
	ChangeCoordinate, DeferredParams, FlowTransaction, deferred::DeferredTransaction, substrate::FlowSubstrate,
};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	transaction::TransactionContext,
};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::datetime::DateTime;

use crate::engine::TestEngine;

pub const OPERATOR_ID: OperatorId = OperatorId(1);

pub fn make_row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

pub fn key(s: &str) -> GroupStateKey {
	let width = KEYSPACES
		.iter()
		.find(|spec| spec.id == KeyspaceId::CUSTOM_UNMANAGED)
		.expect("the fixture keyspace must appear in the catalogue")
		.suffix_width();
	let mut suffix = vec![0u8; width];
	for (slot, byte) in suffix.iter_mut().zip(s.as_bytes()) {
		*slot = *byte;
	}
	OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::CUSTOM_UNMANAGED, suffix)
}

pub fn engine() -> TestEngine {
	TestEngine::new()
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

	pub fn deferred(self) -> DeferredTransaction {
		let version = self.version;
		let mut txn = DeferredTransaction::new(DeferredParams {
			ctx: Arc::new(TransactionContext::system(self.engine.runtime_context().clone())),
			version,
			pending: Pending::new(),
			query: Some(self
				.engine
				.multi()
				.begin_query(self.engine.multi().generate_transaction_id())
				.unwrap()),
			state_query: Some(self
				.engine
				.multi()
				.begin_query(self.engine.multi().generate_transaction_id())
				.unwrap()),
			catalog: self.catalog,
			interceptors: Interceptors::new(),
			clock: self.clock,
			substrate: FlowSubstrate::with_dictionary(
				self.engine.inner().dictionary_allocators(),
				self.engine.inner().operator_state(),
				WriteTracker::detached(),
			),
			lookup: None,
		});
		txn.set_change_coordinate(default_coordinate());
		txn
	}
}

fn default_coordinate() -> ChangeCoordinate {
	ChangeCoordinate {
		at: Some(DateTime::from_millis(0)),
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
