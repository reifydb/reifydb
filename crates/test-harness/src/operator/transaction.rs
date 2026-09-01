// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	actors::pending::{PendingLayers, PendingWrite},
	common::CommitVersion,
	delta::RemoveVisibility,
	interface::catalog::flow::OperatorId,
	key::{
		Key,
		kind::KeyKind,
		operator::{
			keyspace::KEYSPACES,
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
		},
	},
};
use reifydb_flow::transaction::{
	ChangeCoordinate, DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

use crate::engine::TestEngine;

pub const OPERATOR_ID: OperatorId = OperatorId(1);

pub fn make_row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

pub fn key(s: &str) -> GroupStateKey {
	let width = KEYSPACES
		.iter()
		.find(|spec| spec.id == KeyspaceId::CUSTOM_NOT_CACHED)
		.expect("the fixture keyspace must appear in the catalogue")
		.suffix_width();
	let mut suffix = vec![0u8; width];
	for (slot, byte) in suffix.iter_mut().zip(s.as_bytes()) {
		*slot = *byte;
	}
	OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::CUSTOM_NOT_CACHED, suffix)
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
			version,
			pending: PendingLayers::empty(),
			query: Some(self.engine.multi().begin_query().unwrap()),
			state_query: Some(self.engine.multi().begin_query().unwrap()),
			catalog: self.catalog,
			interceptors: Interceptors::new(),
			clock: self.clock,
			substrate: FlowSubstrate::with_dictionary(
				self.engine.inner().dictionary_allocators(),
				self.engine.inner().operator_state(),
			),
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

	fn commit_pending(&self, txn: &mut DeferredTransaction);
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

	fn commit_pending(&self, txn: &mut DeferredTransaction) {
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
					announce: RemoveVisibility::Announced,
				} => cmd.remove(key).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved,
				} => cmd.remove_unobserved(key).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Silent,
				} => cmd.remove_silent(key).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
		apply_operator_state(&self.inner().operator_state(), &pending);
	}
}
