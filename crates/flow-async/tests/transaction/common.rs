// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{actors::pending::Pending, commit::tracker::WriteTracker, common::CommitVersion};
use reifydb_flow_async::transaction::{
	DeferredParams,
	substrate::{FlowSubstrate, operator_section},
};
use reifydb_runtime::context::{clock::Clock, transaction::TransactionContext};
use reifydb_store_operator::{store::OperatorStore, types::classify_slice};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::admin::AdminTransaction};
use reifydb_value::value::identity::IdentityId;
use reifydb_wal::lsn::Lsn;

pub fn create_test_transaction() -> (TestEngine, AdminTransaction, OperatorStore) {
	let t = TestEngine::new();
	let operators = t.inner().operator_state();
	let parent = t.begin_admin(IdentityId::system()).unwrap();
	(t, parent, operators)
}

static LSN: AtomicU64 = AtomicU64::new(0);

pub fn next_lsn() -> Lsn {
	Lsn::new(LSN.fetch_add(1, Ordering::Relaxed) + 1)
}

pub fn persist(store: &OperatorStore, pending: &Pending) {
	let (slice, sizes) = operator_section(store, pending, &[], &[]).unwrap();
	store.apply_batch_with_checkpoints(&classify_slice(&slice, &sizes), &[], &[], Some(next_lsn())).unwrap();
}

pub fn deferred_params(
	parent: &AdminTransaction,
	operators: OperatorStore,
	version: CommitVersion,
	catalog: Catalog,
	interceptors: Interceptors,
	clock: Clock,
) -> DeferredParams {
	DeferredParams {
		ctx: Arc::new(TransactionContext::system(parent.multi.runtime_context())),
		version,
		pending: Pending::new(),
		query: Some(parent.multi.begin_query(parent.multi.generate_transaction_id()).unwrap()),
		state_query: Some(parent.multi.begin_query(parent.multi.generate_transaction_id()).unwrap()),
		catalog,
		interceptors,
		clock,
		substrate: FlowSubstrate::with_dictionary(
			parent.dictionary_allocators()
				.expect("admin transaction reached a flow without a dictionary registry"),
			operators,
			WriteTracker::detached(),
		),
		lookup: None,
	}
}
