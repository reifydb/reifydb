// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::Pending,
	common::CommitVersion,
	encoded::{
		key::EncodedKey,
		row::{EncodedRow, SHAPE_HEADER_SIZE},
	},
	interface::catalog::flow::FlowNodeId,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sub_flow::transaction::{FlowTransaction, TransactionalParams};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_type::{util::cowvec::CowVec, value::identity::IdentityId};

pub const NODE_ID: FlowNodeId = FlowNodeId(1);

pub fn make_row(payload: &str, created_at: u64, updated_at: u64) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	EncodedRow(CowVec::new(buf))
}

pub fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

pub fn engine() -> TestEngine {
	TestEngine::new()
}

pub fn deferred_txn(engine: &TestEngine) -> FlowTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	FlowTransaction::deferred(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
}

pub fn transactional_txn(engine: &TestEngine) -> FlowTransaction {
	let query = engine.multi().begin_query().unwrap();
	let state_query = engine.multi().begin_query().unwrap();
	FlowTransaction::transactional(TransactionalParams {
		version: CommitVersion(1),
		pending: Pending::new(),
		base_pending: Pending::new(),
		query,
		state_query,
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(1000)),
		view_overlay: Arc::new(Vec::new()),
	})
}

pub fn ephemeral_txn(engine: &TestEngine) -> FlowTransaction {
	let query = engine.multi().begin_query().unwrap();
	FlowTransaction::ephemeral(
		CommitVersion(1),
		query,
		Catalog::testing(),
		HashMap::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
}

pub fn payload(stored: &EncodedRow) -> &[u8] {
	&stored.0[SHAPE_HEADER_SIZE..]
}
