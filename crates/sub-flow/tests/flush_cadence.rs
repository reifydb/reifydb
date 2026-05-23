// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression guard for review #4 (flush_state cadence). The native backend must
// DEFER flush_state to transaction commit (flush_operator_states), matching the
// FFI backend, rather than flushing inside each apply/tick. The operator below
// writes its state only in flush_state, making the persistence point observable:
// after apply the state must NOT yet be visible in the transaction, and only
// after flush_operator_states must it be persisted. Before this alignment the
// native backend flushed per-apply, so the state was visible immediately after
// apply - which this test forbids. Asserted across all three txn variants.

use std::collections::HashMap;

use fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, transactional_txn};
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diffs},
	},
};
use reifydb_sdk::{
	error::Result as SdkResult,
	operator::{
		OperatorLogic,
		context::{OperatorContext, StateApi},
		view::ChangeView,
	},
};
use reifydb_sub_flow::{
	operator::{Operator, native::NativeOperator},
	transaction::FlowTransaction,
};
use reifydb_type::value::{Value, datetime::DateTime};

#[path = "state/fixtures.rs"]
mod fixtures;

fn cadence_key() -> EncodedKey {
	EncodedKey::new(b"flush-cadence".to_vec())
}

// Persists its buffered value ONLY in flush_state, so the moment of persistence
// reveals whether the backend flushes per-apply or defers to commit.
struct DeferredWriter {
	pending: Option<u64>,
}

impl OperatorLogic for DeferredWriter {
	fn create(_id: FlowNodeId, _config: &HashMap<String, Value>) -> SdkResult<Self> {
		Ok(Self {
			pending: None,
		})
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		self.pending = Some(7);
		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> SdkResult<()> {
		if let Some(value) = self.pending.take() {
			ctx.state().set(&cadence_key(), &value)?;
		}
		Ok(())
	}
}

fn assert_flush_is_deferred(txn: &mut FlowTransaction) {
	let op = NativeOperator::new(
		DeferredWriter {
			pending: None,
		},
		NODE_ID,
		0,
	);
	let change = Change::from_flow(NODE_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));

	op.apply(txn, change).unwrap();

	assert!(
		txn.state_get(NODE_ID, &cadence_key()).unwrap().is_none(),
		"native must defer flush_state to commit, but state was persisted during apply"
	);

	txn.flush_operator_states().unwrap();

	assert!(
		txn.state_get(NODE_ID, &cadence_key()).unwrap().is_some(),
		"flush_operator_states must persist the deferred state"
	);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_flush_is_deferred(&mut txn);
}
