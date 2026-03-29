// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Interceptors for transactional (inline) view processing.
//!
//! **Pre-commit** (`TransactionalFlowPreCommitInterceptor`):
//! When a `CommandTransaction` or `AdminTransaction` commits, runs any
//! transactional flows that depend on the tables changed in the transaction.
//! The view writes produced by those flows are fed back into the transaction
//! as `ctx.pending_writes` and committed atomically with the original DML.
//!
//! **Post-commit** (`TransactionalFlowPostCommitInterceptor`):
//! After a `CREATE VIEW` (transactional) commits, eagerly registers the
//! flow so it is available for the very next transaction's pre-commit phase.

use std::sync::{Arc, RwLock};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::flow::FlowId,
	change::{Change, ChangeOrigin},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::{
	change::OperationType,
	interceptor::{
		interceptors::Interceptors,
		transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	},
	multi::transaction::read::MultiReadTransaction,
};
use reifydb_type::Result;
use tracing::warn;

use crate::{
	engine::FlowEngine,
	transaction::{
		FlowTransaction,
		pending::{Pending, PendingWrite},
	},
	transactional::registrar::TransactionalFlowRegistrar,
};

/// Pre-commit interceptor that executes transactional (inline) flows.
///
/// This interceptor holds a separate `FlowEngine` containing ONLY transactional
/// views — it is distinct from the coordinator's CDC `FlowEngine` which handles
/// deferred views.
pub struct TransactionalFlowPreCommitInterceptor {
	/// The flow engine containing only transactional view flows.
	pub flow_engine: Arc<RwLock<FlowEngine>>,
	/// The standard engine used to create read transactions for flow processing.
	pub engine: StandardEngine,
	/// The catalog for metadata access inside flow transactions.
	pub catalog: Catalog,
}

impl PreCommitInterceptor for TransactionalFlowPreCommitInterceptor {
	fn intercept(&self, ctx: &mut PreCommitContext) -> Result<()> {
		let engine = self.flow_engine.read().unwrap();
		execute_inline_flow_changes(&engine, &self.engine, &self.catalog, ctx)
	}
}

pub(crate) fn execute_inline_flow_changes(
	flow_engine: &FlowEngine,
	engine: &StandardEngine,
	catalog: &Catalog,
	ctx: &mut PreCommitContext,
) -> Result<()> {
	if ctx.flow_changes.is_empty() {
		return Ok(());
	}

	let execution_order = flow_engine.calculate_execution_order();
	if execution_order.is_empty() {
		return Ok(());
	}

	let read_version = {
		let q: MultiReadTransaction = engine.multi().begin_query()?;
		q.version()
	};

	// Stamp the correct version on changes from the accumulator
	let mut available_changes: Vec<Change> = ctx
		.flow_changes
		.iter()
		.map(|c| {
			let mut c = c.clone();
			c.version = read_version;
			c
		})
		.collect();

	for flow_id in execution_order {
		let relevant: Vec<Change> = available_changes
			.iter()
			.filter(|c| flow_is_interested_in(c, flow_id, flow_engine))
			.cloned()
			.collect();

		if relevant.is_empty() {
			continue;
		}

		let primitive_query: MultiReadTransaction = engine.multi().begin_query()?;
		let state_query: MultiReadTransaction = engine.multi().begin_query()?;
		let interceptors: Interceptors = engine.create_interceptors();

		let mut base_pending = Pending::new();
		for (key, value) in &ctx.transaction_writes {
			match value {
				Some(v) => base_pending.insert(key.clone(), v.clone()),
				None => base_pending.remove(key.clone()),
			}
		}

		let mut flow_txn = FlowTransaction::transactional(
			read_version,
			Pending::new(),
			base_pending,
			primitive_query,
			state_query,
			catalog.clone(),
			interceptors,
		);

		for change in relevant {
			flow_engine.process(&mut flow_txn, change, flow_id)?;
		}

		let view_entries = flow_txn.take_accumulator_entries();
		for (id, diff) in &view_entries {
			available_changes.push(Change {
				origin: ChangeOrigin::Shape(id.clone()),
				version: read_version,
				diffs: vec![diff.clone()],
			});
		}
		ctx.view_entries.extend(view_entries);

		let flow_pending = flow_txn.take_pending();
		for (key, pw) in flow_pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => ctx.pending_writes.push((key.clone(), Some(v.clone()))),
				PendingWrite::Remove => ctx.pending_writes.push((key.clone(), None)),
			}
		}
	}

	Ok(())
}

/// Returns true if the given change is relevant to the given flow.
///
/// Uses the flow engine's `sources` map which records which primitives
/// (tables/views) each flow listens to.
fn flow_is_interested_in(change: &Change, flow_id: FlowId, engine: &FlowEngine) -> bool {
	if let ChangeOrigin::Shape(source) = change.origin {
		engine.sources
			.get(&source)
			.map(|registrations| registrations.iter().any(|(fid, _)| *fid == flow_id))
			.unwrap_or(false)
	} else {
		false
	}
}

/// Post-commit interceptor that eagerly registers transactional flows.
///
/// When an admin transaction that creates a new flow commits, this interceptor
/// loads the flow DAG and registers it in the transactional `FlowEngine` so it
/// is available for the very next transaction's pre-commit phase — without
/// waiting for CDC polling to discover it.
pub struct TransactionalFlowPostCommitInterceptor {
	pub registrar: TransactionalFlowRegistrar,
}

impl PostCommitInterceptor for TransactionalFlowPostCommitInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		for flow_change in &ctx.changes.flow {
			if flow_change.op == OperationType::Create {
				if let Some(flow) = &flow_change.post {
					if let Err(e) = self.registrar.try_register_by_id(flow.id) {
						warn!(
							flow_id = flow.id.0,
							error = %e,
							"failed to register transactional flow on commit"
						);
					}
				}
			}
		}
		Ok(())
	}
}
