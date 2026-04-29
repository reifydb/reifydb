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

use std::{
	mem,
	sync::{Arc, RwLock},
};

use rayon::prelude::*;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::{
	change::OperationType,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	multi::transaction::read::MultiReadTransaction,
	transaction::Transaction,
};
use reifydb_type::{
	Result,
	value::{datetime::DateTime, identity::IdentityId},
};
use smallvec::smallvec;
use tracing::warn;

use crate::{
	engine::FlowEngine,
	transaction::{FlowTransaction, TransactionalParams},
	transactional::registrar::TransactionalFlowRegistrar,
};

/// Pre-commit interceptor that executes transactional (inline) flows.
///
/// This interceptor holds a separate `FlowEngine` containing ONLY transactional
/// views - it is distinct from the coordinator's CDC `FlowEngine` which handles
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
		execute_inline_flow_changes(&engine, &self.engine, &self.catalog, ctx)?;

		if !ctx.pending_shapes.is_empty() {
			let shapes = mem::take(&mut ctx.pending_shapes);
			let mut cmd = self.engine.begin_command(IdentityId::system())?;
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut cmd), shapes)?;
			cmd.commit()?;
		}

		Ok(())
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

	let execution_levels = flow_engine.calculate_execution_levels();
	if execution_levels.is_empty() {
		return Ok(());
	}

	let read_version = {
		let q: MultiReadTransaction = engine.multi().begin_query()?;
		q.version()
	};

	let mut available_changes: Vec<Change> = ctx
		.flow_changes
		.iter()
		.map(|c| {
			let mut c = c.clone();
			c.version = read_version;
			c
		})
		.collect();

	let base_pending = {
		let mut p = Pending::new();
		for (key, value) in &ctx.transaction_writes {
			match value {
				Some(v) => p.insert(key.clone(), v.clone()),
				None => p.remove(key.clone()),
			}
		}
		p
	};

	for level in execution_levels {
		// Snapshot of all view-origin changes produced by previous levels (or the
		// original `flow_changes` from the committing txn). Shared read-only with
		// every flow txn in this level so pull paths on a view parent can overlay
		// these on top of their `read_version` storage scan. See the regression at
		// `testsuite/flow/tests/scripts/transactional/regression/004_left_join_between_views`.
		let view_overlay: Arc<Vec<Change>> = Arc::new(
			available_changes
				.iter()
				.filter(|c| matches!(c.origin, ChangeOrigin::Shape(ShapeId::View(_))))
				.cloned()
				.collect(),
		);

		let mut flow_txns: Vec<(FlowId, Vec<Change>, FlowTransaction)> = Vec::new();
		for &flow_id in &level {
			let relevant: Vec<Change> = available_changes
				.iter()
				.filter(|c| flow_is_interested_in(c, flow_id, flow_engine))
				.cloned()
				.collect();

			if relevant.is_empty() {
				continue;
			}

			let query = engine.multi().begin_query()?;
			let state_query = engine.multi().begin_query()?;
			let interceptors = engine.create_interceptors();

			let flow_txn = FlowTransaction::transactional(TransactionalParams {
				version: read_version,
				pending: Pending::new(),
				base_pending: base_pending.clone(),
				query,
				state_query,
				catalog: catalog.clone(),
				interceptors,
				clock: engine.clock().clone(),
				view_overlay: Arc::clone(&view_overlay),
			});

			flow_txns.push((flow_id, relevant, flow_txn));
		}

		let pools = engine.actor_system().pools();
		let results: Vec<Result<FlowResult>> = pools.system_pool().install(|| {
			flow_txns
				.into_par_iter()
				.map(|(flow_id, relevant, mut flow_txn)| {
					for change in relevant {
						flow_engine.process(&mut flow_txn, change, flow_id)?;
					}

					// Flush cached operator state so its writes go into
					// `pending` and commit atomically with the rest of
					// this transactional flow's outputs. Release per-FFI
					// arenas at the same boundary.
					flow_txn.flush_operator_states()?;
					flow_txn.release_ffi_scratch();

					Ok(FlowResult {
						view_entries: flow_txn.take_accumulator_entries(),
						pending: flow_txn.take_pending(),
						pending_shapes: flow_txn.take_pending_shapes(),
					})
				})
				.collect()
		});

		for result in results {
			let result = result?;
			for (id, diff) in &result.view_entries {
				available_changes.push(Change {
					origin: ChangeOrigin::Shape(*id),
					version: read_version,
					diffs: smallvec![diff.clone()],
					changed_at: DateTime::from_nanos(engine.clock().now_nanos()),
				});
			}
			ctx.view_entries.extend(result.view_entries);
			ctx.pending_shapes.extend(result.pending_shapes);
			for (key, pw) in result.pending.iter_sorted() {
				match pw {
					PendingWrite::Set(v) => ctx.pending_writes.push((key.clone(), Some(v.clone()))),
					PendingWrite::Remove => ctx.pending_writes.push((key.clone(), None)),
				}
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

struct FlowResult {
	view_entries: Vec<(ShapeId, Diff)>,
	pending: Pending,
	pending_shapes: Vec<RowShape>,
}

/// Post-commit interceptor that eagerly registers transactional flows.
///
/// When an admin transaction that creates a new flow commits, this interceptor
/// loads the flow DAG and registers it in the transactional `FlowEngine` so it
/// is available for the very next transaction's pre-commit phase - without
/// waiting for CDC polling to discover it.
pub struct TransactionalFlowPostCommitInterceptor {
	pub registrar: TransactionalFlowRegistrar,
}

impl PostCommitInterceptor for TransactionalFlowPostCommitInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		for flow_change in &ctx.changes.flow {
			if flow_change.op == OperationType::Create
				&& let Some(flow) = &flow_change.post
				&& let Err(e) = self.registrar.try_register_by_id(flow.id)
			{
				warn!(
					flow_id = flow.id.0,
					error = %e,
					"failed to register transactional flow on commit"
				);
			}
		}
		Ok(())
	}
}
