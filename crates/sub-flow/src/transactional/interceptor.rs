// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::mem;

use rayon::scope;
use reifydb_catalog::catalog::Catalog;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{reifydb_assertions, sync::mutex::Mutex};
use reifydb_transaction::{
	change::OperationType,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	multi::transaction::read::MultiReadTransaction,
	transaction::Transaction,
};
use reifydb_value::{Result, value::identity::IdentityId};
use tracing::warn;

use crate::{
	engine::{FlowEngine, FlowEngineInner},
	transactional::{
		registry::TransactionalFlowRegistry,
		scheduler::{Scheduler, SchedulerState, build_base_pending, prepare_available_changes},
	},
};

pub struct TransactionalFlowPreCommitInterceptor {
	pub flow_engine: FlowEngine,

	pub engine: StandardEngine,

	pub catalog: Catalog,
}

impl PreCommitInterceptor for TransactionalFlowPreCommitInterceptor {
	fn intercept(&self, ctx: &mut PreCommitContext) -> Result<()> {
		let engine = self.flow_engine.read_recursive();
		execute_inline_flow_changes(&engine, &self.engine, &self.catalog, ctx)?;

		if !ctx.pending_shapes.is_empty() {
			let shapes = mem::take(&mut ctx.pending_shapes);
			let mut cmd = self.engine.begin_command(IdentityId::system())?;
			cmd.disable_conflict_tracking()?;
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut cmd), shapes)?;
			cmd.commit_unchecked()?;
		}

		Ok(())
	}
}

pub(crate) fn execute_inline_flow_changes(
	flow_engine: &FlowEngineInner,
	engine: &StandardEngine,
	catalog: &Catalog,
	ctx: &mut PreCommitContext,
) -> Result<()> {
	if ctx.flow_changes.is_empty() {
		return Ok(());
	}

	let mut schedule = flow_engine.calculate_schedule();
	if schedule.roots.is_empty() {
		return Ok(());
	}

	let base_query = engine.multi().begin_query()?;
	let base_state_query = engine.multi().begin_query()?;

	let read_version = {
		let q: MultiReadTransaction = engine.multi().begin_query()?;
		q.version()
	};

	let available_changes = prepare_available_changes(&ctx.flow_changes, read_version);
	let base_pending = build_base_pending(&ctx.transaction_writes);

	let scheduler = Scheduler {
		flow_engine,
		engine,
		catalog,
		read_version,
		base_pending: &base_pending,
		base_query: &base_query,
		base_state_query: &base_state_query,
		state: Mutex::new(SchedulerState {
			available_changes,
			in_degree: mem::take(&mut schedule.in_degree),
			consumers: mem::take(&mut schedule.consumers),
			view_entries: Vec::new(),
			pending_shapes: Vec::new(),
			pending_writes: Vec::new(),
			drops: Vec::new(),
			first_error: None,
		}),
	};

	let pools = engine.spawner().pools();
	pools.commit_pool().install(|| {
		scope(|s| {
			for root in &schedule.roots {
				scheduler.dispatch(s, *root);
			}
		})
	});

	let mut state = scheduler.state.lock();

	if let Some(err) = state.first_error.take() {
		return Err(err);
	}

	reifydb_assertions! {
		let unscheduled: Vec<u64> =
			state.in_degree.iter().filter(|&(_, deg)| *deg > 0).map(|(id, _)| id.0).collect();
		assert!(
			unscheduled.is_empty(),
			"dataflow scheduler finished with {} flow(s) never scheduled (their in_degree never reached \
			 zero), so their views would silently not update this commit: {:?}; the inter-flow dependency \
			 graph is cyclic or the in_degree bookkeeping is wrong",
			unscheduled.len(),
			unscheduled
		);
	}

	ctx.view_entries.append(&mut state.view_entries);
	ctx.pending_shapes.append(&mut state.pending_shapes);
	ctx.pending_writes.append(&mut state.pending_writes);
	ctx.drops.append(&mut state.drops);

	Ok(())
}

pub struct TransactionalFlowPostCommitInterceptor {
	pub registrar: TransactionalFlowRegistry,
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
