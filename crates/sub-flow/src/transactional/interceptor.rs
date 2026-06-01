// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem, sync::Arc};

use rayon::{Scope, scope};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	reifydb_assertions,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_transaction::{
	change::OperationType,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	multi::transaction::read::MultiReadTransaction,
	transaction::Transaction,
};
use reifydb_value::{
	Result,
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};
use smallvec::smallvec;
use tracing::warn;

use crate::{
	engine::FlowEngine,
	transaction::{FlowTransaction, TransactionalParams},
	transactional::registry::TransactionalFlowRegistry,
};

pub struct TransactionalFlowPreCommitInterceptor {
	pub flow_engine: Arc<RwLock<FlowEngine>>,

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
	flow_engine: &FlowEngine,
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

#[inline]
fn prepare_available_changes(flow_changes: &[Change], read_version: CommitVersion) -> Vec<Change> {
	flow_changes
		.iter()
		.map(|c| {
			let mut c = c.clone();
			c.version = read_version;
			c
		})
		.collect()
}

#[inline]
fn build_base_pending(transaction_writes: &[(EncodedKey, Option<EncodedRow>)]) -> Pending {
	let mut p = Pending::new();
	for (key, value) in transaction_writes {
		match value {
			Some(v) => p.insert(key.clone(), v.clone()),
			None => p.remove(key.clone()),
		}
	}
	p
}

#[inline]
fn build_view_overlay(available_changes: &[Change]) -> Arc<Vec<Change>> {
	Arc::new(
		available_changes
			.iter()
			.filter(|c| matches!(c.origin, ChangeOrigin::Shape(ShapeId::View(_))))
			.cloned()
			.collect(),
	)
}

#[inline]
fn run_flow(
	flow_engine: &FlowEngine,
	flow_id: FlowId,
	relevant: Vec<Change>,
	flow_txn: &mut FlowTransaction,
) -> Result<FlowResult> {
	flow_engine.process_batch(flow_txn, relevant, flow_id)?;

	flow_txn.flush_operator_states()?;

	Ok(FlowResult {
		view_entries: flow_txn.take_accumulator_entries(),
		pending: flow_txn.take_pending(),
		pending_shapes: flow_txn.take_pending_shapes(),
	})
}

struct Scheduler<'a> {
	flow_engine: &'a FlowEngine,
	engine: &'a StandardEngine,
	catalog: &'a Catalog,
	read_version: CommitVersion,
	base_pending: &'a Pending,
	base_query: &'a MultiReadTransaction,
	base_state_query: &'a MultiReadTransaction,
	state: Mutex<SchedulerState>,
}

struct SchedulerState {
	available_changes: Vec<Change>,
	in_degree: BTreeMap<FlowId, usize>,
	consumers: BTreeMap<FlowId, Vec<FlowId>>,
	view_entries: Vec<(ShapeId, Diff)>,
	pending_shapes: Vec<RowShape>,
	pending_writes: Vec<(EncodedKey, Option<EncodedRow>)>,
	drops: Vec<EncodedKey>,
	first_error: Option<Error>,
}

impl<'a> Scheduler<'a> {
	fn dispatch<'scope>(&'scope self, s: &Scope<'scope>, flow_id: FlowId) {
		s.spawn(move |s| self.run(s, flow_id));
	}

	fn run<'scope>(&'scope self, s: &Scope<'scope>, flow_id: FlowId) {
		let prepared = {
			let state = self.state.lock();
			if state.first_error.is_some() {
				return;
			}
			self.prepare_flow_txn(&state.available_changes, flow_id)
		};

		let outcome = prepared
			.map(|(relevant, mut flow_txn)| run_flow(self.flow_engine, flow_id, relevant, &mut flow_txn));

		let mut state = self.state.lock();
		match outcome {
			Some(Err(err)) => {
				if state.first_error.is_none() {
					state.first_error = Some(err);
				}
				return;
			}
			Some(Ok(result)) => self.merge_flow_result(&mut state, result),
			None => {}
		}

		let newly_ready = self.settle(&mut state, flow_id);
		drop(state);

		for child in newly_ready {
			self.dispatch(s, child);
		}
	}

	fn prepare_flow_txn(
		&self,
		available_changes: &[Change],
		flow_id: FlowId,
	) -> Option<(Vec<Change>, FlowTransaction)> {
		let relevant: Vec<Change> = available_changes
			.iter()
			.filter(|c| flow_is_interested_in(c, flow_id, self.flow_engine))
			.cloned()
			.collect();

		if relevant.is_empty() {
			return None;
		}

		let query = self.base_query.clone();
		let state_query = self.base_state_query.clone();
		let interceptors = self.engine.create_interceptors();

		let flow_txn = FlowTransaction::transactional(TransactionalParams {
			version: self.read_version,
			pending: Pending::new(),
			base_pending: self.base_pending.clone(),
			query,
			state_query,
			single: self.engine.single().clone(),
			catalog: self.catalog.clone(),
			interceptors,
			clock: self.engine.clock().clone(),
			view_overlay: build_view_overlay(available_changes),
		});

		Some((relevant, flow_txn))
	}

	fn merge_flow_result(&self, state: &mut SchedulerState, result: FlowResult) {
		for (id, diff) in &result.view_entries {
			state.available_changes.push(Change {
				origin: ChangeOrigin::Shape(*id),
				version: self.read_version,
				diffs: smallvec![diff.clone()],
				changed_at: DateTime::from_nanos(self.engine.clock().now_nanos()),
			});
		}
		state.view_entries.extend(result.view_entries);
		state.pending_shapes.extend(result.pending_shapes);
		for (key, pw) in result.pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => state.pending_writes.push((key.clone(), Some(v.clone()))),
				PendingWrite::Remove => state.pending_writes.push((key.clone(), None)),
				PendingWrite::Drop => state.drops.push(key.clone()),
			}
		}
	}

	fn settle(&self, state: &mut SchedulerState, flow_id: FlowId) -> Vec<FlowId> {
		let consumers = state.consumers.get_mut(&flow_id).map(mem::take).unwrap_or_default();
		let mut newly_ready = Vec::new();
		for consumer in consumers {
			let degree = state.in_degree.get_mut(&consumer).expect("consumer must have an in_degree entry");
			reifydb_assertions! {
				assert!(
					*degree > 0,
					"dataflow scheduler decremented in_degree of flow {} below zero while settling \
					 producer {}, so the consumer would be dispatched more than once and its operator \
					 state double-applied (its in_degree was already zero)",
					consumer.0,
					flow_id.0
				);
			}
			*degree -= 1;
			if *degree == 0 {
				newly_ready.push(consumer);
			}
		}
		newly_ready
	}
}

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
