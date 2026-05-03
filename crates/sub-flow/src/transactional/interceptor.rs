// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	mem,
	sync::{Arc, RwLock},
};

use rayon::prelude::*;
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
	transactional::registry::TransactionalFlowRegistry,
};

pub struct TransactionalFlowPreCommitInterceptor {
	pub flow_engine: Arc<RwLock<FlowEngine>>,

	pub engine: StandardEngine,

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

	let mut available_changes = prepare_available_changes(&ctx.flow_changes, read_version);
	let base_pending = build_base_pending(&ctx.transaction_writes);

	for level in execution_levels {
		let view_overlay = build_view_overlay(&available_changes);
		let flow_txns = prepare_level_flow_txns(
			&level,
			&available_changes,
			flow_engine,
			engine,
			catalog,
			read_version,
			&base_pending,
			&view_overlay,
		)?;

		if flow_txns.is_empty() {
			continue;
		}

		let pools = engine.actor_system().pools();
		let results: Vec<Result<FlowResult>> = pools.system_pool().install(|| {
			flow_txns
				.into_par_iter()
				.map(|(flow_id, relevant, mut flow_txn)| {
					run_flow_in_level(flow_engine, flow_id, relevant, &mut flow_txn)
				})
				.collect()
		});

		merge_level_results(ctx, &mut available_changes, results, engine, read_version)?;
	}

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
#[allow(clippy::too_many_arguments)]
fn prepare_level_flow_txns(
	level: &[FlowId],
	available_changes: &[Change],
	flow_engine: &FlowEngine,
	engine: &StandardEngine,
	catalog: &Catalog,
	read_version: CommitVersion,
	base_pending: &Pending,
	view_overlay: &Arc<Vec<Change>>,
) -> Result<Vec<(FlowId, Vec<Change>, FlowTransaction)>> {
	let mut flow_txns: Vec<(FlowId, Vec<Change>, FlowTransaction)> = Vec::new();
	for &flow_id in level {
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
			view_overlay: Arc::clone(view_overlay),
		});

		flow_txns.push((flow_id, relevant, flow_txn));
	}
	Ok(flow_txns)
}

#[inline]
fn run_flow_in_level(
	flow_engine: &FlowEngine,
	flow_id: FlowId,
	relevant: Vec<Change>,
	flow_txn: &mut FlowTransaction,
) -> Result<FlowResult> {
	for change in relevant {
		flow_engine.process(flow_txn, change, flow_id)?;
	}

	flow_txn.flush_operator_states()?;

	Ok(FlowResult {
		view_entries: flow_txn.take_accumulator_entries(),
		pending: flow_txn.take_pending(),
		pending_shapes: flow_txn.take_pending_shapes(),
	})
}

#[inline]
fn merge_level_results(
	ctx: &mut PreCommitContext,
	available_changes: &mut Vec<Change>,
	results: Vec<Result<FlowResult>>,
	engine: &StandardEngine,
	read_version: CommitVersion,
) -> Result<()> {
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
	Ok(())
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
