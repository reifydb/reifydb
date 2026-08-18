// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, VecDeque},
	mem,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_transaction::{
	change::OperationType,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	multi::transaction::read::MultiReadTransaction,
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};
use smallvec::smallvec;

use crate::{
	engine::StandardEngine,
	flow::{
		engine::{FlowEngine, FlowEngineInner},
		transaction::{FlowTransaction, TransactionalParams},
		transactional::registry::TransactionalFlowRegistry,
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

	let (base_query, base_state_query, read_version) = prepare_inline_queries(engine)?;

	let mut execution = InlineExecution {
		flow_engine,
		engine,
		catalog,
		read_version,
		base_pending: build_base_pending(&ctx.transaction_writes),
		base_query,
		base_state_query,
		available_changes: prepare_available_changes(&ctx.flow_changes, read_version),
		in_degree: mem::take(&mut schedule.in_degree),
		consumers: mem::take(&mut schedule.consumers),
		view_entries: Vec::new(),
		pending_writes: Vec::new(),
	};

	execution.run(&schedule.roots)?;

	reifydb_assertions! {
		let unscheduled: Vec<u64> =
			execution.in_degree.iter().filter(|&(_, deg)| *deg > 0).map(|(id, _)| id.0).collect();
		assert!(
			unscheduled.is_empty(),
			"dataflow scheduler finished with {} flow(s) never scheduled (their in_degree never reached \
			 zero), so their views would silently not update this commit: {:?}; the inter-flow dependency \
			 graph is cyclic or the in_degree bookkeeping is wrong",
			unscheduled.len(),
			unscheduled
		);
	}

	ctx.view_entries.append(&mut execution.view_entries);
	ctx.pending_writes.append(&mut execution.pending_writes);

	Ok(())
}

struct InlineExecution<'a> {
	flow_engine: &'a FlowEngineInner,
	engine: &'a StandardEngine,
	catalog: &'a Catalog,
	read_version: CommitVersion,
	base_pending: Pending,
	base_query: MultiReadTransaction,
	base_state_query: MultiReadTransaction,
	available_changes: Vec<Change>,
	in_degree: BTreeMap<FlowId, usize>,
	consumers: BTreeMap<FlowId, Vec<FlowId>>,
	view_entries: Vec<(ObjectId, Diff)>,
	pending_writes: Vec<(EncodedKey, PendingWrite)>,
}

impl InlineExecution<'_> {
	fn run(&mut self, roots: &[FlowId]) -> Result<()> {
		let mut ready: VecDeque<FlowId> = roots.iter().copied().collect();

		while let Some(flow_id) = ready.pop_front() {
			if let Some((relevant, mut flow_txn)) = self.prepare_flow_txn(flow_id) {
				let result = run_flow(self.flow_engine, flow_id, relevant, &mut flow_txn)?;
				self.merge_flow_result(result);
			}

			ready.extend(self.settle(flow_id));
		}

		Ok(())
	}

	fn prepare_flow_txn(&self, flow_id: FlowId) -> Option<(Vec<Change>, FlowTransaction)> {
		let relevant: Vec<Change> = self
			.available_changes
			.iter()
			.filter(|c| flow_is_interested_in(c, flow_id, self.flow_engine))
			.cloned()
			.collect();

		if relevant.is_empty() {
			return None;
		}

		let flow_txn = FlowTransaction::transactional(TransactionalParams {
			version: self.read_version,
			pending: Pending::new(),
			base_pending: self.base_pending.clone(),
			query: self.base_query.clone(),
			state_query: self.base_state_query.clone(),
			single: self.engine.single().clone(),
			catalog: self.catalog.clone(),
			interceptors: self.engine.create_interceptors(),
			clock: self.engine.clock().clone(),
			view_overlay: build_view_overlay(&self.available_changes),
			allocators: self.flow_engine.allocators.clone(),
		});

		Some((relevant, flow_txn))
	}

	fn merge_flow_result(&mut self, result: FlowResult) {
		for (id, diff) in &result.view_entries {
			self.available_changes.push(Change {
				origin: ChangeOrigin::Object(*id),
				version: self.read_version,
				diffs: smallvec![diff.clone()],
				changed_at: DateTime::from_nanos(self.engine.clock().now().to_nanos()),
			});
		}
		self.view_entries.extend(result.view_entries);
		for (key, pw) in result.pending.iter_sorted() {
			self.pending_writes.push((key.clone(), pw.clone()));
		}
	}

	fn settle(&mut self, flow_id: FlowId) -> Vec<FlowId> {
		let consumers = self.consumers.get_mut(&flow_id).map(mem::take).unwrap_or_default();
		let mut newly_ready = Vec::new();
		for consumer in consumers {
			let degree = self.in_degree.get_mut(&consumer).expect("consumer must have an in_degree entry");
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
fn build_base_pending(transaction_writes: &[(EncodedKey, Option<EncodedBytes>)]) -> Pending {
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
			.filter(|c| matches!(c.origin, ChangeOrigin::Object(ObjectId::View(_))))
			.cloned()
			.collect(),
	)
}

#[inline]
fn run_flow(
	flow_engine: &FlowEngineInner,
	flow_id: FlowId,
	relevant: Vec<Change>,
	flow_txn: &mut FlowTransaction,
) -> Result<FlowResult> {
	flow_engine.process_batch(flow_txn, relevant, flow_id)?;

	flow_txn.flush_operator_states()?;

	Ok(FlowResult {
		view_entries: flow_txn.take_accumulator_entries(),
		pending: flow_txn.take_pending(),
	})
}

fn flow_is_interested_in(change: &Change, flow_id: FlowId, engine: &FlowEngineInner) -> bool {
	if let ChangeOrigin::Object(source) = change.origin {
		engine.sources
			.get(&source)
			.map(|registrations| registrations.iter().any(|(fid, _)| *fid == flow_id))
			.unwrap_or(false)
	} else {
		false
	}
}

struct FlowResult {
	view_entries: Vec<(ObjectId, Diff)>,
	pending: Pending,
}

fn prepare_inline_queries(
	engine: &StandardEngine,
) -> Result<(MultiReadTransaction, MultiReadTransaction, CommitVersion)> {
	let base_query = engine.multi().begin_query()?;
	let base_state_query = engine.multi().begin_query()?;
	let read_version = {
		let q: MultiReadTransaction = engine.multi().begin_query()?;
		q.version()
	};
	Ok((base_query, base_state_query, read_version))
}

pub struct TransactionalFlowPostCommitInterceptor {
	pub registrar: TransactionalFlowRegistry,
}

impl PostCommitInterceptor for TransactionalFlowPostCommitInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		for flow_change in &ctx.changes.flow {
			match flow_change.op {
				OperationType::Create => {
					if let Some(flow) = &flow_change.post {
						self.registrar.try_register_by_id_at_version(flow.id, ctx.version)?;
					}
				}
				OperationType::Delete => {
					if let Some(flow) = &flow_change.pre {
						self.registrar.flow_engine.write().remove_flow(flow.id);
						self.registrar.lineage.remove(flow.id);
					}
				}
				OperationType::Update => {}
			}
		}
		Ok(())
	}
}
