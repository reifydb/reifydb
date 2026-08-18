// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::BTreeSet;
use std::{
	collections::{BTreeMap, VecDeque},
	mem,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_transaction::{
	change::OperationType,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	transaction::Transaction,
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};
use smallvec::smallvec;

use crate::{
	engine::StandardEngine,
	flow::{
		engine::{FlowEngine, FlowEngineInner},
		transaction::FlowTransaction,
		transactional::registry::TransactionalFlowRegistry,
	},
};

pub struct TransactionalFlowPreCommitInterceptor {
	pub flow_engine: FlowEngine,

	pub engine: StandardEngine,

	pub catalog: Catalog,
}

impl PreCommitInterceptor for TransactionalFlowPreCommitInterceptor {
	fn intercept(&self, txn: &mut Transaction<'_>, ctx: &mut PreCommitContext) -> Result<()> {
		let engine = self.flow_engine.read_recursive();
		execute_inline_flow_changes(&engine, &self.engine, &self.catalog, txn, ctx)?;

		Ok(())
	}
}

pub(crate) fn execute_inline_flow_changes(
	flow_engine: &FlowEngineInner,
	engine: &StandardEngine,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ctx: &mut PreCommitContext,
) -> Result<()> {
	if ctx.flow_changes.is_empty() {
		return Ok(());
	}

	let mut schedule = flow_engine.calculate_schedule();
	if schedule.roots.is_empty() {
		return Ok(());
	}

	let read_version = txn.version();

	let mut execution = InlineExecution {
		flow_engine,
		engine,
		catalog,
		read_version,
		available_changes: prepare_available_changes(&ctx.flow_changes, read_version),
		in_degree: mem::take(&mut schedule.in_degree),
		consumers: mem::take(&mut schedule.consumers),
		view_entries: Vec::new(),
	};

	execution.run(txn, &schedule.roots)?;

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

	ctx.published_entries.append(&mut execution.view_entries);

	Ok(())
}

struct InlineExecution<'a> {
	flow_engine: &'a FlowEngineInner,
	engine: &'a StandardEngine,
	catalog: &'a Catalog,
	read_version: CommitVersion,
	available_changes: Vec<Change>,
	in_degree: BTreeMap<FlowId, usize>,
	consumers: BTreeMap<FlowId, Vec<FlowId>>,
	view_entries: Vec<(ObjectId, Diff)>,
}

impl InlineExecution<'_> {
	fn run(&mut self, txn: &mut Transaction<'_>, roots: &[FlowId]) -> Result<()> {
		reifydb_assertions! {
			let mut seen: BTreeSet<FlowId> = BTreeSet::new();
			for root in roots {
				assert!(
					seen.insert(*root),
					"dataflow scheduler was handed flow {} twice as a root, so it would be dispatched \
					 twice for the same set of changes and its operator state applied twice",
					root.0
				);
				assert!(
					self.in_degree.get(root).copied().unwrap_or(0) == 0,
					"dataflow scheduler was handed flow {} as a root while it still has upstream \
					 producers, so settling those producers dispatches it a second time for the same \
					 set of changes",
					root.0
				);
			}
		}
		let mut ready: VecDeque<FlowId> = roots.iter().copied().collect();

		while let Some(flow_id) = ready.pop_front() {
			if let Some(relevant) = self.relevant_changes(flow_id) {
				let result = self.run_flow(txn, flow_id, relevant)?;
				self.merge_flow_result(result);
			}

			ready.extend(self.settle(flow_id));
		}

		Ok(())
	}

	fn relevant_changes(&self, flow_id: FlowId) -> Option<Vec<Change>> {
		let relevant: Vec<Change> = self
			.available_changes
			.iter()
			.filter(|c| flow_is_interested_in(c, flow_id, self.flow_engine))
			.cloned()
			.collect();

		if relevant.is_empty() {
			return None;
		}

		Some(relevant)
	}

	fn run_flow(
		&self,
		txn: &mut Transaction<'_>,
		flow_id: FlowId,
		relevant: Vec<Change>,
	) -> Result<FlowResult> {
		let mut flow_txn = FlowTransaction::new(
			txn,
			self.catalog.clone(),
			self.engine.create_interceptors(),
			self.engine.clock().clone(),
			self.flow_engine.allocators.clone(),
		);

		self.flow_engine.process_batch(&mut flow_txn, relevant, flow_id)?;
		flow_txn.flush_operator_states()?;

		Ok(FlowResult {
			view_entries: flow_txn.take_accumulator_entries(),
		})
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
