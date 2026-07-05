// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem, sync::Arc};

use rayon::Scope;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_transaction::multi::transaction::read::MultiReadTransaction;
use reifydb_value::{Result, error::Error, reifydb_assertions, value::datetime::DateTime};
use smallvec::smallvec;

use crate::{
	engine::FlowEngineInner,
	transaction::{FlowTransaction, TransactionalParams},
};

#[inline]
pub(crate) fn prepare_available_changes(flow_changes: &[Change], read_version: CommitVersion) -> Vec<Change> {
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
pub(crate) fn build_base_pending(transaction_writes: &[(EncodedKey, Option<EncodedRow>)]) -> Pending {
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
		pending_shapes: flow_txn.take_pending_shapes(),
	})
}

pub(crate) struct Scheduler<'a> {
	pub(crate) flow_engine: &'a FlowEngineInner,
	pub(crate) engine: &'a StandardEngine,
	pub(crate) catalog: &'a Catalog,
	pub(crate) read_version: CommitVersion,
	pub(crate) base_pending: &'a Pending,
	pub(crate) base_query: &'a MultiReadTransaction,
	pub(crate) base_state_query: &'a MultiReadTransaction,
	pub(crate) state: Mutex<SchedulerState>,
}

pub(crate) struct SchedulerState {
	pub(crate) available_changes: Vec<Change>,
	pub(crate) in_degree: BTreeMap<FlowId, usize>,
	pub(crate) consumers: BTreeMap<FlowId, Vec<FlowId>>,
	pub(crate) view_entries: Vec<(ShapeId, Diff)>,
	pub(crate) pending_shapes: Vec<RowShape>,
	pub(crate) pending_writes: Vec<(EncodedKey, PendingWrite)>,
	pub(crate) first_error: Option<Error>,
}

impl<'a> Scheduler<'a> {
	pub(crate) fn dispatch<'scope>(&'scope self, s: &Scope<'scope>, flow_id: FlowId) {
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
			allocators: self.flow_engine.allocators.clone(),
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
			state.pending_writes.push((key.clone(), pw.clone()));
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

fn flow_is_interested_in(change: &Change, flow_id: FlowId, engine: &FlowEngineInner) -> bool {
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
