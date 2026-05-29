// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	event::row::OperatorRowsExpiredEvent,
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::{Change, ChangeOrigin},
	},
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNode};
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::{Span, field, instrument};

use crate::{engine::FlowEngine, operator::Operators, transaction::FlowTransaction};

impl FlowEngine {
	#[instrument(name = "flow::engine::process", level = "debug", skip(self, txn, change), fields(
		flow_id = ?flow_id,
		origin = ?change.origin,
		version = change.version.0,
		diff_count = change.diffs.len(),
		row_count = change.row_count(),
		nodes_processed = field::Empty
	))]
	pub fn process(&self, txn: &mut FlowTransaction, change: Change, flow_id: FlowId) -> Result<()> {
		self.process_batch(txn, vec![change], flow_id)
	}

	#[instrument(name = "flow::engine::process_batch", level = "debug", skip(self, txn, changes), fields(
		flow_id = ?flow_id,
		batch_change_count = changes.len(),
		batch_row_count = changes.iter().map(Change::row_count).sum::<usize>(),
		version_count = field::Empty,
		nodes_processed = field::Empty
	))]
	pub fn process_batch(&self, txn: &mut FlowTransaction, changes: Vec<Change>, flow_id: FlowId) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => Arc::clone(f),
			None => return Ok(()),
		};

		let mut by_version: BTreeMap<CommitVersion, Vec<Change>> = BTreeMap::new();
		for change in changes {
			by_version.entry(change.version).or_default().push(change);
		}
		Span::current().record("version_count", by_version.len());

		let topo = flow.topological_order()?;
		let mut nodes_processed = 0u32;

		for (_, version_changes) in by_version {
			let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
			for change in version_changes {
				self.seed_entry_nodes(&flow, flow_id, change, &mut pending);
			}

			for node_id in &topo {
				let inbox = match pending.remove(node_id) {
					Some(v) if !v.is_empty() => v,
					_ => continue,
				};

				let node = match flow.get_node(node_id) {
					Some(n) => n.clone(),
					None => continue,
				};

				let combined_output = self.dispatch_node(txn, &node, inbox)?;
				nodes_processed += 1;
				if combined_output.diffs.is_empty() {
					continue;
				}

				for child_id in &node.outputs {
					pending.entry(*child_id).or_default().push(combined_output.clone());
				}
			}
		}

		Span::current().record("nodes_processed", nodes_processed);
		Ok(())
	}

	fn seed_entry_nodes(
		&self,
		flow: &FlowDag,
		flow_id: FlowId,
		change: Change,
		pending: &mut HashMap<FlowNodeId, Vec<Change>>,
	) {
		match &change.origin {
			ChangeOrigin::Shape(source) => {
				if let Some(registrations) = self.sources.get(source) {
					for (registered_flow_id, node_id) in registrations {
						if *registered_flow_id != flow_id {
							continue;
						}
						if flow.get_node(node_id).is_none() {
							continue;
						}
						let routed = Change {
							origin: ChangeOrigin::Flow(*node_id),
							version: change.version,
							diffs: change.diffs.clone(),
							changed_at: change.changed_at,
						};
						pending.entry(*node_id).or_default().push(routed);
					}
				}
			}
			ChangeOrigin::Flow(node_id) => {
				if flow.get_node(node_id).is_some() {
					pending.entry(*node_id).or_default().push(change);
				}
			}
		}
	}

	fn dispatch_node(&self, txn: &mut FlowTransaction, node: &FlowNode, inbox: Vec<Change>) -> Result<Change> {
		let merged = Change::merge(inbox)?;
		let version = merged.version;
		let changed_at = merged.changed_at;
		let result = self.apply(txn, node, merged)?;
		let combined = Change::from_flow(node.id, version, result.diffs, changed_at.max(result.changed_at));
		Ok(combined)
	}

	#[instrument(name = "flow::engine::apply", level = "trace", skip(self, txn, change, node), fields(
		node_id = ?node.id,
		node_type = node.ty.label(),
		num_parents = node.inputs.len(),
		input_diffs = change.diffs.len(),
		input_rows = field::Empty,
		output_diffs_raw = field::Empty,
		output_diffs = field::Empty,
		output_rows = field::Empty,
		lock_wait_us = field::Empty,
		apply_time_us = field::Empty,
		coalesce_time_us = field::Empty
	))]
	fn apply(&self, txn: &mut FlowTransaction, node: &FlowNode, change: Change) -> Result<Change> {
		let lock_start = self.runtime_context.clock.instant();
		let operator = self.operators.get(&node.id).unwrap().clone();
		Span::current().record("lock_wait_us", lock_start.elapsed().as_micros() as u64);

		Span::current().record("input_rows", change.row_count());

		let apply_start = self.runtime_context.clock.instant();
		let result = operator.apply(txn, change)?;
		Span::current().record("apply_time_us", apply_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs_raw", result.diffs.len());

		let coalesce_start = self.runtime_context.clock.instant();
		Span::current().record("coalesce_time_us", coalesce_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs", result.diffs.len());
		Span::current().record("output_rows", result.row_count());
		Ok(result)
	}

	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		timestamp = %timestamp
	))]
	pub fn process_tick(&self, txn: &mut FlowTransaction, flow_id: FlowId, timestamp: DateTime) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => Arc::clone(f),
			None => return Ok(()),
		};

		let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
		for node_id in flow.topological_order()? {
			let node = match flow.get_node(&node_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			if let Some(inbox) = pending.remove(&node_id).filter(|v| !v.is_empty()) {
				let combined_output = self.dispatch_node(txn, &node, inbox)?;
				if !combined_output.diffs.is_empty() {
					for child_id in &node.outputs {
						pending.entry(*child_id).or_default().push(combined_output.clone());
					}
				}
			}

			let operator = match self.operators.get(&node_id) {
				Some(op) => op.clone(),
				None => continue,
			};
			let interval = match operator.ticks() {
				Some(interval) => interval,
				None => continue,
			};
			if matches!(&*operator, Operators::Custom(_) | Operators::Apply(_))
				&& !self.operator_due(node_id, timestamp.to_nanos(), interval)
			{
				continue;
			}
			if let Some(tick_emission) = operator.tick(
				txn,
				Tick {
					now: timestamp,
				},
			)? && !tick_emission.diffs.is_empty()
			{
				for child_id in &node.outputs {
					pending.entry(*child_id).or_default().push(tick_emission.clone());
				}
			}
		}

		self.emit_operator_drop_metrics(txn);
		Ok(())
	}

	fn emit_operator_drop_metrics(&self, txn: &FlowTransaction) {
		let mut per_node: HashMap<FlowNodeId, u64> = HashMap::new();
		for (key, write) in txn.pending().iter_sorted() {
			if !matches!(write, PendingWrite::Drop) {
				continue;
			}
			let node = FlowNodeStateKey::decode(key)
				.map(|k| k.node)
				.or_else(|| FlowNodeInternalStateKey::decode(key).map(|k| k.node));
			if let Some(node) = node {
				*per_node.entry(node).or_default() += 1;
			}
		}

		if per_node.is_empty() {
			return;
		}

		let rows: u64 = per_node.values().copied().sum();
		self.event_bus.emit(OperatorRowsExpiredEvent::new(
			per_node.len() as u64,
			0,
			rows,
			rows,
			per_node.clone(),
			per_node,
		));
	}
}
