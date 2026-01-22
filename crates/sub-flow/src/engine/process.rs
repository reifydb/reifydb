// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_rql::flow::{
	flow::FlowDag,
	node::{FlowNode, FlowNodeType::SourceInlineData},
};
use reifydb_sdk::flow::{FlowChange, FlowChangeOrigin};
use tracing::{Span, instrument};

use crate::{engine::FlowEngine, transaction::FlowTransaction};

impl FlowEngine {
	#[instrument(name = "flow::engine::process", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		origin = ?change.origin,
		version = change.version.0,
		diff_count = change.diffs.len(),
		nodes_processed = tracing::field::Empty
	))]
	pub fn process(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		flow_id: FlowId,
	) -> reifydb_type::Result<()> {
		let mut nodes_processed = 0;

		match change.origin {
			FlowChangeOrigin::External(source) => {
				let node_registrations = self.sources.get(&source).cloned();

				if let Some(node_registrations) = node_registrations {
					for (registered_flow_id, node_id) in node_registrations {
						// Only process the flow that was passed as parameter
						if registered_flow_id != flow_id {
							continue;
						}

						let flow_and_node =
							self.flows.get(&registered_flow_id).and_then(|flow| {
								flow.get_node(&node_id)
									.map(|node| (flow.clone(), node.clone()))
							});

						if let Some((flow, node)) = flow_and_node {
							self.process_change(
								txn,
								&flow,
								&node,
								FlowChange::internal(
									node_id,
									change.version,
									change.diffs.clone(),
								),
							)?;
							nodes_processed += 1;
						}
					}
				}
			}
			FlowChangeOrigin::Internal(node_id) => {
				// Internal changes are already scoped to a specific node
				// This path is used by the partition logic to directly process a node's changes
				// Use the flow_id parameter for direct lookup instead of iterating all flows
				let flow_and_node = self.flows.get(&flow_id).and_then(|flow| {
					flow.get_node(&node_id).map(|node| (flow.clone(), node.clone()))
				});

				if let Some((flow, node)) = flow_and_node {
					self.process_change(txn, &flow, &node, change)?;
					nodes_processed += 1;
				}
			}
		}

		Span::current().record("nodes_processed", nodes_processed);
		Ok(())
	}

	#[instrument(name = "flow::engine::apply", level = "trace", skip(self, txn), fields(
		node_id = ?node.id,
		node_type = ?node.ty,
		input_diffs = change.diffs.len(),
		output_diffs = tracing::field::Empty,
		lock_wait_us = tracing::field::Empty,
		apply_time_us = tracing::field::Empty
	))]
	fn apply(
		&self,
		txn: &mut FlowTransaction,
		node: &FlowNode,
		change: FlowChange,
	) -> reifydb_type::Result<FlowChange> {
		let lock_start = self.clock.instant();
		let operator = self.operators.get(&node.id).unwrap().clone();
		Span::current().record("lock_wait_us", lock_start.elapsed().as_micros() as u64);

		let apply_start = self.clock.instant();
		let result = operator.apply(txn, change, &self.evaluator)?;
		Span::current().record("apply_time_us", apply_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs", result.diffs.len());
		Ok(result)
	}

	#[instrument(name = "flow::engine::process_change", level = "trace", skip(self, txn, flow), fields(
		flow_id = ?flow.id,
		node_id = ?node.id,
		input_diffs = change.diffs.len(),
		output_diffs = tracing::field::Empty,
		downstream_count = node.outputs.len(),
		propagation_time_us = tracing::field::Empty
	))]
	fn process_change(
		&self,
		txn: &mut FlowTransaction,
		flow: &FlowDag,
		node: &FlowNode,
		change: FlowChange,
	) -> reifydb_type::Result<()> {
		let node_type = &node.ty;
		let changes = &node.outputs;

		let change = match &node_type {
			SourceInlineData {} => unimplemented!(),
			_ => {
				let result = self.apply(txn, node, change)?;
				Span::current().record("output_diffs", result.diffs.len());
				result
			}
		};

		let propagation_start = self.clock.instant();
		if changes.is_empty() {
		} else if changes.len() == 1 {
			let output_id = changes[0];
			self.process_change(txn, flow, flow.get_node(&output_id).unwrap(), change)?;
		} else {
			let (last, rest) = changes.split_last().unwrap();
			for output_id in rest {
				self.process_change(txn, flow, flow.get_node(output_id).unwrap(), change.clone())?;
			}
			self.process_change(txn, flow, flow.get_node(last).unwrap(), change)?;
		}
		Span::current().record("propagation_time_us", propagation_start.elapsed().as_micros() as u64);

		Ok(())
	}
}
