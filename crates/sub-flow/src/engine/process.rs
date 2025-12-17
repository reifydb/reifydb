// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::FlowId;
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin};
use reifydb_rql::flow::{Flow, FlowNode, FlowNodeType::SourceInlineData};
use tracing::instrument;

use crate::{engine::FlowEngine, transaction::FlowTransaction};

impl FlowEngine {
	#[instrument(name = "flow::process", level = "debug", skip(self, txn), fields(flow_id = ?flow_id, origin = ?change.origin, version = change.version.0, diff_count = change.diffs.len()))]
	pub fn process(&self, txn: &mut FlowTransaction, change: FlowChange, flow_id: FlowId) -> crate::Result<()> {
		match change.origin {
			FlowChangeOrigin::External(source) => {
				let sources = self.inner.sources.read();
				if let Some(node_registrations) = sources.get(&source) {
					// Clone the node registrations to avoid holding the lock while processing
					let node_registrations = node_registrations.clone();
					drop(sources);

					for (flow_id, node_id) in node_registrations {
						let flows = self.inner.flows.read();
						if let Some(flow) = flows.get(&flow_id) {
							if let Some(node) = flow.get_node(&node_id) {
								let flow = flow.clone();
								let node = node.clone();
								drop(flows);

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
							} else {
								drop(flows);
							}
						} else {
							drop(flows);
						}
					}
				}
			}
			FlowChangeOrigin::Internal(node_id) => {
				// Internal changes are already scoped to a specific node
				// This path is used by the partition logic to directly process a node's changes
				// Use the flow_id parameter for direct lookup instead of iterating all flows
				let flows = self.inner.flows.read();
				if let Some(flow) = flows.get(&flow_id) {
					if let Some(node) = flow.get_node(&node_id) {
						let flow = flow.clone();
						let node = node.clone();
						drop(flows);

						self.process_change(txn, &flow, &node, change)?;
					}
				}
			}
		}
		Ok(())
	}

	#[instrument(name = "flow::apply", level = "trace", skip(self, txn), fields(node_id = ?node.id, input_diffs = change.diffs.len(), output_diffs))]
	fn apply(&self, txn: &mut FlowTransaction, node: &FlowNode, change: FlowChange) -> crate::Result<FlowChange> {
		let operator = self.inner.operators.read().get(&node.id).unwrap().clone();
		let result = operator.apply(txn, change, &self.inner.evaluator)?;
		tracing::Span::current().record("output_diffs", result.diffs.len());
		Ok(result)
	}

	#[instrument(name = "flow::process::change", level = "trace", skip(self, txn, flow), fields(flow_id = ?flow.id, node_id = ?node.id, input_diffs = change.diffs.len(), output_diffs))]
	fn process_change(
		&self,
		txn: &mut FlowTransaction,
		flow: &Flow,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<()> {
		let node_type = &node.ty;
		let changes = &node.outputs;

		let change = match &node_type {
			SourceInlineData {} => unimplemented!(),
			_ => {
				let result = self.apply(txn, node, change)?;
				tracing::Span::current().record("output_diffs", result.diffs.len());
				result
			}
		};

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

		Ok(())
	}
}
