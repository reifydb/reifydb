// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_engine::StandardCommandTransaction;
use reifydb_rql::flow::{
	Flow, FlowNode,
	FlowNodeType::{SourceInlineData, SourceTable, SourceView},
};

use crate::{
	engine::FlowEngine,
	flow::{FlowChange, FlowChangeOrigin},
};

impl FlowEngine {
	pub fn process(&self, txn: &mut StandardCommandTransaction, change: FlowChange) -> crate::Result<()> {
		match change.origin {
			FlowChangeOrigin::External(source) => {
				if let Some(node_registrations) = self.sources.get(&source) {
					for (flow_id, node_id) in node_registrations {
						if let Some(flow) = self.flows.get(flow_id) {
							if let Some(node) = flow.get_node(node_id) {
								self.process_node(
									txn,
									flow,
									node,
									FlowChange::internal(
										*node_id,
										change.version,
										change.diffs.clone(),
									),
								)?;
							}
						}
					}
				}
			}
			_ => unreachable!(),
		}
		Ok(())
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<FlowChange> {
		let operator = self.operators.get(&node.id).unwrap();
		let result = operator.apply(txn, change, &self.evaluator)?;
		Ok(result)
	}

	fn process_node(
		&self,
		txn: &mut StandardCommandTransaction,
		flow: &Flow,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<()> {
		let node_type = &node.ty;
		let changes = &node.outputs;

		let change = match &node_type {
			SourceInlineData {} => unimplemented!(),
			SourceTable {
				..
			} => change,
			SourceView {
				..
			} => change,
			_ => self.apply(txn, node, change)?,
		};

		// Propagate to downstream
		if changes.is_empty() {
		} else if changes.len() == 1 {
			let output_id = changes[0];
			self.process_node(txn, flow, flow.get_node(&output_id).unwrap(), change)?;
		} else {
			let (last, rest) = changes.split_last().unwrap();
			for output_id in rest {
				self.process_node(txn, flow, flow.get_node(output_id).unwrap(), change.clone())?;
			}
			self.process_node(txn, flow, flow.get_node(last).unwrap(), change)?;
		}

		Ok(())
	}
}
