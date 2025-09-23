// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::{
	flow::{
		Flow, FlowChange, FlowNode,
		FlowNodeType::{SourceInlineData, SourceTable, SourceView},
	},
	interface::Transaction,
};
use reifydb_engine::StandardCommandTransaction;

use crate::engine::FlowEngine;

impl<T: Transaction> FlowEngine<T> {
	pub fn process(&self, txn: &mut StandardCommandTransaction<T>, change: FlowChange) -> crate::Result<()> {
		let mut diffs_by_source = HashMap::new();

		for diff in change.diffs {
			let source = diff.source();
			diffs_by_source.entry(source).or_insert_with(Vec::new).push(diff);
		}

		for (source, diffs) in diffs_by_source {
			if let Some(node_registrations) = self.sources.get(&source) {
				for (flow_id, node_id) in node_registrations {
					if let Some(flow) = self.flows.get(flow_id) {
						if let Some(node) = flow.get_node(node_id) {
							self.process_node(
								txn,
								flow,
								node,
								FlowChange {
									diffs: diffs.clone(),
								},
							)?;
						}
					}
				}
			}
		}
		Ok(())
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		node: &FlowNode,
		change: FlowChange,
	) -> crate::Result<FlowChange> {
		let operator = self.operators.get(&node.id).unwrap();
		let result = operator.apply(txn, change, &self.evaluator)?;
		Ok(result)
	}

	fn process_node(
		&self,
		txn: &mut StandardCommandTransaction<T>,
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
