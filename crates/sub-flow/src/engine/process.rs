// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::{
	flow::{
		Flow, FlowChange, FlowDiff, FlowNode, FlowNodeType,
		FlowNodeType::{SourceInlineData, SourceTable, SourceView},
	},
	interface::{EncodableKey, MultiVersionCommandTransaction, RowKey, SourceId, Transaction, ViewId},
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
			// Find all nodes triggered by this source
			if let Some(node_registrations) = self.sources.get(&source) {
				// Process the diffs for each registered node
				for (flow_id, node_id) in node_registrations {
					if let Some(flow) = self.flows.get(flow_id) {
						if let Some(node) = flow.get_node(node_id) {
							let bulkchange = FlowChange {
								diffs: diffs.clone(),
							};
							// Process this specific
							// node with the change
							self.process_node(txn, flow, node, bulkchange)?;
						}
					}
				}
			}
		}
		Ok(())
	}

	fn apply_operator(
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
		let node_outputs = &node.outputs;

		let output = match &node_type {
			SourceInlineData {} => {
				unimplemented!()
			}
			SourceTable {
				..
			} => {
				// Source nodes just propagate the change
				change
			}
			SourceView {
				..
			} => {
				// Source view nodes also propagate the change
				// This enables view-to-view dependencies
				change
			}
			FlowNodeType::Operator {
				..
			} => self.apply_operator(txn, node, change)?,
			FlowNodeType::SinkView {
				view,
				..
			} => {
				// Sinks persist the final results
				// View writes will generate CDC events that
				// trigger dependent flows
				self.apply_to_view(txn, *view, &change)?;
				change
			}
		};

		// Propagate to downstream nodes
		if node_outputs.is_empty() {
			// No outputs, nothing to do
		} else if node_outputs.len() == 1 {
			// Single output - pass ownership directly
			let output_id = node_outputs[0];
			self.process_node(txn, flow, flow.get_node(&output_id).unwrap(), output)?;
		} else {
			// Multiple outputs - clone for all but the last
			let (last, rest) = node_outputs.split_last().unwrap();
			for output_id in rest {
				self.process_node(txn, flow, flow.get_node(output_id).unwrap(), output.clone())?;
			}
			// Last output gets ownership
			self.process_node(txn, flow, flow.get_node(last).unwrap(), output)?;
		}

		Ok(())
	}

	fn apply_to_view(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		view_id: ViewId,
		change: &FlowChange,
	) -> crate::Result<()> {
		// For now, we just directly write the row to the view
		// TODO: This assumes source and view layouts are compatible

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					post: row_data,
					..
				} => {
					let row_id = row_data.number;
					let row = row_data.encoded.clone();

					let key = RowKey {
						source: SourceId::view(view_id),
						row: row_id,
					}
					.encode();

					txn.set(&key, row)?;
				}
				FlowDiff::Update {
					pre: _,
					post: row_data,
					..
				} => {
					let row_id = row_data.number;
					let new_row = row_data.encoded.clone();

					let key = RowKey {
						source: SourceId::view(view_id),
						row: row_id,
					}
					.encode();

					txn.set(&key, new_row)?;
				}
				FlowDiff::Remove {
					pre: row_data,
					..
				} => {
					let row_id = row_data.number;

					let key = RowKey {
						source: SourceId::view(view_id),
						row: row_id,
					}
					.encode();

					txn.remove(&key)?;
				}
			}
		}

		Ok(())
	}
}
