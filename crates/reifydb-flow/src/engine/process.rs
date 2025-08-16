// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use FlowNodeType::SourceTable;
use reifydb_catalog::sequence::ViewRowSequence;
use reifydb_core::{
	Type, Value,
	interface::{
		ActiveCommandTransaction, ColumnIndex, EncodableKey, Evaluator,
		GetEncodedRowLayout, SchemaId, SourceId, SourceId::Table,
		Transaction, VersionedCommandTransaction, ViewColumnDef,
		ViewColumnId, ViewDef, ViewId, ViewRowKey,
	},
};

use crate::{Change, Diff, Flow, FlowNode, FlowNodeType, engine::FlowEngine};

impl<E: Evaluator> FlowEngine<'_, E> {
	pub fn process<T: Transaction>(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		change: Change,
	) -> crate::Result<()> {
		let mut diffs_by_source = HashMap::new();

		for diff in change.diffs {
			diffs_by_source
				.entry(diff.source())
				.or_insert_with(Vec::new)
				.push(diff);
		}

		for (source, diffs) in diffs_by_source {
			// Find all flows triggered by this source
			if let Some(flow_ids) = self.sources.get(&source) {
				// Process the diffs once for all flows with
				// this source
				let bulk_change = Change {
					diffs,
					metadata: change.metadata.clone(),
				};

				for flow_id in flow_ids {
					if let Some(flow) =
						self.flows.get(flow_id)
					{
						// Find the source node in the
						// flow that matches this source
						if let Some(node) =
							find_source_node(
								flow, &source,
							) {
							// Process this node
							// with all diffs for
							// this source
							self.process_node(
								txn,
								flow,
								node,
								&bulk_change,
							)?;
						}
					}
				}
			}
		}
		Ok(())
	}

	fn apply_operator(
		&self,
		node: &FlowNode,
		change: &Change,
	) -> crate::Result<Change> {
		let operator = self.operators.get(&node.id).unwrap();
		let context = self.contexts.get(&node.id).unwrap();
		operator.apply(context, change)
	}

	fn process_node<T: Transaction>(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		flow: &Flow,
		node: &FlowNode,
		change: &Change,
	) -> crate::Result<()> {
		let node_type = &node.ty;
		let node_outputs = &node.outputs;

		let output_change = match &node_type {
			SourceTable {
				..
			} => {
				// Source nodes just propagate the change
				change
			}
			FlowNodeType::Operator {
				..
			} => &self.apply_operator(node, &change)?,
			FlowNodeType::SinkView {
				view,
				..
			} => {
				// Sinks persist the final results
				self.apply_to_view(txn, *view, &change)?;
				change
			}
		};

		// Propagate to downstream nodes
		for output_id in node_outputs {
			self.process_node(
				txn,
				flow,
				flow.get_node(output_id).unwrap(),
				output_change,
			)?;
		}

		Ok(())
	}

	fn apply_to_view<T: Transaction>(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		view_id: ViewId,
		change: &Change,
	) -> crate::Result<()> {
		// TODO: This is a simplified version - in production we'd get
		// the actual view definition from the catalog
		let view = ViewDef {
			id: view_id,
			schema: SchemaId(0),
			name: "view".to_string(),
			columns: vec![
				ViewColumnDef {
					id: ViewColumnId(0),
					name: "name".to_string(),
					ty: Type::Utf8,
					index: ColumnIndex(0),
				},
				ViewColumnDef {
					id: ViewColumnId(1),
					name: "age".to_string(),
					ty: Type::Int1,
					index: ColumnIndex(1),
				},
			],
		};

		let layout = view.get_layout();

		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					after,
					..
				} => {
					let row_count = after.row_count();

					for row_idx in 0..row_count {
						let mut row =
							layout.allocate_row();

						// For each view column, find if
						// it exists in the input
						// columns
						for (view_idx, view_column) in
							view.columns
								.iter()
								.enumerate()
						{
							let value = if let Some(input_column) =
                                after.iter().find(|col| col.name() == view_column.name)
                            {
                                input_column.data().get_value(row_idx)
                            } else {
                                Value::Undefined
                            };

							match value {
                                Value::Bool(v) => layout.set_bool(&mut row, view_idx, v),
                                Value::Float4(v) => layout.set_f32(&mut row, view_idx, *v),
                                Value::Float8(v) => layout.set_f64(&mut row, view_idx, *v),
                                Value::Int1(v) => layout.set_i8(&mut row, view_idx, v),
                                Value::Int2(v) => layout.set_i16(&mut row, view_idx, v),
                                Value::Int4(v) => layout.set_i32(&mut row, view_idx, v),
                                Value::Int8(v) => layout.set_i64(&mut row, view_idx, v),
                                Value::Int16(v) => layout.set_i128(&mut row, view_idx, v),
                                Value::Utf8(v) => layout.set_utf8(&mut row, view_idx, v),
                                Value::Uint1(v) => layout.set_u8(&mut row, view_idx, v),
                                Value::Uint2(v) => layout.set_u16(&mut row, view_idx, v),
                                Value::Uint4(v) => layout.set_u32(&mut row, view_idx, v),
                                Value::Uint8(v) => layout.set_u64(&mut row, view_idx, v),
                                Value::Uint16(v) => layout.set_u128(&mut row, view_idx, v),
                                Value::Date(v) => layout.set_date(&mut row, view_idx, v),
                                Value::DateTime(v) => layout.set_datetime(&mut row, view_idx, v),
                                Value::Time(v) => layout.set_time(&mut row, view_idx, v),
                                Value::Interval(v) => layout.set_interval(&mut row, view_idx, v),
                                Value::RowId(_v) => {}
                                Value::IdentityId(v) => layout.set_identity_id(&mut row, view_idx, v),
                                Value::Uuid4(v) => layout.set_uuid4(&mut row, view_idx, v),
                                Value::Uuid7(v) => layout.set_uuid7(&mut row, view_idx, v),
                                Value::Blob(v) => layout.set_blob(&mut row, view_idx, &v),
                                Value::Undefined => layout.set_undefined(&mut row, view_idx),
                            }
						}

						// Insert the row into the
						// database
						let row_id = ViewRowSequence::next_row_id(txn, view_id)?;

						txn.set(
							&ViewRowKey {
								view: view_id,
								row: row_id,
							}
							.encode(),
							row,
						)?;
					}
				}
				Diff::Update {
					..
				} => {
					// TODO: Implement update logic
					todo!(
						"Update logic not yet implemented"
					)
				}
				Diff::Remove {
					..
				} => {
					// TODO: Implement remove logic
					todo!(
						"Remove logic not yet implemented"
					)
				}
			}
		}

		Ok(())
	}
}

/// Find the source node in a flow that corresponds to the given source
fn find_source_node<'a>(
	flow: &'a Flow,
	source: &SourceId,
) -> Option<&'a FlowNode> {
	for node_id in flow.get_node_ids() {
		if let Some(node) = flow.get_node(&node_id) {
			if let SourceTable {
				table,
				..
			} = &node.ty
			{
				if *source == Table(*table) {
					return Some(node);
				}
			}
		}
	}
	None
}
