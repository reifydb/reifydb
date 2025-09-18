// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{
		Flow, FlowChange, FlowDiff, FlowNode, FlowNodeType,
		FlowNodeType::{SourceInlineData, SourceTable, SourceView},
	},
	interface::{
		EncodableKey, GetEncodedRowLayout, RowKey, SourceId, Transaction, VersionedCommandTransaction, ViewId,
	},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_type::Value;

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
		let view = CatalogStore::get_view(txn, view_id)?;
		let layout = view.get_layout();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					rows: row_ids,
					post: after,
					..
				} => {
					let row_count = after.row_count();

					// Ensure row_ids matches row count
					if row_ids.len() != row_count {
						panic!(
							"row_ids length {} doesn't match row count {}",
							row_ids.len(),
							row_count
						);
					}

					for (row_idx, &row_id) in row_ids.iter().enumerate() {
						let mut row = layout.allocate_row();

						// For each view column, find if
						// it exists in the input
						// columns
						for (view_idx, view_column) in view.columns.iter().enumerate() {
							let value = if let Some(input_column) =
								after.iter().find(|col| col.name() == view_column.name)
							{
								input_column.data().get_value(row_idx)
							} else {
								Value::Undefined
							};

							match value {
								Value::Boolean(v) => {
									layout.set_bool(&mut row, view_idx, v)
								}
								Value::Float4(v) => {
									layout.set_f32(&mut row, view_idx, *v)
								}
								Value::Float8(v) => {
									layout.set_f64(&mut row, view_idx, *v)
								}
								Value::Int1(v) => layout.set_i8(&mut row, view_idx, v),
								Value::Int2(v) => layout.set_i16(&mut row, view_idx, v),
								Value::Int4(v) => layout.set_i32(&mut row, view_idx, v),
								Value::Int8(v) => layout.set_i64(&mut row, view_idx, v),
								Value::Int16(v) => {
									layout.set_i128(&mut row, view_idx, v)
								}
								Value::Utf8(v) => {
									layout.set_utf8(&mut row, view_idx, v)
								}
								Value::Uint1(v) => layout.set_u8(&mut row, view_idx, v),
								Value::Uint2(v) => {
									layout.set_u16(&mut row, view_idx, v)
								}
								Value::Uint4(v) => {
									layout.set_u32(&mut row, view_idx, v)
								}
								Value::Uint8(v) => {
									layout.set_u64(&mut row, view_idx, v)
								}
								Value::Uint16(v) => {
									layout.set_u128(&mut row, view_idx, v)
								}
								Value::Date(v) => {
									layout.set_date(&mut row, view_idx, v)
								}
								Value::DateTime(v) => {
									layout.set_datetime(&mut row, view_idx, v)
								}
								Value::Time(v) => {
									layout.set_time(&mut row, view_idx, v)
								}
								Value::Interval(v) => {
									layout.set_interval(&mut row, view_idx, v)
								}
								Value::RowNumber(_v) => {}
								Value::IdentityId(v) => {
									layout.set_identity_id(&mut row, view_idx, v)
								}
								Value::Uuid4(v) => {
									layout.set_uuid4(&mut row, view_idx, v)
								}
								Value::Uuid7(v) => {
									layout.set_uuid7(&mut row, view_idx, v)
								}
								Value::Blob(v) => {
									layout.set_blob(&mut row, view_idx, &v)
								}
								Value::Int(v) => layout.set_int(&mut row, view_idx, &v),
								Value::Uint(v) => {
									layout.set_uint(&mut row, view_idx, &v)
								}
								Value::Decimal(v) => {
									layout.set_decimal(&mut row, view_idx, &v)
								}
								Value::Undefined => {
									layout.set_undefined(&mut row, view_idx)
								}
							}
						}

						// Use the row_id from the diff
						// for consistent addressing
						// Check if this row already
						// exists (for idempotent
						// updates)
						let key = RowKey {
							source: SourceId::view(view_id),
							row: row_id,
						}
						.encode();

						// Insert or update the row
						txn.set(&key, row)?;
					}
				}
				FlowDiff::Update {
					rows: row_ids,
					pre: _,
					post: after,
					..
				} => {
					// Use row_ids to directly update the
					// rows
					let row_count = after.row_count();

					// Ensure row_ids matches row count
					if row_ids.len() != row_count {
						panic!(
							"row_ids length {} doesn't match row count {}",
							row_ids.len(),
							row_count
						);
					}

					for (row_idx, &row_id) in row_ids.iter().enumerate() {
						// Build the new row
						let mut new_row = layout.allocate_row();

						for (view_idx, view_column) in view.columns.iter().enumerate() {
							let value = if let Some(input_column) =
								after.iter().find(|col| col.name() == view_column.name)
							{
								input_column.data().get_value(row_idx)
							} else {
								Value::Undefined
							};

							match value {
								Value::Boolean(v) => {
									layout.set_bool(&mut new_row, view_idx, v)
								}
								Value::Float4(v) => {
									layout.set_f32(&mut new_row, view_idx, *v)
								}
								Value::Float8(v) => {
									layout.set_f64(&mut new_row, view_idx, *v)
								}
								Value::Int1(v) => {
									layout.set_i8(&mut new_row, view_idx, v)
								}
								Value::Int2(v) => {
									layout.set_i16(&mut new_row, view_idx, v)
								}
								Value::Int4(v) => {
									layout.set_i32(&mut new_row, view_idx, v)
								}
								Value::Int8(v) => {
									layout.set_i64(&mut new_row, view_idx, v)
								}
								Value::Int16(v) => {
									layout.set_i128(&mut new_row, view_idx, v)
								}
								Value::Utf8(v) => {
									layout.set_utf8(&mut new_row, view_idx, v)
								}
								Value::Uint1(v) => {
									layout.set_u8(&mut new_row, view_idx, v)
								}
								Value::Uint2(v) => {
									layout.set_u16(&mut new_row, view_idx, v)
								}
								Value::Uint4(v) => {
									layout.set_u32(&mut new_row, view_idx, v)
								}
								Value::Uint8(v) => {
									layout.set_u64(&mut new_row, view_idx, v)
								}
								Value::Uint16(v) => {
									layout.set_u128(&mut new_row, view_idx, v)
								}
								Value::Date(v) => {
									layout.set_date(&mut new_row, view_idx, v)
								}
								Value::DateTime(v) => {
									layout.set_datetime(&mut new_row, view_idx, v)
								}
								Value::Time(v) => {
									layout.set_time(&mut new_row, view_idx, v)
								}
								Value::Interval(v) => {
									layout.set_interval(&mut new_row, view_idx, v)
								}
								Value::RowNumber(_v) => {}
								Value::IdentityId(v) => layout.set_identity_id(
									&mut new_row,
									view_idx,
									v,
								),
								Value::Uuid4(v) => {
									layout.set_uuid4(&mut new_row, view_idx, v)
								}
								Value::Uuid7(v) => {
									layout.set_uuid7(&mut new_row, view_idx, v)
								}
								Value::Blob(v) => {
									layout.set_blob(&mut new_row, view_idx, &v)
								}
								Value::Int(v) => {
									layout.set_int(&mut new_row, view_idx, &v)
								}
								Value::Uint(v) => {
									layout.set_uint(&mut new_row, view_idx, &v)
								}
								Value::Decimal(v) => {
									layout.set_decimal(&mut new_row, view_idx, &v)
								}
								Value::Undefined => {
									layout.set_undefined(&mut new_row, view_idx)
								}
							}
						}

						// Directly update the row using
						// its row_id
						let key = RowKey {
							source: SourceId::view(view_id),
							row: row_id,
						}
						.encode();

						txn.set(&key, new_row)?;
					}
				}
				FlowDiff::Remove {
					rows: row_ids,
					pre: before,
					..
				} => {
					// Use row_ids to directly remove the
					// rows
					let row_count = before.row_count();

					// Ensure row_ids matches row count
					if row_ids.len() != row_count {
						panic!(
							"row_ids length {} doesn't match row count {}",
							row_ids.len(),
							row_count
						);
					}

					// Remove each row by its row_id
					for &row_id in row_ids.iter() {
						let key = RowKey {
							source: SourceId::view(view_id),
							row: row_id,
						}
						.encode();

						txn.remove(&key)?;
					}
				}
			}
		}

		Ok(())
	}
}
