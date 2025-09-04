// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{
		Flow, FlowChange, FlowDiff, FlowNode, FlowNodeType,
		FlowNodeType::{SourceInlineData, SourceTable},
	},
	interface::{
		CommandTransaction, EncodableKey, Evaluator,
		GetEncodedRowLayout, RowKey, SourceId, ViewId,
	},
	log_debug,
};
use reifydb_type::Value;

use crate::{engine::FlowEngine, operator::OperatorContext};

impl<E: Evaluator> FlowEngine<E> {
	pub fn process<T: CommandTransaction>(
		&self,
		txn: &mut T,
		change: FlowChange,
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
				use reifydb_core::log_debug;
				log_debug!(
					"FlowEngine: Source {:?} triggers {} flows",
					source,
					flow_ids.len()
				);
				// Process the diffs once for all flows with
				// this source
				let bulkchange = FlowChange {
					diffs,
					metadata: change.metadata.clone(),
				};

				for flow_id in flow_ids {
					if let Some(flow) =
						self.flows.get(flow_id)
					{
						log_debug!(
							"FlowEngine: Processing flow {:?} for source {:?}",
							flow_id,
							source
						);
						// Find the source node in the
						// flow that matches this source
						if let Some(node) =
							find_source_node(
								flow, &source,
							) {
							log_debug!(
								"FlowEngine: Found source node {:?} in flow {:?}",
								node.id,
								flow_id
							);
							// Process this node
							// with all diffs for
							// this source
							self.process_node(
								txn,
								flow,
								node,
								&bulkchange,
							)?;
						} else {
							log_debug!(
								"FlowEngine: No source node found for {:?} in flow {:?}",
								source,
								flow_id
							);
						}
					}
				}
			} else {
				log_debug!(
					"FlowEngine: No flows registered for source {:?}",
					source
				);
			}
		}
		Ok(())
	}

	fn apply_operator<T: CommandTransaction>(
		&self,
		txn: &mut T,
		node: &FlowNode,
		change: &FlowChange,
	) -> crate::Result<FlowChange> {
		let operator = self.operators.get(&node.id).unwrap();
		let mut context = OperatorContext::new(&self.evaluator, txn);
		operator.apply(&mut context, change)
	}

	fn process_node<T: CommandTransaction>(
		&self,
		txn: &mut T,
		flow: &Flow,
		node: &FlowNode,
		change: &FlowChange,
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
			FlowNodeType::Operator {
				..
			} => &self.apply_operator(txn, node, &change)?,
			FlowNodeType::SinkView {
				view,
				..
			} => {
				log_debug!(
					"FlowEngine: Applying {} diffs to view {:?}",
					change.diffs.len(),
					view
				);
				for diff in &change.diffs {
					match diff {
						FlowDiff::Insert {
							row_ids,
							..
						} => {
							log_debug!(
								"FlowEngine: Inserting {} rows to view {:?}: {:?}",
								row_ids.len(),
								view,
								row_ids
							);
						}
						_ => {}
					}
				}
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
				output,
			)?;
		}

		Ok(())
	}

	fn apply_to_view<T: CommandTransaction>(
		&self,
		txn: &mut T,
		view_id: ViewId,
		change: &FlowChange,
	) -> crate::Result<()> {
		let view = CatalogStore::get_view(txn, view_id)?;
		let layout = view.get_layout();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					row_ids,
					after,
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

					for (row_idx, &row_id) in
						row_ids.iter().enumerate()
					{
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
                                Value::RowNumber(_v) => {}
                                Value::IdentityId(v) => layout.set_identity_id(&mut row, view_idx, v),
                                Value::Uuid4(v) => layout.set_uuid4(&mut row, view_idx, v),
                                Value::Uuid7(v) => layout.set_uuid7(&mut row, view_idx, v),
                                Value::Blob(v) => layout.set_blob(&mut row, view_idx, &v),
                                Value::VarInt(v) => layout.set_varint(&mut row, view_idx, &v),
                                Value::VarUint(v) => layout.set_varuint(&mut row, view_idx, &v),
                                Value::BigDecimal(v) => layout.set_bigdecimal(&mut row, view_idx, &v),
                                Value::Undefined => layout.set_undefined(&mut row, view_idx)}
						}

						// Use the row_id from the diff
						// for consistent addressing
						// Check if this row already
						// exists (for idempotent
						// updates)
						let key = RowKey {
							source: SourceId::view(
								view_id,
							),
							row: row_id,
						}
						.encode();

						log_debug!(
							"Writing row to view {:?} with row_id {:?}, key: {:?}",
							view_id,
							row_id,
							key
						);

						// Insert or update the row
						txn.set(&key, row)?;
					}
				}
				FlowDiff::Update {
					row_ids,
					before: _,
					after,
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

					for (row_idx, &row_id) in
						row_ids.iter().enumerate()
					{
						// Build the new row
						let mut new_row =
							layout.allocate_row();

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
								Value::Bool(v) => layout.set_bool(&mut new_row, view_idx, v),
								Value::Float4(v) => layout.set_f32(&mut new_row, view_idx, *v),
								Value::Float8(v) => layout.set_f64(&mut new_row, view_idx, *v),
								Value::Int1(v) => layout.set_i8(&mut new_row, view_idx, v),
								Value::Int2(v) => layout.set_i16(&mut new_row, view_idx, v),
								Value::Int4(v) => layout.set_i32(&mut new_row, view_idx, v),
								Value::Int8(v) => layout.set_i64(&mut new_row, view_idx, v),
								Value::Int16(v) => layout.set_i128(&mut new_row, view_idx, v),
								Value::Utf8(v) => layout.set_utf8(&mut new_row, view_idx, v),
								Value::Uint1(v) => layout.set_u8(&mut new_row, view_idx, v),
								Value::Uint2(v) => layout.set_u16(&mut new_row, view_idx, v),
								Value::Uint4(v) => layout.set_u32(&mut new_row, view_idx, v),
								Value::Uint8(v) => layout.set_u64(&mut new_row, view_idx, v),
								Value::Uint16(v) => layout.set_u128(&mut new_row, view_idx, v),
								Value::Date(v) => layout.set_date(&mut new_row, view_idx, v),
								Value::DateTime(v) => layout.set_datetime(&mut new_row, view_idx, v),
								Value::Time(v) => layout.set_time(&mut new_row, view_idx, v),
								Value::Interval(v) => layout.set_interval(&mut new_row, view_idx, v),
								Value::RowNumber(_v) => {},
								Value::IdentityId(v) => layout.set_identity_id(&mut new_row, view_idx, v),
								Value::Uuid4(v) => layout.set_uuid4(&mut new_row, view_idx, v),
								Value::Uuid7(v) => layout.set_uuid7(&mut new_row, view_idx, v),
								Value::Blob(v) => layout.set_blob(&mut new_row, view_idx, &v),
								Value::VarInt(v) => layout.set_varint(&mut new_row, view_idx, &v),
							Value::VarUint(v) => layout.set_varuint(&mut new_row, view_idx, &v),
								Value::BigDecimal(v) => layout.set_bigdecimal(&mut new_row, view_idx, &v),
								Value::Undefined => layout.set_undefined(&mut new_row, view_idx)}
						}

						// Directly update the row using
						// its row_id
						let key = RowKey {
							source: SourceId::view(
								view_id,
							),
							row: row_id,
						}
						.encode();

						txn.set(&key, new_row)?;
					}
				}
				FlowDiff::Remove {
					row_ids,
					before,
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
					for &row_id in row_ids {
						let key = RowKey {
							source: SourceId::view(
								view_id,
							),
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

/// Find the source node in a flow that corresponds to the given source
fn find_source_node<'a>(
	flow: &'a Flow<'static>,
	source: &SourceId,
) -> Option<&'a FlowNode<'static>> {
	for node_id in flow.get_node_ids() {
		if let Some(node) = flow.get_node(&node_id) {
			if let SourceTable {
				table,
				..
			} = &node.ty
			{
				if *source == SourceId::table(*table) {
					return Some(node);
				}
			}
		}
	}
	None
}
