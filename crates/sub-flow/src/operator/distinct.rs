// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CowVec, Error,
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::{EncodedRow, EncodedRowLayout, EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Blob, Params, RowNumber, Type, internal_error};
use serde::{Deserialize, Serialize};

use crate::{
	flow::{FlowChange, FlowDiff},
	operator::{
		Operator,
		stateful::{RawStatefulOperator, SingleStateful},
		transform::TransformOperator,
	},
};

static EMPTY_PARAMS: Params = Params::None;

/// Layout information shared across all rows
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctLayout {
	names: Vec<String>,
	types: Vec<Type>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedRow {
	number: RowNumber,
	#[serde(with = "serde_bytes")]
	encoded_bytes: Vec<u8>,
}

impl SerializedRow {
	fn from_row(row: &Row) -> Self {
		Self {
			number: row.number,
			encoded_bytes: row.encoded.as_slice().to_vec(),
		}
	}

	fn to_row(self, layout: &DistinctLayout) -> Row {
		let fields: Vec<(String, Type)> =
			layout.names.iter().cloned().zip(layout.types.iter().cloned()).collect();

		let layout = EncodedRowNamedLayout::new(fields);
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes));

		Row {
			number: self.number,
			encoded,
			layout,
		}
	}
}

impl DistinctLayout {
	fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	/// Update the layout with a new row, keeping the most defined types
	fn update_from_row(&mut self, row: &Row) {
		let names = row.layout.names();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		if self.names.is_empty() {
			self.names = names.to_vec();
			self.types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		// Never turn a defined type into undefined
		for (i, new_type) in types.iter().enumerate() {
			if i < self.types.len() {
				// Keep the more defined type
				// If current is Undefined and new is not, update to new type
				if self.types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.types[i] = *new_type;
				}
			} else {
				// New field
				self.types.push(*new_type);
				if i < names.len() {
					self.names.push(names[i].clone());
				}
			}
		}
	}
}

/// Entry for tracking distinct values
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctEntry {
	/// Number of times this distinct value appears
	count: usize,
	/// The first row that had this distinct value
	first_row: SerializedRow,
}

/// State for tracking distinct values
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctState {
	/// Map from hash of distinct expressions to entry
	entries: HashMap<Hash128, DistinctEntry>,
	/// Shared layout information
	layout: DistinctLayout,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: HashMap::new(),
			layout: DistinctLayout::new(),
		}
	}
}

pub struct DistinctOperator {
	node: FlowNodeId,
	expressions: Vec<Expression<'static>>,
	layout: EncodedRowLayout,
}

impl DistinctOperator {
	pub fn new(node: FlowNodeId, expressions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			expressions,
			layout: EncodedRowLayout::new(&[Type::Blob]),
		}
	}

	fn compute_hash(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<Hash128> {
		let ctx = RowEvaluationContext {
			row: row.clone(),
			target: None,
			params: &EMPTY_PARAMS,
		};

		let mut data = Vec::new();

		if self.expressions.is_empty() {
			// Hash the entire row if no expressions
			data.extend_from_slice(row.encoded.as_slice());
		} else {
			for expr in &self.expressions {
				let value = evaluator.evaluate(&ctx, expr)?;
				let value_str = value.to_string();
				data.extend_from_slice(value_str.as_bytes());
			}
		}

		Ok(xxh3_128(&data))
	}

	fn load_distinct_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
	) -> crate::Result<DistinctState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DistinctState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DistinctState::default());
		}

		let config = standard();
		decode_from_slice(blob.as_ref(), config)
			.map(|(state, _)| state)
			.map_err(|e| Error(internal_error!("Failed to deserialize DistinctState: {}", e)))
	}

	fn save_distinct_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		state: &DistinctState,
	) -> crate::Result<()> {
		let config = standard();
		let serialized = encode_to_vec(state, config)
			.map_err(|e| Error(internal_error!("Failed to serialize DistinctState: {}", e)))?;

		let mut state_row = self.layout.allocate_row();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, state_row)
	}
}

impl<T: Transaction> TransformOperator<T> for DistinctOperator {}

impl<T: Transaction> RawStatefulOperator<T> for DistinctOperator {}

impl<T: Transaction> SingleStateful<T> for DistinctOperator {
	fn layout(&self) -> EncodedRowLayout {
		self.layout.clone()
	}
}

impl<T: Transaction> Operator<T> for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let mut state = self.load_distinct_state(txn)?;
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					state.layout.update_from_row(&post);

					let hash = self.compute_hash(&post, evaluator)?;

					match state.entries.get_mut(&hash) {
						Some(entry) => {
							entry.count += 1;
							// Already seen this distinct value - just increment count
							// Don't emit anything since it's a duplicate
						}
						None => {
							state.entries.insert(
								hash,
								DistinctEntry {
									count: 1,
									first_row: SerializedRow::from_row(&post),
								},
							);
							result.push(FlowDiff::Insert {
								post,
							});
						}
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Update layout with this row's types
					state.layout.update_from_row(&post);

					// Compute hashes for both old and new values
					let pre_hash = self.compute_hash(&pre, evaluator)?;
					let post_hash = self.compute_hash(&post, evaluator)?;

					if pre_hash == post_hash {
						// Distinct key didn't change - update the stored row
						if let Some(entry) = state.entries.get_mut(&pre_hash) {
							// Update to use the new row data
							if entry.first_row.number == post.number {
								entry.first_row = SerializedRow::from_row(&post);
							}
							result.push(FlowDiff::Update {
								pre,
								post,
							});
						}
					} else {
						if let Some(entry) = state.entries.get_mut(&pre_hash) {
							if entry.count > 1 {
								entry.count -= 1;
							} else {
								// Last instance - remove from state
								state.entries.remove(&pre_hash);
								result.push(FlowDiff::Remove {
									pre,
								});
							}
						}

						match state.entries.get_mut(&post_hash) {
							Some(entry) => {
								entry.count += 1;
							}
							None => {
								state.entries.insert(
									post_hash,
									DistinctEntry {
										count: 1,
										first_row: SerializedRow::from_row(
											&post,
										),
									},
								);
								result.push(FlowDiff::Insert {
									post,
								});
							}
						}
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					let hash = self.compute_hash(&pre, evaluator)?;

					if let Some(entry) = state.entries.get_mut(&hash) {
						if entry.count > 1 {
							entry.count -= 1;
						} else {
							let removed_entry = state.entries.remove(&hash);
							if let Some(entry) = removed_entry {
								let stored_row = entry.first_row.to_row(&state.layout);
								result.push(FlowDiff::Remove {
									pre: stored_row,
								});
							}
						}
					}
				}
			}
		}

		self.save_distinct_state(txn, &state)?;

		Ok(FlowChange::internal(self.node, change.version, result))
	}
}
