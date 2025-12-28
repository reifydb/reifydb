// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use indexmap::IndexMap;
use reifydb_core::{
	CowVec, Error, Row,
	interface::FlowNodeId,
	value::{
		column::Columns,
		encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
	},
};
use reifydb_engine::{ColumnEvaluationContext, StandardColumnEvaluator, stack::Stack};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::expression::Expression;
use reifydb_sdk::{FlowChange, FlowDiff};
use reifydb_type::{Blob, Params, RowNumber, Type, internal};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{
		Operator, Operators,
		stateful::{RawStatefulOperator, SingleStateful},
		transform::TransformOperator,
	},
	transaction::FlowTransaction,
};
static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

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

	fn from_columns(columns: &Columns) -> Self {
		// For single-row Columns, extract the row for serialization
		let row = columns.to_single_row();
		Self::from_row(&row)
	}

	fn to_row(self, layout: &DistinctLayout) -> Row {
		let fields: Vec<(String, Type)> =
			layout.names.iter().cloned().zip(layout.types.iter().cloned()).collect();

		let layout = EncodedValuesNamedLayout::new(fields);
		let encoded = EncodedValues(CowVec::new(self.encoded_bytes));

		Row {
			number: self.number,
			encoded,
			layout,
		}
	}

	fn to_columns(self, layout: &DistinctLayout) -> Columns {
		Columns::from_row(&self.to_row(layout))
	}
}

impl DistinctLayout {
	fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	/// Update the layout from Columns (uses first row if multiple)
	fn update_from_columns(&mut self, columns: &Columns) {
		if columns.is_empty() {
			return;
		}

		let names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
		let types: Vec<Type> = columns.iter().map(|c| c.data().get_type()).collect();

		if self.names.is_empty() {
			self.names = names;
			self.types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		for (i, new_type) in types.iter().enumerate() {
			if i < self.types.len() {
				if self.types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.types[i] = *new_type;
				}
			} else {
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
	/// Using IndexMap to preserve insertion order for "first occurrence" semantics
	entries: IndexMap<Hash128, DistinctEntry>,
	/// Shared layout information
	layout: DistinctLayout,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: IndexMap::new(),
			layout: DistinctLayout::new(),
		}
	}
}

pub struct DistinctOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	expressions: Vec<Expression>,
	layout: EncodedValuesLayout,
	column_evaluator: StandardColumnEvaluator,
}

impl DistinctOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			expressions,
			layout: EncodedValuesLayout::new(&[Type::Blob]),
			column_evaluator: StandardColumnEvaluator::default(),
		}
	}

	/// Compute hashes for all rows in Columns
	fn compute_hashes(&self, columns: &Columns) -> crate::Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.expressions.is_empty() {
			// Hash the entire row data for each row
			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in columns.iter() {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		} else {
			let ctx = ColumnEvaluationContext {
				target: None,
				columns: columns.clone(),
				row_count,
				take: None,
				params: &EMPTY_PARAMS,
				stack: &EMPTY_STACK,
				is_aggregate_context: false,
			};

			// Evaluate each expression on entire batch
			let mut expr_columns = Vec::new();
			for expr in &self.expressions {
				let col = self.column_evaluator.evaluate(&ctx, expr)?;
				expr_columns.push(col);
			}

			// Compute hash for each row
			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in &expr_columns {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		}
	}

	async fn load_distinct_state(&self, txn: &mut FlowTransaction) -> crate::Result<DistinctState> {
		let state_row = self.load_state(txn).await?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DistinctState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DistinctState::default());
		}

		postcard::from_bytes(blob.as_ref())
			.map_err(|e| Error(internal!("Failed to deserialize DistinctState: {}", e)))
	}

	fn save_distinct_state(&self, txn: &mut FlowTransaction, state: &DistinctState) -> crate::Result<()> {
		let serialized = postcard::to_stdvec(state)
			.map_err(|e| Error(internal!("Failed to serialize DistinctState: {}", e)))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, state_row)
	}

	/// Process inserts
	fn process_insert(&self, state: &mut DistinctState, columns: &Columns) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(columns);
		let hashes = self.compute_hashes(columns)?;

		for row_idx in 0..row_count {
			let hash = hashes[row_idx];
			let single_row = columns.extract_row(row_idx);

			match state.entries.get_mut(&hash) {
				Some(entry) => {
					entry.count += 1;
					// Already seen this distinct value - just increment count
				}
				None => {
					state.entries.insert(
						hash,
						DistinctEntry {
							count: 1,
							first_row: SerializedRow::from_columns(&single_row),
						},
					);
					result.push(FlowDiff::Insert {
						post: single_row,
					});
				}
			}
		}

		Ok(result)
	}

	/// Process updates
	fn process_update(
		&self,
		state: &mut DistinctState,
		pre_columns: &Columns,
		post_columns: &Columns,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();
		let row_count = post_columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(post_columns);
		let pre_hashes = self.compute_hashes(pre_columns)?;
		let post_hashes = self.compute_hashes(post_columns)?;

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];
			let pre_row = pre_columns.extract_row(row_idx);
			let post_row = post_columns.extract_row(row_idx);

			if pre_hash == post_hash {
				// Distinct key didn't change - update the stored row
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					if entry.first_row.number == post_row.number() {
						entry.first_row = SerializedRow::from_columns(&post_row);
					}
					result.push(FlowDiff::Update {
						pre: pre_row,
						post: post_row,
					});
				}
			} else {
				// Key changed - remove from old, add to new
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					if entry.count > 1 {
						entry.count -= 1;
					} else {
						state.entries.shift_remove(&pre_hash);
						result.push(FlowDiff::Remove {
							pre: pre_row,
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
								first_row: SerializedRow::from_columns(&post_row),
							},
						);
						result.push(FlowDiff::Insert {
							post: post_row,
						});
					}
				}
			}
		}

		Ok(result)
	}

	/// Process removes
	fn process_remove(&self, state: &mut DistinctState, columns: &Columns) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		let hashes = self.compute_hashes(columns)?;

		for row_idx in 0..row_count {
			let hash = hashes[row_idx];

			if let Some(entry) = state.entries.get_mut(&hash) {
				if entry.count > 1 {
					entry.count -= 1;
				} else {
					let removed_entry = state.entries.shift_remove(&hash);
					if let Some(entry) = removed_entry {
						let stored_columns = entry.first_row.to_columns(&state.layout);
						result.push(FlowDiff::Remove {
							pre: stored_columns,
						});
					}
				}
			}
		}

		Ok(result)
	}
}

impl TransformOperator for DistinctOperator {}

impl RawStatefulOperator for DistinctOperator {}

impl SingleStateful for DistinctOperator {
	fn layout(&self) -> EncodedValuesLayout {
		self.layout.clone()
	}
}

#[async_trait]
impl Operator for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		let mut state = self.load_distinct_state(txn).await?;
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let insert_result = self.process_insert(&mut state, &post)?;
					result.extend(insert_result);
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let update_result = self.process_update(&mut state, &pre, &post)?;
					result.extend(update_result);
				}
				FlowDiff::Remove {
					pre,
				} => {
					let remove_result = self.process_remove(&mut state, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		self.save_distinct_state(txn, &state)?;

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		self.parent.pull(txn, rows).await
	}
}
