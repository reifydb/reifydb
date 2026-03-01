// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use indexmap::IndexMap;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::Expression;
use reifydb_runtime::{
	clock::Clock,
	hash::{Hash128, xxh3_128},
};
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, blob::Blob, identity::IdentityId, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

/// Layout information shared across all rows
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctLayout {
	names: Vec<String>,
	types: Vec<Type>,
}

/// Serialized row data - stores column values directly without Row conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedRow {
	number: RowNumber,
	/// Column values serialized with postcard
	#[serde(with = "serde_bytes")]
	values_bytes: Vec<u8>,
}

impl SerializedRow {
	/// Create from Columns at a specific row index - no Row allocation
	fn from_columns_at_index(columns: &Columns, row_idx: usize) -> Self {
		let number = columns.row_numbers[row_idx];

		let values: Vec<Value> = columns.iter().map(|c| c.data().get_value(row_idx)).collect();

		// Serialize values directly with postcard
		let values_bytes = to_stdvec(&values).expect("Failed to serialize column values");

		Self {
			number,
			values_bytes,
		}
	}

	/// Convert back to Columns - no Row allocation
	fn to_columns(&self, layout: &DistinctLayout) -> Columns {
		// Deserialize values
		let values: Vec<Value> = from_bytes(&self.values_bytes).expect("Failed to deserialize column values");

		let mut columns_vec = Vec::with_capacity(layout.names.len());
		for (i, (name, typ)) in layout.names.iter().zip(layout.types.iter()).enumerate() {
			let value = values.get(i).cloned().unwrap_or(Value::none());
			let mut col_data = ColumnData::with_capacity(typ.clone(), 1);
			col_data.push_value(value);
			columns_vec.push(Column {
				name: Fragment::internal(name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(vec![self.number]),
			columns: CowVec::new(columns_vec),
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

		for (i, new_type) in types.iter().enumerate() {
			if i < self.types.len() {
				if !self.types[i].is_option() && new_type.is_option() {
					self.types[i] = new_type.clone();
				}
			} else {
				self.types.push(new_type.clone());
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
	compiled_expressions: Vec<CompiledExpr>,
	schema: Schema,
	functions: Functions,
	clock: Clock,
}

impl DistinctOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		functions: Functions,
		clock: Clock,
	) -> Self {
		let symbol_table = SymbolTable::new();
		let compile_ctx = CompileContext {
			functions: &functions,
			symbol_table: &symbol_table,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			compiled_expressions,
			schema: Schema::testing(&[Type::Blob]),
			functions,
			clock,
		}
	}

	/// Compute hashes for all rows in Columns
	fn compute_hashes(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_expressions.is_empty() {
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
			let exec_ctx = EvalContext {
				target: None,
				columns: columns.clone(),
				row_count,
				take: None,
				params: &EMPTY_PARAMS,
				symbol_table: &EMPTY_SYMBOL_TABLE,
				is_aggregate_context: false,
				functions: &self.functions,
				clock: &self.clock,
				arena: None,
				identity: IdentityId::root(),
			};
			let mut expr_columns = Vec::new();
			for compiled_expr in &self.compiled_expressions {
				let col = compiled_expr.execute(&exec_ctx)?;
				expr_columns.push(col);
			}

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

	fn load_distinct_state(&self, txn: &mut FlowTransaction) -> Result<DistinctState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DistinctState::default());
		}

		let blob = self.schema.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DistinctState::default());
		}

		from_bytes(blob.as_ref()).map_err(|e| Error(internal!("Failed to deserialize DistinctState: {}", e)))
	}

	fn save_distinct_state(&self, txn: &mut FlowTransaction, state: &DistinctState) -> Result<()> {
		let serialized =
			to_stdvec(state).map_err(|e| Error(internal!("Failed to serialize DistinctState: {}", e)))?;

		let mut state_row = self.schema.allocate();
		let blob = Blob::from(serialized);
		self.schema.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, state_row)
	}

	/// Process inserts - operates directly on Columns without Row conversion
	fn process_insert(&self, state: &mut DistinctState, columns: &Columns) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(columns);
		let hashes = self.compute_hashes(columns)?;

		let mut new_distinct_indices: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			let hash = hashes[row_idx];

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
							first_row: SerializedRow::from_columns_at_index(
								columns, row_idx,
							),
						},
					);
					new_distinct_indices.push(row_idx);
				}
			}
		}

		if !new_distinct_indices.is_empty() {
			let output = columns.extract_by_indices(&new_distinct_indices);
			result.push(Diff::Insert {
				post: output,
			});
		}

		Ok(result)
	}

	/// Process updates - operates directly on Columns without Row conversion
	fn process_update(
		&self,
		state: &mut DistinctState,
		pre_columns: &Columns,
		post_columns: &Columns,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = post_columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(post_columns);
		let pre_hashes = self.compute_hashes(pre_columns)?;
		let post_hashes = self.compute_hashes(post_columns)?;

		// Group updates by type for batch output
		let mut same_key_update_indices: Vec<usize> = Vec::new();
		let mut removed_indices: Vec<usize> = Vec::new();
		let mut inserted_indices: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];

			if pre_hash == post_hash {
				// Distinct key didn't change - update the stored row
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					if entry.first_row.number == post_columns.row_numbers[row_idx] {
						entry.first_row =
							SerializedRow::from_columns_at_index(post_columns, row_idx);
					}
					same_key_update_indices.push(row_idx);
				}
			} else {
				// Key changed - remove from old, add to new
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					if entry.count > 1 {
						entry.count -= 1;
					} else {
						state.entries.shift_remove(&pre_hash);
						removed_indices.push(row_idx);
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
								first_row: SerializedRow::from_columns_at_index(
									post_columns,
									row_idx,
								),
							},
						);
						inserted_indices.push(row_idx);
					}
				}
			}
		}

		if !same_key_update_indices.is_empty() {
			let pre_output = pre_columns.extract_by_indices(&same_key_update_indices);
			let post_output = post_columns.extract_by_indices(&same_key_update_indices);
			result.push(Diff::Update {
				pre: pre_output,
				post: post_output,
			});
		}

		if !removed_indices.is_empty() {
			let output = pre_columns.extract_by_indices(&removed_indices);
			result.push(Diff::Remove {
				pre: output,
			});
		}

		if !inserted_indices.is_empty() {
			let output = post_columns.extract_by_indices(&inserted_indices);
			result.push(Diff::Insert {
				post: output,
			});
		}

		Ok(result)
	}

	/// Process removes - operates directly on Columns without Row conversion
	fn process_remove(&self, state: &mut DistinctState, columns: &Columns) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		let hashes = self.compute_hashes(columns)?;

		let mut removed_hashes: Vec<Hash128> = Vec::new();

		for row_idx in 0..row_count {
			let hash = hashes[row_idx];

			if let Some(entry) = state.entries.get_mut(&hash) {
				if entry.count > 1 {
					entry.count -= 1;
				} else {
					removed_hashes.push(hash);
				}
			}
		}

		for hash in removed_hashes {
			if let Some(entry) = state.entries.shift_remove(&hash) {
				let stored_columns = entry.first_row.to_columns(&state.layout);
				result.push(Diff::Remove {
					pre: stored_columns,
				});
			}
		}

		Ok(result)
	}
}

impl RawStatefulOperator for DistinctOperator {}

impl SingleStateful for DistinctOperator {
	fn layout(&self) -> Schema {
		self.schema.clone()
	}
}

impl Operator for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut state = self.load_distinct_state(txn)?;
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					let insert_result = self.process_insert(&mut state, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
				} => {
					let update_result = self.process_update(&mut state, &pre, &post)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
				} => {
					let remove_result = self.process_remove(&mut state, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		self.save_distinct_state(txn, &state)?;

		Ok(Change::from_flow(self.node, change.version, result))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
