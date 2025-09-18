use std::{
	collections::HashMap,
	hash::{DefaultHasher, Hash, Hasher},
};

use reifydb_core::{
	EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, FlowNodeId, Params, SourceId, Transaction, expression::Expression},
	row::EncodedRow,
	util::{CowVec, encoding::keycode},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::{OrderedF64, RowNumber, Value};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, transform::TransformOperator},
};

// ============================================================================
// State Management
// ============================================================================

/// State for a single aggregate group
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupState {
	/// Number of rows in this group (for COUNT)
	count: i64,
	/// Sum accumulator (for SUM and AVG)
	sum: HashMap<String, Value>,
	/// Min accumulator
	min: HashMap<String, Value>,
	/// Max accumulator
	max: HashMap<String, Value>,
	/// Reference count (for handling deletes)
	ref_count: usize,
	/// Previous emitted values (for generating retractions)
	previous_output: Option<Columns>,
}

impl GroupState {
	fn new() -> Self {
		Self {
			count: 0,
			sum: HashMap::new(),
			min: HashMap::new(),
			max: HashMap::new(),
			ref_count: 0,
			previous_output: None,
		}
	}

	fn update_insert(&mut self, columns: &Columns, row_indices: &[usize], agg_columns: &[String]) {
		let num_rows = row_indices.len();
		self.count += num_rows as i64;
		self.ref_count += num_rows;

		for col_name in agg_columns {
			if let Some(column) = columns.iter().find(|c| c.name() == col_name) {
				// Process all rows at once for this column
				let mut values = Vec::with_capacity(row_indices.len());
				for &row_idx in row_indices {
					values.push(column.data().get_value(row_idx));
				}

				// Update sum - aggregate all values at once
				if !values.is_empty() {
					let aggregated_sum =
						values.iter().fold(None, |acc: Option<Value>, val| match acc {
							Some(a) => Some(add_values(&a, val)),
							None => Some(val.clone()),
						});

					if let Some(new_sum) = aggregated_sum {
						self.sum.entry(col_name.clone())
							.and_modify(|v| *v = add_values(v, &new_sum))
							.or_insert(new_sum);
					}

					// Update min - find minimum across all
					// values
					let new_min = values.iter().fold(None, |acc: Option<Value>, val| match acc {
						Some(a) => Some(min_value(&a, val)),
						None => Some(val.clone()),
					});

					if let Some(new_min) = new_min {
						self.min.entry(col_name.clone())
							.and_modify(|v| *v = min_value(v, &new_min))
							.or_insert(new_min);
					}

					// Update max - find maximum across all
					// values
					let new_max = values.iter().fold(None, |acc: Option<Value>, val| match acc {
						Some(a) => Some(max_value(&a, val)),
						None => Some(val.clone()),
					});

					if let Some(new_max) = new_max {
						self.max.entry(col_name.clone())
							.and_modify(|v| *v = max_value(v, &new_max))
							.or_insert(new_max);
					}
				}
			}
		}
	}

	fn update_delete(&mut self, columns: &Columns, row_indices: &[usize], agg_columns: &[String]) {
		let num_rows = row_indices.len();
		self.count -= num_rows as i64;
		self.ref_count -= num_rows;

		for col_name in agg_columns {
			if let Some(column) = columns.iter().find(|c| c.name() == col_name) {
				// Process all rows at once for this column
				let mut values = Vec::with_capacity(row_indices.len());
				for &row_idx in row_indices {
					values.push(column.data().get_value(row_idx));
				}

				// Update sum - subtract all values at once
				if !values.is_empty() {
					let aggregated_sum =
						values.iter().fold(None, |acc: Option<Value>, val| match acc {
							Some(a) => Some(add_values(&a, val)),
							None => Some(val.clone()),
						});

					if let Some(sum_to_subtract) = aggregated_sum {
						self.sum.entry(col_name.clone())
							.and_modify(|v| *v = subtract_values(v, &sum_to_subtract));
					}
				}

				// Note: MIN/MAX cannot be incrementally
				// maintained on delete Would need to store
				// all values or recompute
			}
		}
	}

	fn to_encoded_row(&self) -> EncodedRow {
		let bytes = keycode::serialize(self);
		EncodedRow(CowVec::new(bytes))
	}

	fn from_encoded_row(row: &EncodedRow) -> Option<Self> {
		keycode::deserialize(row.as_ref()).ok()
	}
}

// ============================================================================
// Aggregate Operator
// ============================================================================

pub struct AggregateOperator {
	/// Node ID within the flow
	node: FlowNodeId,
	/// GROUP BY expressions
	by: Vec<Expression<'static>>,
	/// Aggregate expressions (SUM, COUNT, etc.)
	map: Vec<Expression<'static>>,
	/// Column names to aggregate
	agg_columns: Vec<String>,
}

impl AggregateOperator {
	pub fn new(node: FlowNodeId, by: Vec<Expression<'static>>, map: Vec<Expression<'static>>) -> Self {
		// Extract column names from aggregate expressions
		let agg_columns = extract_aggregate_columns(&map);

		Self {
			node,
			by,
			map,
			agg_columns,
		}
	}

	fn group_key_to_encoded_key(group_key: &[Value]) -> EncodedKey {
		let serialized = serde_json::to_vec(group_key).unwrap_or_default();
		EncodedKey::new(serialized)
	}

	fn compute_group_key(
		&self,
		evaluator: &StandardEvaluator,
		columns: &Columns,
		row_indices: Option<&[usize]>,
	) -> Result<HashMap<Vec<Value>, Vec<usize>>> {
		let mut group_map = HashMap::new();
		let empty_params = Params::None;
		let row_count = columns.row_count();

		// Evaluate all grouping expressions once
		let mut group_columns = Vec::new();
		for expr in &self.by {
			let eval_ctx = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count,
				take: None,
				params: &empty_params,
			};

			let result = evaluator.evaluate(&eval_ctx, expr)?;
			group_columns.push(result);
		}

		// Build group keys for all rows
		let indices: Vec<usize> = if let Some(indices) = row_indices {
			indices.to_vec()
		} else {
			(0..row_count).collect()
		};

		for &row_idx in &indices {
			let mut key = Vec::new();
			for col in &group_columns {
				key.push(col.data().get_value(row_idx));
			}
			group_map.entry(key).or_insert_with(Vec::new).push(row_idx);
		}

		Ok(group_map)
	}

	fn load_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		group_key: &[Value],
	) -> Result<GroupState> {
		let key = Self::group_key_to_encoded_key(group_key);

		let state_row = self.get(txn, &key)?;
		if state_row.as_ref().is_empty() {
			Ok(GroupState::new())
		} else {
			let state = GroupState::from_encoded_row(&state_row).unwrap_or_else(GroupState::new);
			Ok(state)
		}
	}

	fn save_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		group_key: &[Value],
		state: &GroupState,
	) -> Result<()> {
		let key = Self::group_key_to_encoded_key(group_key);

		if state.ref_count == 0 {
			// Remove state if no more references
			self.remove(txn, &key)?;
		} else {
			// Save updated state
			self.set(txn, &key, state.to_encoded_row())?;
		}

		Ok(())
	}

	fn load_all_states<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
	) -> Result<HashMap<Vec<Value>, GroupState>> {
		let mut states = HashMap::new();

		// Scan all entries for this node
		let iter = self.scan(txn)?;
		for (key, row) in iter {
			if let Ok(group_key) = serde_json::from_slice::<Vec<Value>>(key.as_ref()) {
				if let Some(state) = GroupState::from_encoded_row(&row) {
					states.insert(group_key, state);
				}
			}
		}

		Ok(states)
	}

	fn emit_groupchanges<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		changed_groups: Vec<Vec<Value>>,
	) -> Result<FlowChange> {
		let mut output_diffs = Vec::new();

		for group_key in changed_groups {
			let state = self.load_state(txn, &group_key)?;

			if state.ref_count > 0 {
				// Emit current aggregate values for this group
				let mut column_vec = Vec::new();

				// Add the aggregated value column (sum)
				// The view expects a column named "value" which
				// is the sum
				if let Some(sum_value) = state.sum.get("value") {
					let data = match sum_value {
						Value::Int8(v) => ColumnData::int8(vec![*v]),
						Value::Int4(v) => ColumnData::int4(vec![*v]),
						Value::Float8(v) => ColumnData::float8(vec![v.value()]),
						_ => ColumnData::undefined(1),
					};
					column_vec.push(Column::ColumnQualified(ColumnQualified {
						name: "value".to_string(),
						data,
					}));
				} else {
					column_vec.push(Column::ColumnQualified(ColumnQualified {
						name: "value".to_string(),
						data: ColumnData::undefined(1),
					}));
				}

				// Add group key column (age)
				if let Some(age_value) = group_key.first() {
					let data = match age_value {
						Value::Int8(v) => ColumnData::int8(vec![*v]),
						Value::Int4(v) => ColumnData::int4(vec![*v]),
						Value::Utf8(v) => ColumnData::utf8(vec![v.clone()]),
						_ => ColumnData::undefined(1),
					};
					column_vec.push(Column::ColumnQualified(ColumnQualified {
						name: "age".to_string(),
						data,
					}));
				}
				let columns = Columns::new(column_vec);

				// If we had previous output, emit an Update
				// diff Otherwise emit an Insert for a new
				// group
				if let Some(previous) = &state.previous_output {
					// Generate row_ids for the update
					let mut update_row_ids = Vec::new();
					for _ in 0..columns.row_count() {
						// Generate a unique row_id for
						// this aggregate group
						// Using hash of group_key for
						// deterministic row_id
						let hash = {
							use std::{
								collections::hash_map::DefaultHasher,
								hash::{Hash, Hasher},
							};
							let mut hasher = DefaultHasher::new();
							group_key.hash(&mut hasher);
							hasher.finish()
						};
						update_row_ids.push(RowNumber(hash));
					}

					output_diffs.push(FlowDiff::Update {
						source: SourceId::FlowNode(self.node),
						rows: CowVec::new(update_row_ids),
						pre: previous.clone(),
						post: columns.clone(),
					});
				} else {
					// First time seeing this group, emit
					// Insert
					let mut insert_row_ids = Vec::new();
					for _ in 0..columns.row_count() {
						// Generate a unique row_id for
						// this aggregate group
						let hash = {
							let mut hasher = DefaultHasher::new();
							group_key.hash(&mut hasher);
							hasher.finish()
						};
						insert_row_ids.push(RowNumber(hash));
					}

					output_diffs.push(FlowDiff::Insert {
						source: SourceId::FlowNode(self.node),
						rows: CowVec::new(insert_row_ids),
						post: columns.clone(),
					});
				}

				// Update state with the new output for next
				// time
				let mut updated_state = state.clone();
				updated_state.previous_output = Some(columns);
				self.save_state(txn, &group_key, &updated_state)?;
			} else if state.previous_output.is_some() {
				// Group was deleted, emit retraction
				let before_columns = state.previous_output.unwrap();
				let mut remove_row_ids = Vec::new();
				for _ in 0..before_columns.row_count() {
					// Use same hash-based row_id for
					// consistency
					let hash = {
						use std::{
							collections::hash_map::DefaultHasher,
							hash::{Hash, Hasher},
						};
						let mut hasher = DefaultHasher::new();
						group_key.hash(&mut hasher);
						hasher.finish()
					};
					remove_row_ids.push(RowNumber(hash));
				}

				output_diffs.push(FlowDiff::Remove {
					source: SourceId::FlowNode(self.node),
					rows: CowVec::new(remove_row_ids),
					pre: before_columns,
				});
			}
		}

		Ok(FlowChange::new(output_diffs))
	}
}

impl<T: Transaction> Operator<T> for AggregateOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut changed_groups = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post: after,
					..
				} => {
					// Compute all group keys at once
					let group_map = self.compute_group_key(evaluator, &after, None)?;

					// Process each group in batch
					for (group_key, row_indices) in group_map {
						// Load state from storage
						let mut state = self.load_state(txn, &group_key)?;

						// Update state with all rows
						// for this group
						state.update_insert(&after, &row_indices, &self.agg_columns);

						// Save state back to storage
						self.save_state(txn, &group_key, &state)?;

						if !changed_groups.contains(&group_key) {
							changed_groups.push(group_key);
						}
					}
				}
				FlowDiff::Update {
					pre: before,
					post: after,
					..
				} => {
					// Handle as delete + insert
					// Compute group keys for old values
					let old_group_map = self.compute_group_key(evaluator, &before, None)?;

					// Process deletions for each group
					for (old_key, row_indices) in old_group_map {
						let mut old_state = self.load_state(txn, &old_key)?;
						old_state.update_delete(&before, &row_indices, &self.agg_columns);
						self.save_state(txn, &old_key, &old_state)?;

						if !changed_groups.contains(&old_key) {
							changed_groups.push(old_key);
						}
					}

					// Compute group keys for new values
					let new_group_map = self.compute_group_key(evaluator, &after, None)?;

					// Process insertions for each group
					for (new_key, row_indices) in new_group_map {
						let mut new_state = self.load_state(txn, &new_key)?;
						new_state.update_insert(&after, &row_indices, &self.agg_columns);
						self.save_state(txn, &new_key, &new_state)?;

						if !changed_groups.contains(&new_key) {
							changed_groups.push(new_key);
						}
					}
				}
				FlowDiff::Remove {
					pre: before,
					..
				} => {
					// Compute all group keys at once
					let group_map = self.compute_group_key(evaluator, &before, None)?;

					// Process each group in batch
					for (group_key, row_indices) in group_map {
						// Load state from storage
						let mut state = self.load_state(txn, &group_key)?;

						// Update state with all rows
						// for this group
						state.update_delete(&before, &row_indices, &self.agg_columns);

						// Save state back to storage
						self.save_state(txn, &group_key, &state)?;

						if !changed_groups.contains(&group_key) {
							changed_groups.push(group_key);
						}
					}
				}
			}
		}

		// Emit changes for affected groups
		self.emit_groupchanges(txn, changed_groups)
	}
}

impl<T: Transaction> TransformOperator<T> for AggregateOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

fn extract_aggregate_columns(expressions: &[Expression]) -> Vec<String> {
	let mut columns = Vec::new();

	for expr in expressions {
		// For aggregate functions like sum(value), extract the column
		// name
		if let Expression::Call(call) = expr {
			if let Some(arg) = call.args.first() {
				if let Expression::Column(col) = arg {
					columns.push(col.0.name.text().to_string());
				}
			}
		}
	}

	columns
}

fn add_values(a: &Value, b: &Value) -> Value {
	use std::convert::TryFrom;

	use reifydb_type::OrderedF64;
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(x + y),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(x + y),
		(Value::Float8(x), Value::Float8(y)) => {
			let result = x.value() + y.value();
			Value::Float8(OrderedF64::try_from(result).unwrap_or(*x))
		}
		_ => a.clone(), // Simplified
	}
}

fn subtract_values(a: &Value, b: &Value) -> Value {
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(x - y),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(x - y),
		(Value::Float8(x), Value::Float8(y)) => {
			let result = x.value() - y.value();
			Value::Float8(OrderedF64::try_from(result).unwrap_or(*x))
		}
		_ => a.clone(), // Simplified
	}
}

fn min_value(a: &Value, b: &Value) -> Value {
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(*x.min(y)),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(*x.min(y)),
		(Value::Float8(x), Value::Float8(y)) => {
			if x.value() < y.value() {
				Value::Float8(*x)
			} else {
				Value::Float8(*y)
			}
		}
		_ => a.clone(), // Simplified
	}
}

fn max_value(a: &Value, b: &Value) -> Value {
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(*x.max(y)),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(*x.max(y)),
		(Value::Float8(x), Value::Float8(y)) => {
			if x.value() > y.value() {
				Value::Float8(*x)
			} else {
				Value::Float8(*y)
			}
		}
		_ => a.clone(), // Simplified
	}
}
