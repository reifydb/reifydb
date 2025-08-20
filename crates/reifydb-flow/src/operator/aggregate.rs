use std::{collections::HashMap, ops::Bound};

use reifydb_catalog::row::RowId;
use reifydb_core::{
	CowVec, Value,
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		SourceId::View, Transaction, VersionedCommandTransaction,
		VersionedQueryTransaction, ViewId, expression::Expression,
	},
	row::{EncodedKey, EncodedKeyRange, EncodedRow},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	core::{Change, Diff},
	operator::{Operator, OperatorContext},
};

// ============================================================================
// Key Implementation for Aggregate State Storage
// ============================================================================

/// Key for storing aggregate operator state
#[derive(Debug, Clone, PartialEq, Eq)]
struct FlowAggregateStateKey {
	/// Flow ID
	flow_id: u64,
	/// Node ID within the flow
	node_id: u64,
	/// Group key (serialized values)
	group_key: Vec<u8>,
}

impl FlowAggregateStateKey {
	const KEY_PREFIX: u8 = 0xF0; // Custom prefix for flow state

	fn new(flow_id: u64, node_id: u64, group_key: Vec<Value>) -> Self {
		// Serialize group key values
		let serialized =
			bincode::serialize(&group_key).unwrap_or_default();
		Self {
			flow_id,
			node_id,
			group_key: serialized,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&self.group_key);
		EncodedKey(CowVec::new(key))
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_ref();
		if bytes.len() < 17 || bytes[0] != Self::KEY_PREFIX {
			return None;
		}

		let flow_id = u64::from_be_bytes([
			bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
			bytes[6], bytes[7], bytes[8],
		]);

		let node_id = u64::from_be_bytes([
			bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
			bytes[14], bytes[15], bytes[16],
		]);

		let group_key = bytes[17..].to_vec();

		Some(Self {
			flow_id,
			node_id,
			group_key,
		})
	}

	/// Create a range key for scanning all groups of a node
	fn range_for_node(
		flow_id: u64,
		node_id: u64,
	) -> (EncodedKey, EncodedKey) {
		let mut start = Vec::new();
		start.push(Self::KEY_PREFIX);
		start.extend(&flow_id.to_be_bytes());
		start.extend(&node_id.to_be_bytes());

		let mut end = start.clone();
		// Increment node_id for exclusive upper bound
		let next_node = node_id + 1;
		end[9..17].copy_from_slice(&next_node.to_be_bytes());

		(EncodedKey(CowVec::new(start)), EncodedKey(CowVec::new(end)))
	}
}

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

	fn update_insert(
		&mut self,
		columns: &Columns,
		row_idx: usize,
		agg_columns: &[String],
	) {
		self.count += 1;
		self.ref_count += 1;

		for col_name in agg_columns {
			if let Some(column) =
				columns.iter().find(|c| c.name() == col_name)
			{
				let value = column.data().get_value(row_idx);

				// Update sum
				self.sum.entry(col_name.clone())
					.and_modify(|v| {
						*v = add_values(v, &value)
					})
					.or_insert(value.clone());

				// Update min
				self.min.entry(col_name.clone())
					.and_modify(|v| {
						*v = min_value(v, &value)
					})
					.or_insert(value.clone());

				// Update max
				self.max.entry(col_name.clone())
					.and_modify(|v| {
						*v = max_value(v, &value)
					})
					.or_insert(value.clone());
			}
		}
	}

	fn update_delete(
		&mut self,
		columns: &Columns,
		row_idx: usize,
		agg_columns: &[String],
	) {
		self.count -= 1;
		self.ref_count -= 1;

		for col_name in agg_columns {
			if let Some(column) =
				columns.iter().find(|c| c.name() == col_name)
			{
				let value = column.data().get_value(row_idx);

				// Update sum (subtract)
				self.sum.entry(col_name.clone()).and_modify(
					|v| *v = subtract_values(v, &value),
				);

				// Note: MIN/MAX cannot be incrementally
				// maintained on delete Would need to store
				// all values or recompute
			}
		}
	}

	fn to_encoded_row(&self) -> EncodedRow {
		let bytes = bincode::serialize(self).unwrap_or_default();
		EncodedRow(CowVec::new(bytes))
	}

	fn from_encoded_row(row: &EncodedRow) -> Option<Self> {
		bincode::deserialize(row.as_ref()).ok()
	}
}

// ============================================================================
// Aggregate Operator
// ============================================================================

pub struct AggregateOperator {
	/// Flow ID this operator belongs to
	flow_id: u64,
	/// Node ID within the flow
	node_id: u64,
	/// GROUP BY expressions
	by: Vec<Expression>,
	/// Aggregate expressions (SUM, COUNT, etc.)
	map: Vec<Expression>,
	/// Column names to aggregate
	agg_columns: Vec<String>,
}

impl AggregateOperator {
	pub fn new(
		flow_id: u64,
		node_id: u64,
		by: Vec<Expression>,
		map: Vec<Expression>,
	) -> Self {
		// Extract column names from aggregate expressions
		let agg_columns = extract_aggregate_columns(&map);

		Self {
			flow_id,
			node_id,
			by,
			map,
			agg_columns,
		}
	}

	fn compute_group_key<E: Evaluator, T: Transaction>(
		&self,
		ctx: &OperatorContext<E, T>,
		columns: &Columns,
		row_idx: usize,
	) -> Result<Vec<Value>> {
		let mut key = Vec::new();
		let empty_params = Params::None;

		for expr in &self.by {
			// Don't use take, evaluate on the full columns
			let eval_ctx = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count: columns.row_count(),
				take: None,
				params: &empty_params,
			};

			let result = ctx.evaluate(&eval_ctx, expr)?;
			// Get the value at the specific row index
			let value = result.data().get_value(row_idx);
			key.push(value);
		}

		Ok(key)
	}

	fn load_state<T: Transaction>(
		&self,
		txn: &mut CommandTransaction<T>,
		group_key: &[Value],
	) -> Result<GroupState> {
		let key = FlowAggregateStateKey::new(
			self.flow_id,
			self.node_id,
			group_key.to_vec(),
		);

		eprintln!(
			"[AggregateOperator] Loading state for flow_id={}, node_id={}, group_key={:?}",
			self.flow_id, self.node_id, group_key
		);

		match txn.get(&key.encode())? {
			Some(versioned) => {
				let state = GroupState::from_encoded_row(
					&versioned.row,
				)
				.unwrap_or_else(GroupState::new);
				eprintln!(
					"[AggregateOperator] Loaded existing state: count={}, sum={:?}, ref_count={}",
					state.count, state.sum, state.ref_count
				);
				Ok(state)
			}
			None => {
				eprintln!(
					"[AggregateOperator] No existing state found, creating new"
				);
				Ok(GroupState::new())
			}
		}
	}

	fn save_state<T: Transaction>(
		&self,
		txn: &mut CommandTransaction<T>,
		group_key: &[Value],
		state: &GroupState,
	) -> Result<()> {
		let key = FlowAggregateStateKey::new(
			self.flow_id,
			self.node_id,
			group_key.to_vec(),
		);

		eprintln!(
			"[AggregateOperator] Saving state for flow_id={}, node_id={}, group_key={:?}",
			self.flow_id, self.node_id, group_key
		);
		eprintln!(
			"[AggregateOperator] State to save: count={}, sum={:?}, ref_count={}",
			state.count, state.sum, state.ref_count
		);

		if state.ref_count == 0 {
			// Remove state if no more references
			eprintln!(
				"[AggregateOperator] Removing state (ref_count=0)"
			);
			txn.remove(&key.encode())?;
		} else {
			// Save updated state
			eprintln!("[AggregateOperator] Persisting state");
			txn.set(&key.encode(), state.to_encoded_row())?;
		}

		Ok(())
	}

	fn load_all_states<T: Transaction>(
		&self,
		txn: &mut CommandTransaction<T>,
	) -> Result<HashMap<Vec<Value>, GroupState>> {
		let mut states = HashMap::new();
		let (start, end) = FlowAggregateStateKey::range_for_node(
			self.flow_id,
			self.node_id,
		);

		let range = EncodedKeyRange::new(
			Bound::Included(start),
			Bound::Excluded(end),
		);
		let iter = txn.range(range)?;
		for versioned in iter {
			if let Some(state_key) =
				FlowAggregateStateKey::decode(&versioned.key)
			{
				if let Ok(group_key) =
					bincode::deserialize::<Vec<Value>>(
						&state_key.group_key,
					) {
					if let Some(state) =
						GroupState::from_encoded_row(
							&versioned.row,
						) {
						states.insert(group_key, state);
					}
				}
			}
		}

		Ok(states)
	}

	fn emit_group_changes<T: Transaction>(
		&self,
		txn: &mut CommandTransaction<T>,
		changed_groups: Vec<Vec<Value>>,
	) -> Result<Change> {
		let mut output_diffs = Vec::new();

		for group_key in changed_groups {
			let state = self.load_state(txn, &group_key)?;

			if state.ref_count > 0 {
				// Emit current aggregate values for this group
				let mut column_vec = Vec::new();

				// Add the aggregated value column (sum)
				// The view expects a column named "value" which
				// is the sum
				if let Some(sum_value) = state.sum.get("value")
				{
					let data = match sum_value {
						Value::Int8(v) => {
							ColumnData::int8(vec![
								*v,
							])
						}
						Value::Int4(v) => {
							ColumnData::int4(vec![
								*v,
							])
						}
						Value::Float8(v) => {
							ColumnData::float8(
								vec![v.value()],
							)
						}
						_ => ColumnData::undefined(1),
					};
					column_vec.push(
						Column::ColumnQualified(
							ColumnQualified {
								name: "value"
									.to_string(
									),
								data,
							},
						),
					);
				} else {
					column_vec.push(Column::ColumnQualified(ColumnQualified {
                        name: "value".to_string(),
                        data: ColumnData::undefined(1),
                    }));
				}

				// Add group key column (age)
				if let Some(age_value) = group_key.first() {
					let data = match age_value {
						Value::Int8(v) => {
							ColumnData::int8(vec![
								*v,
							])
						}
						Value::Int4(v) => {
							ColumnData::int4(vec![
								*v,
							])
						}
						Value::Utf8(v) => {
							ColumnData::utf8(vec![
								v.clone(),
							])
						}
						_ => ColumnData::undefined(1),
					};
					column_vec.push(
						Column::ColumnQualified(
							ColumnQualified {
								name: "age"
									.to_string(
									),
								data,
							},
						),
					);
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
						let hash =
							{
								use std::collections::hash_map::DefaultHasher;
                            	use std::hash::{Hash, Hasher};
								let mut hasher = DefaultHasher::new();
								group_key.hash(&mut hasher);
								hasher.finish()
							};
						update_row_ids
							.push(RowId(hash));
					}

					eprintln!(
						"[AggregateOperator] Emitting Update diff for group {:?}",
						group_key
					);
					eprintln!(
						"[AggregateOperator]   Before: {:?}",
						previous
					);
					eprintln!(
						"[AggregateOperator]   After: {:?}",
						columns
					);
					output_diffs.push(Diff::Update {
						source: View(ViewId(0)),
						row_ids: update_row_ids,
						before: previous.clone(),
						after: columns.clone(),
					});
				} else {
					// First time seeing this group, emit
					// Insert
					let mut insert_row_ids = Vec::new();
					for _ in 0..columns.row_count() {
						// Generate a unique row_id for
						// this aggregate group
						let hash =
							{
								use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
								let mut hasher = DefaultHasher::new();
								group_key.hash(&mut hasher);
								hasher.finish()
							};
						insert_row_ids
							.push(RowId(hash));
					}

					output_diffs.push(Diff::Insert {
						source: View(ViewId(0)),
						row_ids: insert_row_ids,
						after: columns.clone(),
					});
				}

				// Update state with the new output for next
				// time
				let mut updated_state = state.clone();
				updated_state.previous_output = Some(columns);
				self.save_state(
					txn,
					&group_key,
					&updated_state,
				)?;
			} else if state.previous_output.is_some() {
				// Group was deleted, emit retraction
				let before_columns =
					state.previous_output.unwrap();
				let mut remove_row_ids = Vec::new();
				for _ in 0..before_columns.row_count() {
					// Use same hash-based row_id for
					// consistency
					let hash = {
						use std::{
							collections::hash_map::DefaultHasher,
							hash::{Hash, Hasher},
						};
						let mut hasher =
							DefaultHasher::new();
						group_key.hash(&mut hasher);
						hasher.finish()
					};
					remove_row_ids.push(RowId(hash));
				}

				output_diffs.push(Diff::Remove {
					source: View(ViewId(0)),
					row_ids: remove_row_ids,
					before: before_columns,
				});
			}
		}

		Ok(Change::new(output_diffs))
	}
}

impl<E: Evaluator> Operator<E> for AggregateOperator {
	fn apply<T: Transaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &Change,
	) -> Result<Change> {
		let mut changed_groups = Vec::new();

		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					after,
					..
				} => {
					let row_count = after.row_count();
					for row_idx in 0..row_count {
						let group_key = self
							.compute_group_key(
								ctx, after,
								row_idx,
							)?;

						// Load state from storage
						let mut state = self
							.load_state(
								ctx.txn,
								&group_key,
							)?;

						// Update state
						state.update_insert(
							after,
							row_idx,
							&self.agg_columns,
						);

						// Save state back to storage
						self.save_state(
							ctx.txn, &group_key,
							&state,
						)?;

						if !changed_groups
							.contains(&group_key)
						{
							changed_groups.push(
								group_key,
							);
						}
					}
				}
				Diff::Update {
					before,
					after,
					..
				} => {
					eprintln!(
						"[AggregateOperator] Processing Update diff"
					);
					eprintln!(
						"[AggregateOperator]   Before columns: {:?}",
						before
					);
					eprintln!(
						"[AggregateOperator]   After columns: {:?}",
						after
					);
					// Handle as delete + insert
					let row_count = before.row_count();
					for row_idx in 0..row_count {
						// Delete old
						let old_key = self
							.compute_group_key(
								ctx, before,
								row_idx,
							)?;
						let mut old_state = self
							.load_state(
								ctx.txn,
								&old_key,
							)?;
						old_state.update_delete(
							before,
							row_idx,
							&self.agg_columns,
						);
						self.save_state(
							ctx.txn, &old_key,
							&old_state,
						)?;

						if !changed_groups
							.contains(&old_key)
						{
							changed_groups.push(
								old_key.clone(),
							);
						}

						// Insert new
						let new_key = self
							.compute_group_key(
								ctx, after,
								row_idx,
							)?;
						let mut new_state = self
							.load_state(
								ctx.txn,
								&new_key,
							)?;
						new_state.update_insert(
							after,
							row_idx,
							&self.agg_columns,
						);
						self.save_state(
							ctx.txn, &new_key,
							&new_state,
						)?;

						if !changed_groups
							.contains(&new_key)
						{
							changed_groups
								.push(new_key);
						}
					}
				}
				Diff::Remove {
					before,
					..
				} => {
					let row_count = before.row_count();
					for row_idx in 0..row_count {
						let group_key = self
							.compute_group_key(
								ctx, before,
								row_idx,
							)?;

						// Load state from storage
						let mut state = self
							.load_state(
								ctx.txn,
								&group_key,
							)?;

						// Update state
						state.update_delete(
							before,
							row_idx,
							&self.agg_columns,
						);

						// Save state back to storage
						self.save_state(
							ctx.txn, &group_key,
							&state,
						)?;

						if !changed_groups
							.contains(&group_key)
						{
							changed_groups.push(
								group_key,
							);
						}
					}
				}
			}
		}

		// Emit changes for affected groups
		self.emit_group_changes(ctx.txn, changed_groups)
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
					columns.push(col
						.0
						.fragment()
						.to_string());
				}
			}
		}
	}

	columns
}

fn add_values(a: &Value, b: &Value) -> Value {
	use std::convert::TryFrom;

	use reifydb_core::OrderedF64;
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(x + y),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(x + y),
		(Value::Float8(x), Value::Float8(y)) => {
			let result = x.value() + y.value();
			Value::Float8(
				OrderedF64::try_from(result).unwrap_or(*x),
			)
		}
		_ => a.clone(), // Simplified
	}
}

fn subtract_values(a: &Value, b: &Value) -> Value {
	use std::convert::TryFrom;

	use reifydb_core::OrderedF64;
	match (a, b) {
		(Value::Int8(x), Value::Int8(y)) => Value::Int8(x - y),
		(Value::Int4(x), Value::Int4(y)) => Value::Int4(x - y),
		(Value::Float8(x), Value::Float8(y)) => {
			let result = x.value() - y.value();
			Value::Float8(
				OrderedF64::try_from(result).unwrap_or(*x),
			)
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
