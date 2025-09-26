use std::collections::HashMap;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CowVec, Error, JoinType,
	flow::{FlowChange, FlowChangeOrigin, FlowDiff},
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::{EncodedRow, EncodedRowLayout, EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Blob, Params, RowNumber, Type, Value, internal_error};
use serde::{Deserialize, Serialize};

use crate::operator::{
	Operator,
	transform::{
		TransformOperator,
		stateful::{RawStatefulOperator, SingleStateful},
	},
};

static EMPTY_PARAMS: Params = Params::None;

/// Layout information for both sides of the join
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct JoinLayout {
	left_names: Vec<String>,
	left_types: Vec<Type>,
	right_names: Vec<String>,
	right_types: Vec<Type>,
}

impl JoinLayout {
	fn new() -> Self {
		// Initialize with empty vectors - they will be populated as we see data
		Default::default()
	}

	// Create a default layout with placeholder values to ensure consistent serialization
	fn default_with_placeholders() -> Self {
		Self {
			// Use placeholder values that will be replaced when actual data arrives
			left_names: vec!["__placeholder__".to_string()],
			left_types: vec![Type::Undefined],
			right_names: vec!["__placeholder__".to_string()],
			right_types: vec![Type::Undefined],
		}
	}

	fn update_left_from_row(&mut self, row: &Row) {
		let names = row.layout.names();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		if self.left_names.is_empty() {
			self.left_names = names.to_vec();
			self.left_types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		for (i, new_type) in types.iter().enumerate() {
			if i < self.left_types.len() {
				if self.left_types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.left_types[i] = *new_type;
				}
			} else {
				self.left_types.push(*new_type);
				if i < names.len() {
					self.left_names.push(names[i].clone());
				}
			}
		}
	}

	fn update_right_from_row(&mut self, row: &Row) {
		let names = row.layout.names();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		if self.right_names.is_empty() {
			self.right_names = names.to_vec();
			self.right_types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		for (i, new_type) in types.iter().enumerate() {
			if i < self.right_types.len() {
				if self.right_types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.right_types[i] = *new_type;
				}
			} else {
				self.right_types.push(*new_type);
				if i < names.len() {
					self.right_names.push(names[i].clone());
				}
			}
		}
	}
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

	fn to_left_row(&self, layout: &JoinLayout) -> Row {
		// If layout is empty, we shouldn't be deserializing this row
		// This indicates a state consistency issue
		let fields: Vec<(String, Type)> = if layout.left_names.is_empty() {
			vec![]
		} else {
			layout.left_names.iter().cloned().zip(layout.left_types.iter().cloned()).collect()
		};

		let row_layout = EncodedRowNamedLayout::new(fields);
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}

	fn to_right_row(&self, layout: &JoinLayout) -> Row {
		// If layout is empty, we shouldn't be deserializing this row
		// This indicates a state consistency issue
		let fields: Vec<(String, Type)> = if layout.right_names.is_empty() {
			vec![]
		} else {
			layout.right_names.iter().cloned().zip(layout.right_types.iter().cloned()).collect()
		};

		let row_layout = EncodedRowNamedLayout::new(fields);
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}
}

/// Represents rows stored for each side of the join
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinSideEntry {
	rows: Vec<SerializedRow>,
}

/// The complete join state - serialize both data and layout
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinState {
	// Serialize the layout so we can properly reconstruct rows
	layout: JoinLayout,
	// Map from join key hash to rows on the left side
	left_entries: HashMap<Hash128, JoinSideEntry>,
	// Map from join key hash to rows on the right side
	right_entries: HashMap<Hash128, JoinSideEntry>,
}

impl JoinState {
	fn new() -> Self {
		Self {
			layout: JoinLayout::new(),
			left_entries: HashMap::new(),
			right_entries: HashMap::new(),
		}
	}
}

impl Default for JoinState {
	fn default() -> Self {
		Self::new()
	}
}

pub struct JoinOperator {
	node: FlowNodeId,
	join_type: JoinType,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression<'static>>,
	right_exprs: Vec<Expression<'static>>,
	alias: Option<String>,
	layout: EncodedRowLayout,
}

impl JoinOperator {
	pub fn new(
		node: FlowNodeId,
		join_type: JoinType,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
		left_exprs: Vec<Expression<'static>>,
		right_exprs: Vec<Expression<'static>>,
		alias: Option<String>,
	) -> Self {
		let layout = Self::state_layout();

		Self {
			node,
			join_type,
			left_node,
			right_node,
			left_exprs,
			right_exprs,
			alias,
			layout,
		}
	}

	fn state_layout() -> EncodedRowLayout {
		EncodedRowLayout::new(&[Type::Blob])
	}

	fn compute_join_key(
		&self,
		row: &Row,
		exprs: &[Expression<'static>],
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<Hash128>> {
		let mut hasher = Vec::new();
		for (i, expr) in exprs.iter().enumerate() {
			// For AccessSource expressions, extract just the column name and evaluate that
			let value = match expr {
				Expression::AccessSource(access_source) => {
					// Get the column name without the source
					let col_name = access_source.column.name.as_ref();

					// Find the column in the row by name
					let names = row.layout.names();
					let col_index = names.iter().position(|n| n == col_name);

					if let Some(idx) = col_index {
						let val = row.layout.get_value(&row.encoded, idx);
						val
					} else {
						Value::Undefined
					}
				}
				_ => {
					// For other expressions, use the evaluator
					let ctx = RowEvaluationContext {
						row: row.clone(),
						target: None,
						params: &EMPTY_PARAMS,
					};
					let val = evaluator.evaluate(&ctx, expr)?;
					val
				}
			};

			// Check if the value is undefined - undefined values should never match in joins
			if matches!(value, reifydb_type::Value::Undefined) {
				return Ok(None);
			}

			let bytes = encode_to_vec(&value, standard())
				.map_err(|e| Error(internal_error!("Failed to encode value for hash: {}", e)))?;

			hasher.extend_from_slice(&bytes);
		}

		let hash = xxh3_128(&hasher);
		Ok(Some(hash))
	}

	fn join_rows(&self, left: &Row, right: &Row) -> Row {
		// Combine the two rows into a single row
		// Prefix column names with alias to handle naming conflicts
		let mut combined_values = Vec::new();
		let mut combined_names = Vec::new();
		let mut combined_types = Vec::new();

		// Add left side columns - never prefixed
		let left_names = left.layout.names();
		for i in 0..left.layout.fields.len() {
			let value = left.layout.get_value(&left.encoded, i);
			combined_values.push(value);
			if i < left_names.len() {
				combined_names.push(left_names[i].clone());
			}
			combined_types.push(left.layout.fields[i].r#type);
		}

		// Collect left names into a set for conflict detection
		let left_name_set: std::collections::HashSet<String> = left_names.iter().cloned().collect();

		// Add right side columns - prefix with alias when there's a conflict
		let right_names = right.layout.names();
		for i in 0..right.layout.fields.len() {
			let value = right.layout.get_value(&right.encoded, i);
			combined_values.push(value);
			if i < right_names.len() {
				let col_name = &right_names[i];
				// Check if there's a naming conflict with left side
				let final_name = if left_name_set.contains(col_name) {
					// There's a conflict - apply alias prefix if available
					if let Some(ref alias) = self.alias {
						format!("{}_{}", alias, col_name)
					} else {
						// No alias provided but there's a conflict - use double underscore
						// prefix
						format!("__{}__", col_name)
					}
				} else {
					// No conflict - use original name
					col_name.clone()
				};
				combined_names.push(final_name);
			}
			combined_types.push(right.layout.fields[i].r#type);
		}

		// Create combined layout
		let fields: Vec<(String, Type)> = combined_names.into_iter().zip(combined_types.into_iter()).collect();
		let layout = EncodedRowNamedLayout::new(fields);

		// Allocate and populate the new row
		let mut encoded_row = layout.allocate_row();
		layout.set_values(&mut encoded_row, &combined_values);

		// Generate a deterministic unique row number by combining left and right row numbers
		// Use XOR and bit shifting to ensure uniqueness even when row numbers are small
		let combined = (left.number.0.wrapping_mul(0x9e3779b97f4a7c15))
			^ (right.number.0.wrapping_mul(0x517cc1b727220a95));
		let combined_number = RowNumber(combined);
		Row {
			number: combined_number,
			encoded: encoded_row,
			layout,
		}
	}

	fn load_join_state<T: Transaction>(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<JoinState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() {
			return Ok(JoinState::default());
		}

		// Check if the blob field is defined using the encoded row's own method
		// Debug: check the bitvec directly
		if state_row.len() > 0 {
			let first_byte = state_row.as_slice()[0];
		}

		if !state_row.is_defined(0) {
			return Ok(JoinState::default());
		}

		// Use self.layout to get the blob
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(JoinState::default());
		}

		let config = standard();
		let decoded: Result<(JoinState, usize), _> = decode_from_slice(blob.as_ref(), config);

		match decoded {
			Ok((state, bytes_read)) => Ok(state),
			Err(e) => Err(Error(internal_error!("Failed to deserialize JoinState: {}", e))),
		}
	}

	fn save_join_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		state: &JoinState,
	) -> crate::Result<()> {
		let config = standard();
		let serialized = encode_to_vec(state, config)
			.map_err(|e| Error(internal_error!("Failed to serialize JoinState: {}", e)))?;

		// Use self.layout to match what load_state expects
		let mut state_row = self.layout.allocate_row();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		let result = self.save_state(txn, state_row);
		if result.is_ok() {
		} else {
		}
		result
	}

	fn determine_side(&self, change: &FlowChange) -> Option<JoinSide> {
		match &change.origin {
			FlowChangeOrigin::Internal(from_node) => {
				if *from_node == self.left_node {
					Some(JoinSide::Left)
				} else if *from_node == self.right_node {
					Some(JoinSide::Right)
				} else {
					None
				}
			}
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy)]
enum JoinSide {
	Left,
	Right,
}

impl<T: Transaction> TransformOperator<T> for JoinOperator {}

impl<T: Transaction> RawStatefulOperator<T> for JoinOperator {}

impl<T: Transaction> SingleStateful<T> for JoinOperator {
	fn layout(&self) -> EncodedRowLayout {
		self.layout.clone()
	}
}

impl<T: Transaction> Operator<T> for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// Check for self-referential calls (should never happen)
		if let FlowChangeOrigin::Internal(from_node) = &change.origin {
			if *from_node == self.node {
				return Ok(FlowChange::internal(self.node, Vec::new()));
			}
		}

		let mut state = self.load_join_state(txn)?;
		let mut result = Vec::new();

		// Determine which side this change is from
		let side = self
			.determine_side(&change)
			.ok_or_else(|| Error(internal_error!("Join operator received change from unknown node")))?;

		let num_diffs = change.diffs.len();
		for (i, diff) in change.diffs.into_iter().enumerate() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					match side {
						JoinSide::Left => {
							state.layout.update_left_from_row(&post);
							// Debug: extract class_id value
							let class_id_idx = post
								.layout
								.names()
								.iter()
								.position(|n| n == "class_id");
							let class_id = if let Some(idx) = class_id_idx {
								let val = post.layout.get_value(&post.encoded, idx);
								format!("{:?}", val)
							} else {
								"unknown".to_string()
							};
							let key_hash_opt = self.compute_join_key(
								&post,
								&self.left_exprs,
								evaluator,
							)?;

							// Handle undefined join keys - they never match
							if let Some(key_hash) = key_hash_opt {
								// Add to left entries
								let serialized = SerializedRow::from_row(&post);
								let is_new_key =
									!state.left_entries.contains_key(&key_hash);
								let entry = state
									.left_entries
									.entry(key_hash)
									.or_insert_with(|| JoinSideEntry {
										rows: Vec::new(),
									});
								entry.rows.push(serialized.clone());
								// Extract the name for debugging
								let name_idx = post
									.layout
									.names()
									.iter()
									.position(|n| n == "name");
								let name = if let Some(idx) = name_idx {
									let val = post
										.layout
										.get_value(&post.encoded, idx);
									format!("{:?}", val)
								} else {
									"unknown".to_string()
								};

								// Join with all matching right rows
								if let Some(right_entry) =
									state.right_entries.get(&key_hash)
								{
									for right_row_ser in &right_entry.rows {
										let right_row = right_row_ser
											.to_right_row(&state.layout);
										let joined_row = self
											.join_rows(&post, &right_row);
										result.push(FlowDiff::Insert {
											post: joined_row,
										});
									}
								} else if matches!(self.join_type, JoinType::Left) {
									// For left join, emit the left row directly
									result.push(FlowDiff::Insert {
										post: post.clone(),
									});
								}
							} else {
								// Undefined join key - never matches anything
								if matches!(self.join_type, JoinType::Left) {
									// For left join, emit the left row with nulls
									// for right side
									result.push(FlowDiff::Insert {
										post: post.clone(),
									});
								}
							}
						}
						JoinSide::Right => {
							state.layout.update_right_from_row(&post);
							let key_hash_opt = self.compute_join_key(
								&post,
								&self.right_exprs,
								evaluator,
							)?;

							// Handle undefined join keys - they never match
							if let Some(key_hash) = key_hash_opt {
								// Debug: extract class_id and subject
								let class_id_idx = post
									.layout
									.names()
									.iter()
									.position(|n| n == "class_id");
								let class_id = if let Some(idx) = class_id_idx {
									let val = post
										.layout
										.get_value(&post.encoded, idx);
									format!("{:?}", val)
								} else {
									"unknown".to_string()
								};
								let subject_idx = post
									.layout
									.names()
									.iter()
									.position(|n| n == "subject");
								let subject = if let Some(idx) = subject_idx {
									let val = post
										.layout
										.get_value(&post.encoded, idx);
									format!("{:?}", val)
								} else {
									"unknown".to_string()
								};
								// Check if this is the first right row for this key
								// (for LEFT JOIN handling)
								let is_first_right_row =
									!state.right_entries.contains_key(&key_hash);

								// Add to right entries
								let serialized = SerializedRow::from_row(&post);
								state.right_entries
									.entry(key_hash)
									.or_insert_with(|| JoinSideEntry {
										rows: Vec::new(),
									})
									.rows
									.push(serialized);

								// Join with all matching left rows
								if let Some(left_entry) =
									state.left_entries.get(&key_hash)
								{
									// For LEFT JOIN: if this is the first right row
									// for this key, we need to remove the
									// NULL-joined rows first
									if matches!(self.join_type, JoinType::Left)
										&& is_first_right_row
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
											// Remove the left row that was
											// previously emitted
											result.push(FlowDiff::Remove {
												pre: left_row,
											});
										}
									}

									// Now add the properly joined rows
									for left_row_ser in &left_entry.rows {
										let left_row = left_row_ser
											.to_left_row(&state.layout);
										let joined_row = self
											.join_rows(&left_row, &post);
										result.push(FlowDiff::Insert {
											post: joined_row,
										});
									}
								} else {
								}
							}
							// Right side inserts with undefined keys don't produce output
						}
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					match side {
						JoinSide::Left => {
							let key_hash_opt = self.compute_join_key(
								&pre,
								&self.left_exprs,
								evaluator,
							)?;

							if let Some(key_hash) = key_hash_opt {
								// Remove from left entries
								if let Some(left_entry) =
									state.left_entries.get_mut(&key_hash)
								{
									left_entry
										.rows
										.retain(|r| r.number != pre.number);

									// Remove all joins involving this row
									if let Some(right_entry) =
										state.right_entries.get(&key_hash)
									{
										for right_row_ser in &right_entry.rows {
											let right_row = right_row_ser
												.to_right_row(
													&state.layout,
												);
											let joined_row = self
												.join_rows(
													&pre,
													&right_row,
												);
											result.push(FlowDiff::Remove {
												pre: joined_row,
											});
										}
									} else if matches!(
										self.join_type,
										JoinType::Left
									) {
										// Remove the unmatched left join row
										result.push(FlowDiff::Remove {
											pre: pre.clone(),
										});
									}

									// Clean up empty entries
									if left_entry.rows.is_empty() {
										state.left_entries.remove(&key_hash);
									}
								}
							} else {
								// Undefined key - if it's a left join, remove the
								// unmatched row
								if matches!(self.join_type, JoinType::Left) {
									result.push(FlowDiff::Remove {
										pre: pre.clone(),
									});
								}
							}
						}
						JoinSide::Right => {
							let key_hash_opt = self.compute_join_key(
								&pre,
								&self.right_exprs,
								evaluator,
							)?;

							if let Some(key_hash) = key_hash_opt {
								// Remove from right entries
								if let Some(right_entry) =
									state.right_entries.get_mut(&key_hash)
								{
									right_entry
										.rows
										.retain(|r| r.number != pre.number);

									// Remove all joins involving this row
									if let Some(left_entry) =
										state.left_entries.get(&key_hash)
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
											let joined_row = self
												.join_rows(
													&left_row, &pre,
												);
											result.push(FlowDiff::Remove {
												pre: joined_row,
											});
										}

										// For LEFT JOIN: if this was the last
										// right row for this key,
										// emit new rows with
										// NULL right side for all left rows
										if matches!(
											self.join_type,
											JoinType::Left
										) && right_entry.rows.is_empty()
										{
											for left_row_ser in
												&left_entry.rows
											{
												let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
												// Re-add the left row
												// for left
												// join
												result.push(FlowDiff::Insert {
												post: left_row,
											});
											}
										}
									}

									// Clean up empty entries
									if right_entry.rows.is_empty() {
										state.right_entries.remove(&key_hash);
									}
								}
							}
						}
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Handle as remove + insert
					// This is simplified - a more efficient implementation would handle key changes
					// specially
					match side {
						JoinSide::Left => {
							let old_key_opt = self.compute_join_key(
								&pre,
								&self.left_exprs,
								evaluator,
							)?;
							let new_key_opt = self.compute_join_key(
								&post,
								&self.left_exprs,
								evaluator,
							)?;

							if old_key_opt == new_key_opt {
								// Key didn't change, update in place
								if let Some(old_key) = old_key_opt {
									if let Some(left_entry) =
										state.left_entries.get_mut(&old_key)
									{
										// Update the row
										for row in &mut left_entry.rows {
											if row.number == pre.number {
												*row = SerializedRow::from_row(
													&post,
												);
												break;
											}
										}

										// Emit updates for all joined rows
										if let Some(right_entry) = state
											.right_entries
											.get(&old_key)
										{
											for right_row_ser in
												&right_entry.rows
											{
												let right_row = right_row_ser
													.to_right_row(
														&state.layout,
													);
												let old_joined = self
													.join_rows(
													&pre,
													&right_row,
												);
												let new_joined = self
													.join_rows(
													&post,
													&right_row,
												);
												result.push(FlowDiff::Update {
													pre: old_joined,
													post: new_joined,
												});
											}
										} else if matches!(
											self.join_type,
											JoinType::Left
										) {
											result.push(FlowDiff::Update {
												pre: pre.clone(),
												post: post.clone(),
											});
										}
									}
								} else {
									// Both keys are undefined - for left join, just
									// update the row
									if matches!(self.join_type, JoinType::Left) {
										result.push(FlowDiff::Update {
											pre: pre.clone(),
											post: post.clone(),
										});
									}
								}
							} else {
								// Key changed - treat as remove + insert
								// Process remove...
								if let Some(old_key) = old_key_opt {
									if let Some(left_entry) =
										state.left_entries.get_mut(&old_key)
									{
										left_entry.rows.retain(|r| {
											r.number != pre.number
										});
										if let Some(right_entry) = state
											.right_entries
											.get(&old_key)
										{
											for right_row_ser in
												&right_entry.rows
											{
												let right_row = right_row_ser
												.to_right_row(
													&state.layout,
												);
												let joined_row = self
													.join_rows(
													&pre,
													&right_row,
												);
												result.push(FlowDiff::Remove {
												pre: joined_row,
											});
											}
										} else if matches!(
											self.join_type,
											JoinType::Left
										) {
											result.push(FlowDiff::Remove {
												pre: pre.clone(),
											});
										}
										if left_entry.rows.is_empty() {
											state.left_entries
												.remove(&old_key);
										}
									}
								} else if matches!(self.join_type, JoinType::Left) {
									// Old key was undefined - remove the unmatched
									// row
									result.push(FlowDiff::Remove {
										pre: pre.clone(),
									});
								}

								// Process insert...
								if let Some(new_key) = new_key_opt {
									let serialized = SerializedRow::from_row(&post);
									state.left_entries
										.entry(new_key)
										.or_insert_with(|| JoinSideEntry {
											rows: Vec::new(),
										})
										.rows
										.push(serialized);

									if let Some(right_entry) =
										state.right_entries.get(&new_key)
									{
										for right_row_ser in &right_entry.rows {
											let right_row = right_row_ser
												.to_right_row(
													&state.layout,
												);
											let joined_row = self
												.join_rows(
													&post,
													&right_row,
												);
											result.push(FlowDiff::Insert {
												post: joined_row,
											});
										}
									} else if matches!(
										self.join_type,
										JoinType::Left
									) {
										result.push(FlowDiff::Insert {
											post: post.clone(),
										});
									}
								} else if matches!(self.join_type, JoinType::Left) {
									// New key is undefined - insert unmatched row
									result.push(FlowDiff::Insert {
										post: post.clone(),
									});
								}
							}
						}
						JoinSide::Right => {
							// Similar logic for right side updates
							let old_key_opt = self.compute_join_key(
								&pre,
								&self.right_exprs,
								evaluator,
							)?;
							let new_key_opt = self.compute_join_key(
								&post,
								&self.right_exprs,
								evaluator,
							)?;

							if old_key_opt == new_key_opt {
								// Key didn't change, update in place
								if let Some(old_key) = old_key_opt {
									if let Some(right_entry) =
										state.right_entries.get_mut(&old_key)
									{
										// Update the row
										for row in &mut right_entry.rows {
											if row.number == pre.number {
												*row = SerializedRow::from_row(
												&post,
											);
												break;
											}
										}

										// Emit updates for all joined rows
										if let Some(left_entry) =
											state.left_entries.get(&old_key)
										{
											for left_row_ser in
												&left_entry.rows
											{
												let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
												let old_joined = self
													.join_rows(
													&left_row, &pre,
												);
												let new_joined = self
													.join_rows(
													&left_row,
													&post,
												);
												result.push(FlowDiff::Update {
												pre: old_joined,
												post: new_joined,
											});
											}
										}
									}
								}
							} else {
								// Key changed - treat as remove + insert
								// Process remove...
								if let Some(old_key) = old_key_opt {
									if let Some(right_entry) =
										state.right_entries.get_mut(&old_key)
									{
										right_entry.rows.retain(|r| {
											r.number != pre.number
										});
										if let Some(left_entry) =
											state.left_entries.get(&old_key)
										{
											for left_row_ser in
												&left_entry.rows
											{
												let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
												let joined_row = self
													.join_rows(
													&left_row, &pre,
												);
												result.push(FlowDiff::Remove {
												pre: joined_row,
											});
											}
										}
										if right_entry.rows.is_empty() {
											state.right_entries
												.remove(&old_key);
										}
									}
								}

								// Process insert...
								if let Some(new_key) = new_key_opt {
									let serialized = SerializedRow::from_row(&post);
									state.right_entries
										.entry(new_key)
										.or_insert_with(|| JoinSideEntry {
											rows: Vec::new(),
										})
										.rows
										.push(serialized);

									if let Some(left_entry) =
										state.left_entries.get(&new_key)
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(
													&state.layout,
												);
											let joined_row = self
												.join_rows(
													&left_row,
													&post,
												);
											result.push(FlowDiff::Insert {
												post: joined_row,
											});
										}
									}
								}
							}
						}
					}
				}
			}
		}

		self.save_join_state(txn, &state)?;

		Ok(FlowChange::internal(self.node, result))
	}
}
