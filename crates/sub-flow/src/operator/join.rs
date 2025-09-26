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
		stateful::{RawStatefulOperator, SingleStateful, state_get, state_remove, state_set},
	},
};

static EMPTY_PARAMS: Params = Params::None;

/// A key-value store backed by the stateful storage system
/// Provides HashMap-like interface while storing data persistently
struct Store<T> {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
	_phantom: std::marker::PhantomData<T>,
}

impl Store<JoinSideEntry> {
	fn new(node_id: FlowNodeId, side: JoinSide) -> Self {
		// Use different prefixes for left and right stores
		let prefix = match side {
			JoinSide::Left => vec![0x01],
			JoinSide::Right => vec![0x02],
		};
		Self {
			node_id,
			prefix,
			_phantom: std::marker::PhantomData,
		}
	}

	fn make_key(&self, hash: &Hash128) -> reifydb_core::EncodedKey {
		let mut key_bytes = self.prefix.clone();
		// Hash128 is a tuple struct containing u128
		key_bytes.extend_from_slice(&hash.0.to_le_bytes());
		reifydb_core::EncodedKey::new(key_bytes)
	}

	fn get<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
	) -> crate::Result<Option<JoinSideEntry>> {
		let key = self.make_key(hash);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				// Deserialize JoinSideEntry from the row
				let layout = EncodedRowLayout::new(&[Type::Blob]);
				let blob = layout.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let config = standard();
				let (entry, _): (JoinSideEntry, usize) = decode_from_slice(blob.as_ref(), config)
					.map_err(|e| {
						Error(internal_error!("Failed to deserialize JoinSideEntry: {}", e))
					})?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	fn set<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		entry: &JoinSideEntry,
	) -> crate::Result<()> {
		let key = self.make_key(hash);

		// Serialize JoinSideEntry
		let config = standard();
		let serialized = encode_to_vec(entry, config)
			.map_err(|e| Error(internal_error!("Failed to serialize JoinSideEntry: {}", e)))?;

		// Store as a blob in an EncodedRow
		let layout = EncodedRowLayout::new(&[Type::Blob]);
		let mut row = layout.allocate_row();
		let blob = Blob::from(serialized);
		layout.set_blob(&mut row, 0, &blob);

		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	fn contains_key<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
	) -> crate::Result<bool> {
		let key = self.make_key(hash);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}

	fn remove<T: Transaction>(&self, txn: &mut StandardCommandTransaction<T>, hash: &Hash128) -> crate::Result<()> {
		let key = self.make_key(hash);
		state_remove(self.node_id, txn, &key)?;
		Ok(())
	}

	fn get_or_insert_with<T: Transaction, F>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		f: F,
	) -> crate::Result<JoinSideEntry>
	where
		F: FnOnce() -> JoinSideEntry,
	{
		if let Some(entry) = self.get(txn, hash)? {
			Ok(entry)
		} else {
			let entry = f();
			self.set(txn, hash, &entry)?;
			Ok(entry)
		}
	}

	fn update_entry<T: Transaction, F>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		f: F,
	) -> crate::Result<()>
	where
		F: FnOnce(&mut JoinSideEntry),
	{
		if let Some(mut entry) = self.get(txn, hash)? {
			f(&mut entry);
			self.set(txn, hash, &entry)?;
		}
		Ok(())
	}
}

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

/// The complete join state - now uses Stores instead of HashMaps
struct JoinState<'a, T: Transaction> {
	// Layout is stored separately and loaded once
	layout: JoinLayout,
	// Store for left side entries
	left_store: Store<JoinSideEntry>,
	// Store for right side entries
	right_store: Store<JoinSideEntry>,
	// Keep reference to transaction for operations
	txn: &'a mut StandardCommandTransaction<T>,
}

impl<'a, T: Transaction> JoinState<'a, T> {
	fn new(node_id: FlowNodeId, layout: JoinLayout, txn: &'a mut StandardCommandTransaction<T>) -> Self {
		Self {
			layout,
			left_store: Store::new(node_id, JoinSide::Left),
			right_store: Store::new(node_id, JoinSide::Right),
			txn,
		}
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

	fn load_join_layout<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
	) -> crate::Result<JoinLayout> {
		// Load layout from a special key (empty key)
		let layout_key = reifydb_core::EncodedKey::new(vec![0x00]); // Special key for layout
		match state_get(self.node, txn, &layout_key)? {
			Some(row) => {
				// Deserialize JoinLayout from the row
				let blob = self.layout.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(JoinLayout::new());
				}
				let config = standard();
				let (layout, _): (JoinLayout, usize) = decode_from_slice(blob.as_ref(), config)
					.map_err(|e| {
						Error(internal_error!("Failed to deserialize JoinLayout: {}", e))
					})?;
				Ok(layout)
			}
			None => Ok(JoinLayout::new()),
		}
	}

	fn save_join_layout<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		layout: &JoinLayout,
	) -> crate::Result<()> {
		// Save layout to a special key (empty key)
		let layout_key = reifydb_core::EncodedKey::new(vec![0x00]); // Special key for layout

		let config = standard();
		let serialized = encode_to_vec(layout, config)
			.map_err(|e| Error(internal_error!("Failed to serialize JoinLayout: {}", e)))?;

		// Store as a blob in an EncodedRow
		let mut row = self.layout.allocate_row();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut row, 0, &blob);

		state_set(self.node, txn, &layout_key, row)?;
		Ok(())
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

		// Load the layout and create the state with stores
		let mut layout = self.load_join_layout(txn)?;
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
							layout.update_left_from_row(&post);
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
								let left_store = Store::new(self.node, JoinSide::Left);
								let is_new_key =
									!left_store.contains_key(txn, &key_hash)?;
								let mut entry = left_store.get_or_insert_with(
									txn,
									&key_hash,
									|| JoinSideEntry {
										rows: Vec::new(),
									},
								)?;
								entry.rows.push(serialized.clone());
								left_store.set(txn, &key_hash, &entry)?;
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
								let right_store =
									Store::new(self.node, JoinSide::Right);
								if let Some(right_entry) =
									right_store.get(txn, &key_hash)?
								{
									for right_row_ser in &right_entry.rows {
										let right_row = right_row_ser
											.to_right_row(&layout);
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
							layout.update_right_from_row(&post);
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
								let right_store =
									Store::new(self.node, JoinSide::Right);
								let is_first_right_row =
									!right_store.contains_key(txn, &key_hash)?;

								// Add to right entries
								let serialized = SerializedRow::from_row(&post);
								let mut right_entry = right_store.get_or_insert_with(
									txn,
									&key_hash,
									|| JoinSideEntry {
										rows: Vec::new(),
									},
								)?;
								right_entry.rows.push(serialized);
								right_store.set(txn, &key_hash, &right_entry)?;

								// Join with all matching left rows
								let left_store = Store::new(self.node, JoinSide::Left);
								if let Some(left_entry) =
									left_store.get(txn, &key_hash)?
								{
									// For LEFT JOIN: if this is the first right row
									// for this key, we need to remove the
									// NULL-joined rows first
									if matches!(self.join_type, JoinType::Left)
										&& is_first_right_row
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(&layout);
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
											.to_left_row(&layout);
										let joined_row = self
											.join_rows(&left_row, &post);
										result.push(FlowDiff::Insert {
											post: joined_row,
										});
									}
								} else {
									// Layout remains unchanged
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
								let left_store = Store::new(self.node, JoinSide::Left);
								if let Some(mut left_entry) =
									left_store.get(txn, &key_hash)?
								{
									left_entry
										.rows
										.retain(|r| r.number != pre.number);

									// Remove all joins involving this row
									let right_store =
										Store::new(self.node, JoinSide::Right);
									if let Some(right_entry) =
										right_store.get(txn, &key_hash)?
									{
										for right_row_ser in &right_entry.rows {
											let right_row = right_row_ser
												.to_right_row(&layout);
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
										left_store.remove(txn, &key_hash)?;
									} else {
										// Save the updated entry
										left_store.set(
											txn,
											&key_hash,
											&left_entry,
										)?;
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
								let right_store =
									Store::new(self.node, JoinSide::Right);
								if let Some(mut right_entry) =
									right_store.get(txn, &key_hash)?
								{
									right_entry
										.rows
										.retain(|r| r.number != pre.number);

									// Remove all joins involving this row
									let left_store =
										Store::new(self.node, JoinSide::Left);
									if let Some(left_entry) =
										left_store.get(txn, &key_hash)?
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(&layout);
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
												let left_row =
													left_row_ser
														.to_left_row(
														&layout,
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
										right_store.remove(txn, &key_hash)?;
									} else {
										// Save the updated entry
										right_store.set(
											txn,
											&key_hash,
											&right_entry,
										)?;
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
									let left_store =
										Store::new(self.node, JoinSide::Left);
									if let Some(mut left_entry) =
										left_store.get(txn, &old_key)?
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

										left_store.set(
											txn,
											&old_key,
											&left_entry,
										)?;

										// Emit updates for all joined rows
										let right_store = Store::new(
											self.node,
											JoinSide::Right,
										);
										if let Some(right_entry) =
											right_store
												.get(txn, &old_key)?
										{
											for right_row_ser in
												&right_entry.rows
											{
												let right_row =
													right_row_ser
														.to_right_row(
														&layout,
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
									let left_store =
										Store::new(self.node, JoinSide::Left);
									if let Some(mut left_entry) =
										left_store.get(txn, &old_key)?
									{
										left_entry.rows.retain(|r| {
											r.number != pre.number
										});

										let right_store = Store::new(
											self.node,
											JoinSide::Right,
										);
										if let Some(right_entry) =
											right_store
												.get(txn, &old_key)?
										{
											for right_row_ser in
												&right_entry.rows
											{
												let right_row =
													right_row_ser
														.to_right_row(
														&layout,
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
											left_store.remove(
												txn, &old_key,
											)?;
										} else {
											left_store.set(
												txn,
												&old_key,
												&left_entry,
											)?;
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
									let left_store =
										Store::new(self.node, JoinSide::Left);
									let mut left_entry = left_store
										.get_or_insert_with(
											txn,
											&new_key,
											|| JoinSideEntry {
												rows: Vec::new(),
											},
										)?;
									left_entry.rows.push(serialized);
									left_store.set(txn, &new_key, &left_entry)?;

									let right_store =
										Store::new(self.node, JoinSide::Right);
									if let Some(right_entry) =
										right_store.get(txn, &new_key)?
									{
										for right_row_ser in &right_entry.rows {
											let right_row = right_row_ser
												.to_right_row(&layout);
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
									let right_store =
										Store::new(self.node, JoinSide::Right);
									if let Some(mut right_entry) =
										right_store.get(txn, &old_key)?
									{
										// Update the row
										for row in &mut right_entry.rows {
											if row.number == pre.number {
												*row = SerializedRow::from_row(&post);
												break;
											}
										}
										right_store.set(
											txn,
											&old_key,
											&right_entry,
										)?;

										// Emit updates for all joined rows
										let left_store = Store::new(
											self.node,
											JoinSide::Left,
										);
										if let Some(left_entry) =
											left_store.get(txn, &old_key)?
										{
											for left_row_ser in
												&left_entry.rows
											{
												let left_row =
													left_row_ser
														.to_left_row(
														&layout,
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
									let right_store =
										Store::new(self.node, JoinSide::Right);
									if let Some(mut right_entry) =
										right_store.get(txn, &old_key)?
									{
										right_entry.rows.retain(|r| {
											r.number != pre.number
										});

										let left_store = Store::new(
											self.node,
											JoinSide::Left,
										);
										if let Some(left_entry) =
											left_store.get(txn, &old_key)?
										{
											for left_row_ser in
												&left_entry.rows
											{
												let left_row =
													left_row_ser
														.to_left_row(
														&layout,
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
											right_store.remove(
												txn, &old_key,
											)?;
										} else {
											right_store.set(
												txn,
												&old_key,
												&right_entry,
											)?;
										}
									}
								}

								// Process insert...
								if let Some(new_key) = new_key_opt {
									let serialized = SerializedRow::from_row(&post);
									let right_store =
										Store::new(self.node, JoinSide::Right);
									let mut right_entry = right_store
										.get_or_insert_with(
											txn,
											&new_key,
											|| JoinSideEntry {
												rows: Vec::new(),
											},
										)?;
									right_entry.rows.push(serialized);
									right_store.set(txn, &new_key, &right_entry)?;

									let left_store =
										Store::new(self.node, JoinSide::Left);
									if let Some(left_entry) =
										left_store.get(txn, &new_key)?
									{
										for left_row_ser in &left_entry.rows {
											let left_row = left_row_ser
												.to_left_row(&layout);
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

		// Save the updated layout
		self.save_join_layout(txn, &layout)?;

		Ok(FlowChange::internal(self.node, result))
	}
}
