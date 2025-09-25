// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::{
	EncodedKey, EncodedKeyRange, JoinType,
	flow::{FlowChange, FlowDiff, FlowNodeDef},
	interface::{
		EvaluationContext, Evaluator, FlowNodeId, Params, SourceId, Transaction,
		evaluate::expression::{ColumnExpression, Expression},
	},
	util::CowVec,
	value::{
		column::{Column, ColumnData, Columns},
		row::EncodedRow,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::{Fragment, RowNumber, Value};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{
		Operator,
		transform::{TransformOperator, stateful::RawStatefulOperator},
	},
};

// Stored row data for join state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRow {
	row_id: RowNumber,
	source_name: String,
	columns: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinMetadata {
	join_instance_id: u64,
	left_source: String,
	right_source: String,
	initialized: bool,
	// Store column names from each source to create undefined values
	left_columns: Vec<String>,
	right_columns: Vec<String>,
	// Track if this is a repeated table reference
	left_instance_suffix: Option<String>,
	right_instance_suffix: Option<String>,
	// Track which nodes feed into which side of the join
	#[serde(default)]
	initialized_nodes: std::collections::HashMap<String, String>,
}

pub struct JoinOperator {
	node: FlowNodeId,
	join_type: JoinType,
	left_keys: Vec<Expression<'static>>,
	right_keys: Vec<Expression<'static>>,
	left_schema: FlowNodeDef,
	right_schema: FlowNodeDef,
	join_instance_id: u64,
	// Track which source corresponds to which side for repeated tables
	// This should be populated when multiple joins use the same table
	left_source_instance_suffix: Option<String>,
	right_source_instance_suffix: Option<String>,
}

impl JoinOperator {
	pub fn new(
		node: FlowNodeId,
		join_type: JoinType,
		left_keys: Vec<Expression<'static>>,
		right_keys: Vec<Expression<'static>>,
		left_schema: FlowNodeDef,
		right_schema: FlowNodeDef,
	) -> Self {
		Self {
			node,
			join_type,
			left_keys,
			right_keys,
			left_schema,
			right_schema,
			join_instance_id: node.0, /* Use node id as instance
			                           * id by default */
			left_source_instance_suffix: None,
			right_source_instance_suffix: None,
		}
	}

	// Helper properties for compatibility
	fn flow_id(&self) -> u64 {
		1 // Default flow ID
	}

	fn node_id(&self) -> u64 {
		self.node.0
	}

	pub fn with_instance_id(mut self, instance_id: u64) -> Self {
		self.join_instance_id = instance_id;
		self
	}

	pub fn with_flow_id(self, _flow_id: u64) -> Self {
		// No longer needed, kept for compatibility
		self
	}

	pub fn with_source_suffixes(mut self, left_suffix: Option<String>, right_suffix: Option<String>) -> Self {
		self.left_source_instance_suffix = left_suffix;
		self.right_source_instance_suffix = right_suffix;
		self
	}

	// Create encoded key for join state
	fn make_join_key(side: u8, join_key_hash: u64, row_id: RowNumber) -> EncodedKey {
		let mut key = Vec::new();
		key.push(side);
		key.extend(&join_key_hash.to_be_bytes());
		key.extend(&row_id.to_be_bytes());
		EncodedKey::new(key)
	}

	// Create range for scanning join entries
	fn make_join_range(side: u8, join_key_hash: u64) -> (Option<EncodedKey>, Option<EncodedKey>) {
		let mut start = Vec::new();
		start.push(side);
		start.extend(&join_key_hash.to_be_bytes());

		let mut end = start.clone();
		end.extend(&u64::MAX.to_be_bytes());

		(Some(EncodedKey::new(start)), Some(EncodedKey::new(end)))
	}

	// Extract join key values from columns
	fn extract_join_keys(
		evaluator: &StandardEvaluator,
		columns: &Columns,
		row_idx: usize,
		expressions: &[Expression<'static>],
	) -> Result<Vec<Value>> {
		let mut keys = Vec::new();
		let row_count = columns.row_count();
		let empty_params = Params::None;

		let eval_ctx = EvaluationContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &empty_params,
		};

		for expr in expressions {
			// For AccessSource expressions, we need to extract just
			// the column name since we're evaluating against the
			// current table's columns
			let column_expr = match expr {
				Expression::AccessSource(access) => {
					// Convert AccessSource to a simple
					// Column expression with just the
					// column name
					Expression::Column(ColumnExpression(access.column.clone()))
				}
				_ => expr.clone(),
			};

			let result = evaluator.evaluate(&eval_ctx, &column_expr)?;
			let value = result.data().get_value(row_idx);
			keys.push(value);
		}
		Ok(keys)
	}

	// Hash join keys for efficient lookup
	fn hash_join_keys(keys: &[Value]) -> u64 {
		use std::{
			collections::hash_map::DefaultHasher,
			hash::{Hash, Hasher},
		};

		let mut hasher = DefaultHasher::new();
		for key in keys {
			key.hash(&mut hasher);
		}
		hasher.finish()
	}

	// Store a row in the join state
	fn store_row<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
		columns: &Columns,
		row_idx: usize,
	) -> Result<()> {
		// Extract source name from the first column (all columns should
		// have same source)
		let source_name = Fragment::owned_internal("unknown");

		// Check if we already have this row stored
		let key = Self::make_join_key(side, join_key_hash, row_id);

		// Get existing row data if it exists
		let mut column_values = match self.state_get(txn, &key) {
			Ok(Some(existing)) if !existing.as_ref().is_empty() => {
				// Deserialize existing row and start with its columns
				if let Ok(existing_row) = serde_json::from_slice::<StoredRow>(existing.as_ref()) {
					existing_row.columns
				} else {
					HashMap::new()
				}
			}
			_ => HashMap::new(),
		};

		// Add/update with new column values
		for column in columns.iter() {
			let name = column.name().text().to_string();
			let value = column.data().get_value(row_idx);
			column_values.insert(name, value);
		}

		let stored_row = StoredRow {
			row_id,
			source_name: source_name.text().to_string(),
			columns: column_values,
		};

		let serialized = serde_json::to_vec(&stored_row).unwrap_or_default();
		self.state_set(txn, &key, EncodedRow(CowVec::new(serialized)))?;
		Ok(())
	}

	// Retrieve matching rows from the other side
	fn get_matching_rows<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		side: u8,
		join_key_hash: u64,
	) -> Result<Vec<StoredRow>> {
		let mut rows = Vec::new();
		let (start, end) = Self::make_join_range(side, join_key_hash);

		// Scan the range for matching rows
		use std::ops::Bound;
		let range = EncodedKeyRange::new(
			start.map_or(Bound::Unbounded, Bound::Included),
			end.map_or(Bound::Unbounded, Bound::Excluded),
		);
		if let Ok(iter) = self.state_range(txn, range) {
			for (_, row_data) in iter {
				if !row_data.as_ref().is_empty() {
					if let Ok(stored_row) = serde_json::from_slice::<StoredRow>(row_data.as_ref()) {
						rows.push(stored_row);
					}
				}
			}
		}

		Ok(rows)
	}

	// Delete a row from the join state
	fn delete_row<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
	) -> Result<()> {
		let key = Self::make_join_key(side, join_key_hash, row_id);
		self.state_remove(txn, &key)?;
		Ok(())
	}

	// Get or initialize metadata
	// Returns (metadata, is_left_side)
	fn get_or_init_metadata_with_node<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		columns: &Columns,
		left_schema: &FlowNodeDef,
		right_schema: &FlowNodeDef,
		from_node_id: u64,
	) -> Result<(JoinMetadata, bool)> {
		// Use a special key for metadata (empty key)
		let metadata_key = EncodedKey::new(Vec::new());

		// Try to get existing metadata
		if let Ok(Some(data)) = self.state_get(txn, &metadata_key) {
			if !data.as_ref().is_empty() {
				if let Ok(mut metadata) = serde_json::from_slice::<JoinMetadata>(data.as_ref()) {
					// Check if we need to track this node
					let node_key = format!("node_{}", from_node_id);

					// Determine if this is left or right
					// based on tracked nodes
					let is_left = if let Some(side) = metadata.initialized_nodes.get(&node_key) {
						*side == "left"
					} else {
						// First time seeing this node,
						// need to determine which
						// side it is If we
						// already have a left node
						// tracked, this must be
						// right
						let has_left = metadata.initialized_nodes.values().any(|v| v == "left");
						let is_left = !has_left;

						// Track this node
						metadata.initialized_nodes.insert(
							node_key,
							if is_left {
								"left".to_string()
							} else {
								"right".to_string()
							},
						);

						if is_left && metadata.left_source.is_empty() {
							metadata.left_source = "unknown".to_string();
						} else if !is_left && metadata.right_source.is_empty() {
							metadata.right_source = "unknown".to_string();
						}

						// Save updated metadata
						let data = serde_json::to_vec(&metadata).unwrap_or_default();
						self.state_set(txn, &metadata_key, EncodedRow(CowVec::new(data)))?;

						is_left
					};

					return Ok((metadata, is_left));
				}
			}
		}

		// Initialize new metadata - this is the first node we're seeing
		let source_name = Fragment::owned_internal("unknown");

		// Determine if this node should be left or right based on
		// namespaces Check if the source matches the right namespace
		let is_right_source =
			right_schema.source_name.as_ref().map(|s| s == source_name.text()).unwrap_or(false);

		let is_left = !is_right_source;

		let mut initialized_nodes = std::collections::HashMap::new();
		initialized_nodes.insert(
			format!("node_{}", from_node_id),
			if is_left {
				"left".to_string()
			} else {
				"right".to_string()
			},
		);

		let metadata = if is_left {
			JoinMetadata {
				join_instance_id: self.join_instance_id,
				left_source: source_name.text().to_string(),
				right_source: right_schema.source_name.clone().unwrap_or_default(),
				initialized: false,
				left_columns: columns.iter().map(|c| c.name().text().to_string()).collect(),
				right_columns: right_schema.columns.iter().map(|c| c.name.clone()).collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes,
			}
		} else {
			JoinMetadata {
				join_instance_id: self.join_instance_id,
				left_source: left_schema.source_name.clone().unwrap_or_default(),
				right_source: source_name.text().to_string(),
				initialized: false,
				left_columns: left_schema.columns.iter().map(|c| c.name.clone()).collect(),
				right_columns: columns.iter().map(|c| c.name().text().to_string()).collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes,
			}
		};

		// Store the metadata
		let data = serde_json::to_vec(&metadata).unwrap_or_default();
		self.state_set(txn, &metadata_key, EncodedRow(CowVec::new(data)))?;

		Ok((metadata, is_left))
	}

	fn get_or_init_metadata<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		columns: &Columns,
		left_schema: &FlowNodeDef,
		right_schema: &FlowNodeDef,
	) -> Result<(JoinMetadata, bool)> {
		// Use a special key for metadata (empty key)
		let metadata_key = EncodedKey::new(Vec::new());

		// Extract source name from columns
		let source_name = Fragment::owned_internal("unknown");

		// Try to get existing metadata
		let metadata_key = EncodedKey::new(Vec::new());
		if let Ok(Some(data)) = self.state_get(txn, &metadata_key) {
			if !data.as_ref().is_empty() {
				if let Ok(metadata) = serde_json::from_slice::<JoinMetadata>(data.as_ref()) {
					// Determine if this is the left or right source using column names
					let column_names: Vec<String> =
						columns.iter().map(|c| c.name().text().to_string()).collect();

					// Check which side has more matching columns
					let left_matches = metadata
						.left_columns
						.iter()
						.filter(|col| column_names.contains(col))
						.count();
					let right_matches = metadata
						.right_columns
						.iter()
						.filter(|col| column_names.contains(col))
						.count();

					// Use column overlap to determine which side this is
					let is_left = if left_matches > right_matches {
						true
					} else if right_matches > left_matches {
						false
					} else {
						// Fall back to source name if column matching is ambiguous
						source_name == metadata.left_source
					};

					return Ok((metadata, is_left));
				}
			}
		}

		// Initialize metadata - determine if this is left or right using column matching
		// Compare incoming columns with schema columns to determine which side
		let column_names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();

		// Count matching columns with each schema
		let left_schema_matches =
			left_schema.columns.iter().filter(|col| column_names.contains(&col.name)).count();
		let right_schema_matches =
			right_schema.columns.iter().filter(|col| column_names.contains(&col.name)).count();

		// Determine side based on which schema has more matching columns
		let is_left = if left_schema_matches > right_schema_matches {
			true
		} else if right_schema_matches > left_schema_matches {
			false
		} else {
			// If column matching is ambiguous, fall back to source name matching
			left_schema.source_name.as_ref().map(|s| s == source_name.text()).unwrap_or(true)
		};

		let metadata = if is_left {
			// This is the left source
			JoinMetadata {
				join_instance_id: self.join_instance_id,
				left_source: source_name.text().to_string(),
				right_source: right_schema.source_name.clone().unwrap_or_default(),
				initialized: false,
				left_columns: columns.iter().map(|c| c.name().text().to_string()).collect(),
				right_columns: right_schema.columns.iter().map(|c| c.name.clone()).collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes: std::collections::HashMap::new(),
			}
		} else {
			// This is the right source
			JoinMetadata {
				join_instance_id: self.join_instance_id,
				left_source: left_schema.source_name.clone().unwrap_or_default(),
				right_source: source_name.text().to_string(),
				initialized: false,
				left_columns: left_schema.columns.iter().map(|c| c.name.clone()).collect(),
				right_columns: columns.iter().map(|c| c.name().text().to_string()).collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes: std::collections::HashMap::new(),
			}
		};

		let serialized = serde_json::to_vec(&metadata).unwrap_or_default();
		self.state_set(txn, &metadata_key, EncodedRow(CowVec::new(serialized)))?;

		Ok((metadata, is_left))
	}

	// Update metadata with right source
	fn update_metadata<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		mut metadata: JoinMetadata,
		columns: &Columns,
	) -> Result<()> {
		if metadata.right_source.is_empty() {
			metadata.right_source = "unknown".to_string();
			metadata.right_columns = columns.iter().map(|c| c.name().text().to_string()).collect();
			metadata.initialized = true;

			// Use a special key for metadata (empty key)
			let metadata_key = EncodedKey::new(Vec::new());
			let serialized = serde_json::to_vec(&metadata).unwrap_or_default();
			self.state_set(txn, &metadata_key, EncodedRow(CowVec::new(serialized)))?;
		}
		Ok(())
	}

	// Combine left and right rows into output columns
	fn combine_rows(&self, left_row: &StoredRow, right_row: Option<&StoredRow>, _source_id: SourceId) -> FlowDiff {
		let mut column_vec = Vec::new();
		let row_ids = vec![left_row.row_id];

		// Add left columns with full qualification from namespace
		// We need to output ALL columns from the left namespace, not
		// just what's stored
		for column_def in &self.left_schema.columns {
			// Check if we have this column value in the stored row
			let data = if let Some(value) = left_row.columns.get(&column_def.name) {
				value_to_column_data(value)
			} else {
				// Column not in stored data - this shouldn't
				// happen for left side but we'll handle
				// gracefully with undefined
				ColumnData::undefined(1)
			};

			column_vec.push(Column {
				name: Fragment::owned_internal(column_def.name.clone()),
				data,
			});
		}

		// Add right columns or NULLs for LEFT JOIN
		if let Some(right) = right_row {
			// We have a matching right row - output all right
			// columns
			for column_def in &self.right_schema.columns {
				// Check if we have this column value in the
				// stored row
				let data = if let Some(value) = right.columns.get(&column_def.name) {
					value_to_column_data(value)
				} else {
					// Column not in stored data - use
					// undefined
					ColumnData::undefined(1)
				};

				if let (Some(namespace), Some(source)) =
					(&self.right_schema.namespace_name, &self.right_schema.source_name)
				{
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data,
					});
				} else if let Some(source) = &self.right_schema.source_name {
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data,
					});
				} else {
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data,
					});
				}
			}
		} else {
			// For LEFT JOIN with no match, add NULL values for
			// right columns using namespace
			for column_def in &self.right_schema.columns {
				if let (Some(namespace), Some(source)) =
					(&self.right_schema.namespace_name, &self.right_schema.source_name)
				{
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data: ColumnData::undefined(1),
					});
				} else if let Some(source) = &self.right_schema.source_name {
					// Fallback to source qualified
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data: ColumnData::undefined(1),
					});
				} else {
					// Fallback to unqualified (shouldn't
					// happen)
					column_vec.push(Column {
						name: Fragment::owned_internal(column_def.name.clone()),
						data: ColumnData::undefined(1),
					});
				}
			}
		}

		let columns = Columns::new(column_vec);

		FlowDiff::Insert {
			source: SourceId::FlowNode(self.node),
			rows: CowVec::new(row_ids),
			post: columns,
		}
	}

	// Process an insert operation
	fn process_insert<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		evaluator: &StandardEvaluator,
		source: SourceId,
		row_ids: &[RowNumber],
		after: &Columns,
		is_left: bool,
		metadata: &JoinMetadata,
	) -> Result<Vec<FlowDiff>> {
		let mut output_diffs = Vec::new();
		let expressions = if is_left {
			&self.left_keys
		} else {
			&self.right_keys
		};
		let this_side = if is_left {
			0u8
		} else {
			1u8
		};
		let other_side = if is_left {
			1u8
		} else {
			0u8
		};

		let source_name = if is_left {
			Fragment::owned_internal(metadata.left_source.clone())
		} else {
			Fragment::owned_internal(metadata.right_source.clone())
		};

		for (idx, &row_id) in row_ids.iter().enumerate() {
			// Extract join keys
			let join_keys = Self::extract_join_keys(evaluator, after, idx, expressions)?;
			let join_key_hash = Self::hash_join_keys(&join_keys);

			// Store this row
			self.store_row(txn, this_side, join_key_hash, row_id, after, idx)?;

			// Get matching rows from the other side
			let other_rows = self.get_matching_rows(txn, other_side, join_key_hash)?;

			// Create a StoredRow for the current row
			let mut current_columns = HashMap::new();
			for column in after.iter() {
				let name = column.name().text().to_string();
				let value = column.data().get_value(idx);
				current_columns.insert(name, value);
			}
			let current_row = StoredRow {
				row_id,
				source_name: source_name.text().to_string(),
				columns: current_columns,
			};

			// For LEFT JOIN:
			// - If left side changes, emit the join result
			// - If right side changes AND there are matching left rows, emit updates
			// For INNER JOIN: emit for both sides
			if is_left {
				// Left side insert for LEFT or INNER join
				if other_rows.is_empty() {
					// LEFT JOIN with no match - emit with
					// NULLs for right side
					let diff = self.combine_rows(&current_row, None, source);
					output_diffs.push(diff);
				} else {
					// Emit joined rows for each match
					for other_row in &other_rows {
						let diff = self.combine_rows(&current_row, Some(other_row), source);
						output_diffs.push(diff);
					}
				}
			} else if matches!(self.join_type, JoinType::Inner) {
				// Right side insert for INNER join
				for other_row in &other_rows {
					let diff = self.combine_rows(other_row, Some(&current_row), source);
					output_diffs.push(diff);
				}
			} else if matches!(self.join_type, JoinType::Left) && !other_rows.is_empty() {
				// Right side insert for LEFT join - emit
				// updates for matching left rows
				// This is important! When a customer is added
				// that matches existing orders, we need to
				// emit those updated join results
				for other_row in &other_rows {
					let diff = self.combine_rows(other_row, Some(&current_row), source);
					// This should be an UPDATE not INSERT
					// since the left row already existed
					// But for now we'll emit as INSERT to
					// see if it works
					output_diffs.push(diff);
				}
			}
		}

		Ok(output_diffs)
	}

	// Process a remove operation
	fn process_remove<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		evaluator: &StandardEvaluator,
		source: SourceId,
		row_ids: &[RowNumber],
		before: &Columns,
		is_left: bool,
	) -> Result<Vec<FlowDiff>> {
		let mut output_diffs = Vec::new();
		let expressions = if is_left {
			&self.left_keys
		} else {
			&self.right_keys
		};
		let this_side = if is_left {
			0u8
		} else {
			1u8
		};
		let other_side = if is_left {
			1u8
		} else {
			0u8
		};

		for (idx, &row_id) in row_ids.iter().enumerate() {
			// Extract join keys
			let join_keys = Self::extract_join_keys(evaluator, before, idx, expressions)?;
			let join_key_hash = Self::hash_join_keys(&join_keys);

			// Get matching rows from the other side before deleting
			let other_rows = self.get_matching_rows(txn, other_side, join_key_hash)?;

			// Delete this row from state
			self.delete_row(txn, this_side, join_key_hash, row_id)?;

			// Generate removal diffs
			if is_left {
				// Left side delete for LEFT JOIN
				// Convert to owned/static columns
				let mut column_vec = Vec::new();
				for column in before.iter() {
					let static_col = Column {
						name: column.name.clone().to_static(),
						data: column.data.clone(),
					};
					column_vec.push(static_col);
				}
				let columns = Columns::new(column_vec);

				output_diffs.push(FlowDiff::Remove {
					source: SourceId::FlowNode(self.node),
					rows: CowVec::new(vec![row_id]),
					pre: columns,
				});
			} else if matches!(self.join_type, JoinType::Left) {
				// Right side delete for LEFT JOIN
				// Emit updates for affected left rows showing
				// NULL for right columns
				for other_row in &other_rows {
					let diff = self.combine_rows(
						other_row, None, // No right row anymore
						source,
					);
					output_diffs.push(diff);
				}
			} else if matches!(self.join_type, JoinType::Inner) {
				// Either side delete for INNER JOIN - remove
				// the joined rows
				for other_row in &other_rows {
					// We need to generate a removal for the
					// joined result
					// Convert to owned/static columns
					let mut column_vec = Vec::new();
					for column in before.iter() {
						let static_col = Column {
							name: column.name.clone().to_static(),
							data: column.data.clone(),
						};
						column_vec.push(static_col);
					}
					let columns = Columns::new(column_vec);

					output_diffs.push(FlowDiff::Remove {
						source,
						rows: CowVec::new(vec![other_row.row_id]),
						pre: columns,
					});
				}
			}
		}

		Ok(output_diffs)
	}
}

impl<T: Transaction> Operator<T> for JoinOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> Result<FlowChange> {
		let mut output_diffs = Vec::new();

		// Process each diff in the change
		for diff in change.diffs {
			// Process the diff based on its type
			match diff {
				FlowDiff::Insert {
					source,
					rows: row_ids,
					post: after,
				} => {
					let (metadata, is_left) = self.get_or_init_metadata(
						txn,
						&after,
						&self.left_schema,
						&self.right_schema,
					)?;

					// Update metadata if this is the right source
					if !is_left && metadata.right_source.is_empty() {
						self.update_metadata(txn, metadata.clone(), &after)?;
					}

					let diffs = self.process_insert(
						txn, evaluator, source, &row_ids, &after, is_left, &metadata,
					)?;
					output_diffs.extend(diffs);
				}
				FlowDiff::Update {
					source,
					rows: row_ids,
					pre: before,
					post: after,
				} => {
					// Get metadata from after columns
					let (metadata, is_left) = self.get_or_init_metadata(
						txn,
						&after,
						&self.left_schema,
						&self.right_schema,
					)?;

					// Update metadata if this is the right source
					if !is_left && metadata.right_source.is_empty() {
						self.update_metadata(txn, metadata.clone(), &after)?;
					}

					// Handle update as remove + insert
					let remove_diffs = self
						.process_remove(txn, evaluator, source, &row_ids, &before, is_left)?;
					let insert_diffs = self.process_insert(
						txn, evaluator, source, &row_ids, &after, is_left, &metadata,
					)?;
					output_diffs.extend(remove_diffs);
					output_diffs.extend(insert_diffs);
				}
				FlowDiff::Remove {
					source,
					rows: row_ids,
					pre: before,
				} => {
					let (metadata, is_left) = self.get_or_init_metadata(
						txn,
						&before,
						&self.left_schema,
						&self.right_schema,
					)?;

					let diffs = self
						.process_remove(txn, evaluator, source, &row_ids, &before, is_left)?;
					output_diffs.extend(diffs);
				}
			}
		}

		Ok(FlowChange {
			diffs: output_diffs,
		})
	}
}

// Helper function to convert Value to ColumnData
fn value_to_column_data(value: &Value) -> ColumnData {
	match value {
		Value::Undefined => ColumnData::undefined(1),
		Value::Boolean(v) => ColumnData::bool(vec![*v]),
		Value::Uint1(v) => ColumnData::uint1(vec![*v]),
		Value::Uint2(v) => ColumnData::uint2(vec![*v]),
		Value::Uint4(v) => ColumnData::uint4(vec![*v]),
		Value::Uint8(v) => ColumnData::uint8(vec![*v]),
		Value::Uint16(v) => ColumnData::uint16(vec![*v]),
		Value::Int1(v) => ColumnData::int1(vec![*v]),
		Value::Int2(v) => ColumnData::int2(vec![*v]),
		Value::Int4(v) => ColumnData::int4(vec![*v]),
		Value::Int8(v) => ColumnData::int8(vec![*v]),
		Value::Int16(v) => ColumnData::int16(vec![*v]),
		Value::Float4(v) => ColumnData::float4(vec![v.value()]),
		Value::Float8(v) => ColumnData::float8(vec![v.value()]),
		Value::Utf8(v) => ColumnData::utf8(vec![v.clone()]),
		_ => unimplemented!(),
	}
}

impl<T: Transaction> TransformOperator<T> for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}

impl<T: Transaction> RawStatefulOperator<T> for JoinOperator {}
