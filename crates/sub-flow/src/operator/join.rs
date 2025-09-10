// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	JoinType,
	flow::{FlowChange, FlowDiff, FlowNodeSchema},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		SourceId,
		evaluate::expression::{ColumnExpression, Expression},
	},
	row::{EncodedKey, EncodedKeyRange, EncodedRow},
	util::CowVec,
	value::columnar::{
		Column, ColumnData, Columns, FullyQualified, SourceQualified,
	},
};
use reifydb_type::{RowNumber, Value};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing join state
#[derive(Debug, Clone)]
struct FlowJoinStateKey {
	flow_id: u64,
	node_id: u64,
	join_instance: u64,
	side: u8, // 0 = left, 1 = right
	join_key_hash: u64,
	row_id: RowNumber,
}

impl FlowJoinStateKey {
	const KEY_PREFIX: u8 = 0xF1;

	fn new(
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
	) -> Self {
		Self {
			flow_id,
			node_id,
			join_instance,
			side,
			join_key_hash,
			row_id,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&self.join_instance.to_be_bytes());
		key.push(self.side);
		key.extend(&self.join_key_hash.to_be_bytes());
		key.extend(&self.row_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_ref();
		if bytes.len() < 34 || bytes[0] != Self::KEY_PREFIX {
			return None;
		}

		let flow_id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
		let node_id = u64::from_be_bytes(bytes[9..17].try_into().ok()?);
		let join_instance =
			u64::from_be_bytes(bytes[17..25].try_into().ok()?);
		let side = bytes[25];
		let join_key_hash =
			u64::from_be_bytes(bytes[26..34].try_into().ok()?);
		let row_id = if bytes.len() >= 42 {
			RowNumber(u64::from_be_bytes(
				bytes[34..42].try_into().ok()?,
			))
		} else {
			RowNumber(0)
		};

		Some(Self {
			flow_id,
			node_id,
			join_instance,
			side,
			join_key_hash,
			row_id,
		})
	}

	fn range_for_join_key(
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		side: u8,
		join_key_hash: u64,
	) -> EncodedKeyRange {
		let mut start = Vec::new();
		start.push(Self::KEY_PREFIX);
		start.extend(&flow_id.to_be_bytes());
		start.extend(&node_id.to_be_bytes());
		start.extend(&join_instance.to_be_bytes());
		start.push(side);
		start.extend(&join_key_hash.to_be_bytes());

		let mut end = start.clone();
		end.extend(&u64::MAX.to_be_bytes());

		EncodedKeyRange::new(
			Bound::Included(EncodedKey(CowVec::new(start))),
			Bound::Included(EncodedKey(CowVec::new(end))),
		)
	}
}

// Stored row data for join state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRow {
	row_id: RowNumber,
	source_name: String,
	columns: HashMap<String, Value>,
}

// Metadata key for storing source information
#[derive(Debug, Clone)]
struct JoinMetadataKey {
	flow_id: u64,
	node_id: u64,
	join_instance: u64,
}

impl JoinMetadataKey {
	const KEY_PREFIX: u8 = 0xF2;

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&self.join_instance.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
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
	join_type: JoinType,
	left_keys: Vec<Expression<'static>>,
	right_keys: Vec<Expression<'static>>,
	left_schema: FlowNodeSchema,
	right_schema: FlowNodeSchema,
	flow_id: u64,
	node_id: u64,
	join_instance_id: u64,
	// Track which source corresponds to which side for repeated tables
	// This should be populated when multiple joins use the same table
	left_source_instance_suffix: Option<String>,
	right_source_instance_suffix: Option<String>,
}

impl JoinOperator {
	pub fn new(
		join_type: JoinType,
		left_keys: Vec<Expression<'static>>,
		right_keys: Vec<Expression<'static>>,
		left_schema: FlowNodeSchema,
		right_schema: FlowNodeSchema,
	) -> Self {
		// These will be set dynamically when we have more context
		// For now using placeholder values
		Self {
			join_type,
			left_keys,
			right_keys,
			left_schema,
			right_schema,
			flow_id: 1,
			node_id: 1,
			join_instance_id: 1,
			left_source_instance_suffix: None,
			right_source_instance_suffix: None,
		}
	}

	pub fn with_instance_id(mut self, instance_id: u64) -> Self {
		self.join_instance_id = instance_id;
		// Also use instance_id as node_id if not already set
		if self.node_id == 1 {
			self.node_id = instance_id;
		}
		self
	}

	pub fn with_flow_id(mut self, flow_id: u64) -> Self {
		self.flow_id = flow_id;
		self
	}

	pub fn with_source_suffixes(
		mut self,
		left_suffix: Option<String>,
		right_suffix: Option<String>,
	) -> Self {
		self.left_source_instance_suffix = left_suffix;
		self.right_source_instance_suffix = right_suffix;
		self
	}

	// Extract join key values from columns
	fn extract_join_keys<E: Evaluator, T: CommandTransaction>(
		ctx: &OperatorContext<E, T>,
		columns: &Columns,
		row_idx: usize,
		expressions: &[Expression<'static>],
	) -> Result<Vec<Value>> {
		let mut keys = Vec::new();
		let row_count = columns.row_count();
		let empty_params = Params::None;

		let eval_ctx = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
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
					Expression::Column(ColumnExpression(
						access.column.clone(),
					))
				}
				_ => expr.clone(),
			};

			let result = ctx.evaluate(&eval_ctx, &column_expr)?;
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
	fn store_row<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
		columns: &Columns,
		row_idx: usize,
	) -> Result<()> {
		// Extract source name from the first column (all columns should
		// have same source)
		let source_name = if let Some(first_col) = columns.first() {
			match first_col {
				Column::FullyQualified(fq) => fq.source.clone(),
				Column::SourceQualified(sq) => {
					sq.source.clone()
				}
				_ => "unknown".to_string(),
			}
		} else {
			"unknown".to_string()
		};

		// Check if we already have this row stored
		let key = FlowJoinStateKey::new(
			flow_id,
			node_id,
			join_instance,
			side,
			join_key_hash,
			row_id,
		);

		// Get existing row data if it exists
		let mut column_values = if let Ok(Some(existing)) =
			txn.get(&key.encode())
		{
			// Deserialize existing row and start with its columns
			if let Ok(existing_row) = serde_json::from_slice::<
				StoredRow,
			>(&existing.row.0)
			{
				existing_row.columns
			} else {
				HashMap::new()
			}
		} else {
			HashMap::new()
		};

		// Add/update with new column values
		for column in columns.iter() {
			let name = column.name().to_string();
			let value = column.data().get_value(row_idx);
			column_values.insert(name, value);
		}

		let stored_row = StoredRow {
			row_id,
			source_name,
			columns: column_values,
		};

		let serialized =
			serde_json::to_vec(&stored_row).unwrap_or_default();
		txn.set(&key.encode(), EncodedRow(CowVec::new(serialized)))?;
		Ok(())
	}

	// Retrieve matching rows from the other side
	fn get_matching_rows<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		side: u8,
		join_key_hash: u64,
	) -> Result<Vec<StoredRow>> {
		let mut rows = Vec::new();
		let range = FlowJoinStateKey::range_for_join_key(
			flow_id,
			node_id,
			join_instance,
			side,
			join_key_hash,
		);

		// Scan the range for matching rows
		if let Ok(iter) = txn.range(range) {
			for versioned in iter {
				if let Some(state_key) =
					FlowJoinStateKey::decode(&versioned.key)
				{
					if state_key.join_key_hash
						== join_key_hash
					{
						if let Ok(stored_row) =
							serde_json::from_slice::<
								StoredRow,
							>(
								versioned
									.row
									.as_ref(
									),
							) {
							rows.push(stored_row);
						}
					}
				}
			}
		}

		Ok(rows)
	}

	// Delete a row from the join state
	fn delete_row<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
	) -> Result<()> {
		let key = FlowJoinStateKey::new(
			flow_id,
			node_id,
			join_instance,
			side,
			join_key_hash,
			row_id,
		);
		txn.remove(&key.encode())?;
		Ok(())
	}

	// Get or initialize metadata
	// Returns (metadata, is_left_side)
	fn get_or_init_metadata_with_node<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		columns: &Columns,
		left_schema: &FlowNodeSchema,
		right_schema: &FlowNodeSchema,
		from_node_id: u64,
	) -> Result<(JoinMetadata, bool)> {
		let key = JoinMetadataKey {
			flow_id,
			node_id,
			join_instance,
		};

		// Try to get existing metadata
		if let Ok(Some(data)) = txn.get(&key.encode()) {
			if let Ok(mut metadata) = serde_json::from_slice::<
				JoinMetadata,
			>(data.row.as_ref())
			{
				// Check if we need to track this node
				let node_key = format!("node_{}", from_node_id);

				// Determine if this is left or right based on
				// tracked nodes
				let is_left = if let Some(side) = metadata
					.initialized_nodes
					.get(&node_key)
				{
					*side == "left"
				} else {
					// First time seeing this node, need to
					// determine which side it is If we
					// already have a left node tracked,
					// this must be right
					let has_left = metadata
						.initialized_nodes
						.values()
						.any(|v| v == "left");
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

					// Update metadata with source
					// information from columns if needed
					if is_left
						&& metadata
							.left_source
							.is_empty()
					{
						if let Some(first_col) =
							columns.first()
						{
							metadata.left_source = match first_col {
								Column::FullyQualified(fq) => fq.source.clone(),
								Column::SourceQualified(sq) => sq.source.clone(),
								_ => "unknown".to_string(),
							};
						}
					} else if !is_left
						&& metadata
							.right_source
							.is_empty()
					{
						if let Some(first_col) =
							columns.first()
						{
							metadata.right_source = match first_col {
								Column::FullyQualified(fq) => fq.source.clone(),
								Column::SourceQualified(sq) => sq.source.clone(),
								_ => "unknown".to_string(),
							};
						}
					}

					// Save updated metadata
					let data =
						serde_json::to_vec(&metadata)
							.unwrap_or_default();
					txn.set(
						&key.encode(),
						EncodedRow(CowVec::new(data)),
					)?;

					is_left
				};

				return Ok((metadata, is_left));
			}
		}

		// Initialize new metadata - this is the first node we're seeing
		let source_name = if let Some(first_col) = columns.first() {
			match first_col {
				Column::FullyQualified(fq) => fq.source.clone(),
				Column::SourceQualified(sq) => {
					sq.source.clone()
				}
				_ => "unknown".to_string(),
			}
		} else {
			"unknown".to_string()
		};

		// Determine if this node should be left or right based on
		// schemas Check if the source matches the right schema
		let is_right_source = right_schema
			.source_name
			.as_ref()
			.map(|s| s == &source_name)
			.unwrap_or(false);

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
				join_instance_id: join_instance,
				left_source: source_name.clone(),
				right_source: right_schema
					.source_name
					.clone()
					.unwrap_or_default(),
				initialized: false,
				left_columns: columns
					.iter()
					.map(|c| c.name().to_string())
					.collect(),
				right_columns: right_schema
					.columns
					.iter()
					.map(|c| c.name.clone())
					.collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes,
			}
		} else {
			JoinMetadata {
				join_instance_id: join_instance,
				left_source: left_schema
					.source_name
					.clone()
					.unwrap_or_default(),
				right_source: source_name.clone(),
				initialized: false,
				left_columns: left_schema
					.columns
					.iter()
					.map(|c| c.name.clone())
					.collect(),
				right_columns: columns
					.iter()
					.map(|c| c.name().to_string())
					.collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes,
			}
		};

		// Store the metadata
		let data = serde_json::to_vec(&metadata).unwrap_or_default();
		txn.set(&key.encode(), EncodedRow(CowVec::new(data)))?;

		Ok((metadata, is_left))
	}

	fn get_or_init_metadata<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		columns: &Columns,
		left_schema: &FlowNodeSchema,
		right_schema: &FlowNodeSchema,
	) -> Result<(JoinMetadata, bool)> {
		let key = JoinMetadataKey {
			flow_id,
			node_id,
			join_instance,
		};

		// Extract source name from columns
		let source_name = if let Some(first_col) = columns.first() {
			match first_col {
				Column::FullyQualified(fq) => fq.source.clone(),
				Column::SourceQualified(sq) => {
					sq.source.clone()
				}
				_ => "unknown".to_string(),
			}
		} else {
			"unknown".to_string()
		};

		// Try to get existing metadata
		if let Ok(Some(data)) = txn.get(&key.encode()) {
			if let Ok(metadata) = serde_json::from_slice::<
				JoinMetadata,
			>(data.row.as_ref())
			{
				// Determine if this is the left or right source
				let is_left =
					source_name == metadata.left_source;
				return Ok((metadata, is_left));
			}
		}

		// Initialize metadata - determine if this is left or right
		// For the first pass, we'll still use source name matching
		// but track instance IDs for disambiguation
		// TODO: This should be passed through flow metadata

		let is_left_by_schema = left_schema
			.source_name
			.as_ref()
			.map(|s| s == &source_name)
			.unwrap_or(false);

		let metadata = if is_left_by_schema {
			// This is the left source
			JoinMetadata {
				join_instance_id: join_instance,
				left_source: source_name.clone(),
				right_source: right_schema
					.source_name
					.clone()
					.unwrap_or_default(),
				initialized: false,
				left_columns: columns
					.iter()
					.map(|c| c.name().to_string())
					.collect(),
				right_columns: right_schema
					.columns
					.iter()
					.map(|c| c.name.clone())
					.collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes:
					std::collections::HashMap::new(),
			}
		} else {
			// This is the right source
			JoinMetadata {
				join_instance_id: join_instance,
				left_source: left_schema
					.source_name
					.clone()
					.unwrap_or_default(),
				right_source: source_name.clone(),
				initialized: false,
				left_columns: left_schema
					.columns
					.iter()
					.map(|c| c.name.clone())
					.collect(),
				right_columns: columns
					.iter()
					.map(|c| c.name().to_string())
					.collect(),
				left_instance_suffix: None,
				right_instance_suffix: None,
				initialized_nodes:
					std::collections::HashMap::new(),
			}
		};

		let serialized =
			serde_json::to_vec(&metadata).unwrap_or_default();
		txn.set(&key.encode(), EncodedRow(CowVec::new(serialized)))?;

		Ok((metadata, is_left_by_schema))
	}

	// Update metadata with right source
	fn update_metadata<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		join_instance: u64,
		mut metadata: JoinMetadata,
		columns: &Columns,
	) -> Result<()> {
		if metadata.right_source.is_empty() {
			// Extract source name from columns
			let source_name =
				if let Some(first_col) = columns.first() {
					match first_col {
						Column::FullyQualified(fq) => {
							fq.source.clone()
						}
						Column::SourceQualified(sq) => {
							sq.source.clone()
						}
						_ => "unknown".to_string(),
					}
				} else {
					"unknown".to_string()
				};

			metadata.right_source = source_name;
			metadata.right_columns = columns
				.iter()
				.map(|c| c.name().to_string())
				.collect();
			metadata.initialized = true;

			let key = JoinMetadataKey {
				flow_id,
				node_id,
				join_instance,
			};
			let serialized = serde_json::to_vec(&metadata)
				.unwrap_or_default();
			txn.set(
				&key.encode(),
				EncodedRow(CowVec::new(serialized)),
			)?;
		}
		Ok(())
	}

	// Combine left and right rows into output columns
	fn combine_rows(
		&self,
		left_row: &StoredRow,
		right_row: Option<&StoredRow>,
		source_id: SourceId,
	) -> FlowDiff {
		let mut column_vec = Vec::new();
		let row_ids = vec![left_row.row_id];

		// Add left columns with full qualification from schema
		// We need to output ALL columns from the left schema, not just
		// what's stored
		for column_def in &self.left_schema.columns {
			// Check if we have this column value in the stored row
			let data = if let Some(value) =
				left_row.columns.get(&column_def.name)
			{
				value_to_column_data(value)
			} else {
				// Column not in stored data - this shouldn't
				// happen for left side but we'll handle
				// gracefully with undefined
				ColumnData::undefined(1)
			};

			if let (Some(schema), Some(source)) = (
				&self.left_schema.schema_name,
				&self.left_schema.source_name,
			) {
				// Create fully qualified columns
				column_vec.push(Column::FullyQualified(
					FullyQualified {
						schema: schema.clone(),
						source: source.clone(),
						name: column_def.name.clone(),
						data,
					},
				));
			} else if let Some(source) =
				&self.left_schema.source_name
			{
				// Fallback to source qualified
				column_vec.push(Column::SourceQualified(
					SourceQualified {
						source: source.clone(),
						name: column_def.name.clone(),
						data,
					},
				));
			} else {
				// Fallback to unqualified (shouldn't happen)
				column_vec.push(Column::SourceQualified(
					SourceQualified {
						source: "unknown".to_string(),
						name: column_def.name.clone(),
						data,
					},
				));
			}
		}

		// Add right columns or NULLs for LEFT JOIN
		if let Some(right) = right_row {
			// We have a matching right row - output all right
			// columns
			for column_def in &self.right_schema.columns {
				// Check if we have this column value in the
				// stored row
				let data = if let Some(value) =
					right.columns.get(&column_def.name)
				{
					value_to_column_data(value)
				} else {
					// Column not in stored data - use
					// undefined
					ColumnData::undefined(1)
				};

				if let (Some(schema), Some(source)) = (
					&self.right_schema.schema_name,
					&self.right_schema.source_name,
				) {
					// Create fully qualified columns
					column_vec
						.push(Column::FullyQualified(
						FullyQualified {
							schema: schema.clone(),
							source: source.clone(),
							name: column_def
								.name
								.clone(),
							data,
						},
					));
				} else if let Some(source) =
					&self.right_schema.source_name
				{
					// Fallback to source qualified
					column_vec
						.push(Column::SourceQualified(
						SourceQualified {
							source: source.clone(),
							name: column_def
								.name
								.clone(),
							data,
						},
					));
				} else {
					// Fallback to unqualified (shouldn't
					// happen)
					column_vec
						.push(Column::SourceQualified(
						SourceQualified {
							source: "unknown"
								.to_string(),
							name: column_def
								.name
								.clone(),
							data,
						},
					));
				}
			}
		} else {
			// For LEFT JOIN with no match, add NULL values for
			// right columns using schema
			for column_def in &self.right_schema.columns {
				if let (Some(schema), Some(source)) = (
					&self.right_schema.schema_name,
					&self.right_schema.source_name,
				) {
					// Create fully qualified columns with
					// undefined data
					column_vec.push(Column::FullyQualified(
						FullyQualified {
							schema: schema.clone(),
							source: source.clone(),
							name: column_def.name.clone(),
							data: ColumnData::undefined(1),
						},
					));
				} else if let Some(source) =
					&self.right_schema.source_name
				{
					// Fallback to source qualified
					column_vec.push(Column::SourceQualified(
						SourceQualified {
							source: source.clone(),
							name: column_def.name.clone(),
							data: ColumnData::undefined(1),
						},
					));
				} else {
					// Fallback to unqualified (shouldn't
					// happen)
					column_vec.push(Column::SourceQualified(
						SourceQualified {
							source: "unknown".to_string(),
							name: column_def.name.clone(),
							data: ColumnData::undefined(1),
						},
					));
				}
			}
		}

		let columns = Columns::new(column_vec);

		FlowDiff::Insert {
			source: source_id,
			row_ids,
			after: columns,
		}
	}

	// Process an insert operation
	fn process_insert<E: Evaluator, T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
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

		// Extract source name from columns for current row
		let source_name = if let Some(first_col) = after.first() {
			match first_col {
				Column::FullyQualified(fq) => fq.source.clone(),
				Column::SourceQualified(sq) => {
					sq.source.clone()
				}
				_ => {
					if is_left {
						metadata.left_source.clone()
					} else {
						metadata.right_source.clone()
					}
				}
			}
		} else {
			if is_left {
				metadata.left_source.clone()
			} else {
				metadata.right_source.clone()
			}
		};

		for (idx, &row_id) in row_ids.iter().enumerate() {
			// Extract join keys
			let join_keys = Self::extract_join_keys(
				ctx,
				after,
				idx,
				expressions,
			)?;
			let join_key_hash = Self::hash_join_keys(&join_keys);

			// Store this row
			Self::store_row(
				ctx.txn,
				self.flow_id,
				self.node_id,
				self.join_instance_id,
				this_side,
				join_key_hash,
				row_id,
				after,
				idx,
			)?;

			// Get matching rows from the other side
			let other_rows = Self::get_matching_rows(
				ctx.txn,
				self.flow_id,
				self.node_id,
				self.join_instance_id,
				other_side,
				join_key_hash,
			)?;

			// Create a StoredRow for the current row
			let mut current_columns = HashMap::new();
			for column in after.iter() {
				let name = column.name().to_string();
				let value = column.data().get_value(idx);
				current_columns.insert(name, value);
			}
			let current_row = StoredRow {
				row_id,
				source_name: source_name.clone(),
				columns: current_columns,
			};

			// For LEFT JOIN:
			// - If left side changes, emit the join result
			// - If right side changes AND there are matching left
			//   rows, emit updates
			// For INNER JOIN: emit for both sides
			if is_left {
				// Left side insert for LEFT or INNER join
				if other_rows.is_empty() {
					// LEFT JOIN with no match - emit with
					// NULLs for right side
					let diff = self.combine_rows(
						&current_row,
						None,
						source,
					);
					output_diffs.push(diff);
				} else {
					// Emit joined rows for each match
					for other_row in &other_rows {
						let diff = self.combine_rows(
							&current_row,
							Some(other_row),
							source,
						);
						output_diffs.push(diff);
					}
				}
			} else if matches!(self.join_type, JoinType::Inner) {
				// Right side insert for INNER join
				for other_row in &other_rows {
					let diff = self.combine_rows(
						other_row,
						Some(&current_row),
						source,
					);
					output_diffs.push(diff);
				}
			} else if matches!(self.join_type, JoinType::Left)
				&& !other_rows.is_empty()
			{
				// Right side insert for LEFT join - emit
				// updates for matching left rows
				// This is important! When a customer is added
				// that matches existing orders, we need to
				// emit those updated join results
				for other_row in &other_rows {
					let diff = self.combine_rows(
						other_row,
						Some(&current_row),
						source,
					);
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
	fn process_remove<E: Evaluator, T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
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
			let join_keys = Self::extract_join_keys(
				ctx,
				before,
				idx,
				expressions,
			)?;
			let join_key_hash = Self::hash_join_keys(&join_keys);

			// Get matching rows from the other side before deleting
			let other_rows = Self::get_matching_rows(
				ctx.txn,
				self.flow_id,
				self.node_id,
				self.join_instance_id,
				other_side,
				join_key_hash,
			)?;

			// Delete this row from state
			Self::delete_row(
				ctx.txn,
				self.flow_id,
				self.node_id,
				self.join_instance_id,
				this_side,
				join_key_hash,
				row_id,
			)?;

			// Generate removal diffs
			if is_left {
				// Left side delete for LEFT JOIN
				let mut column_vec = Vec::new();
				for column in before.iter() {
					column_vec.push(column.clone());
				}
				let columns = Columns::new(column_vec);

				output_diffs.push(FlowDiff::Remove {
					source,
					row_ids: vec![row_id],
					before: columns,
				});
			} else if matches!(self.join_type, JoinType::Left) {
				// Right side delete for LEFT JOIN
				// Emit updates for affected left rows showing
				// NULL for right columns
				for other_row in &other_rows {
					let diff = self.combine_rows(
						other_row,
						None, // No right row anymore
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
					let mut column_vec = Vec::new();
					for column in before.iter() {
						column_vec.push(column.clone());
					}
					let columns = Columns::new(column_vec);

					output_diffs.push(FlowDiff::Remove {
						source,
						row_ids: vec![other_row.row_id],
						before: columns,
					});
				}
			}
		}

		Ok(output_diffs)
	}
}

impl<E: Evaluator> Operator<E> for JoinOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> Result<FlowChange> {
		use reifydb_core::log_debug;

		// Check which node this data is coming from
		let from_node = change.metadata.get("from_node").and_then(
			|v| match v {
				reifydb_type::Value::Uint8(n) => Some(*n),
				_ => None,
			},
		);

		log_debug!(
			"JoinOperator: Instance {:?} processing {} diffs",
			self.join_instance_id,
			change.diffs.len()
		);
		let mut output_diffs = Vec::new();

		// Process each diff in the change
		for diff in &change.diffs {
			let source = match diff {
				FlowDiff::Insert {
					source,
					..
				}
				| FlowDiff::Update {
					source,
					..
				}
				| FlowDiff::Remove {
					source,
					..
				} => *source,
			};

			// Get columns from diff
			let columns = match diff {
				FlowDiff::Insert {
					after,
					..
				} => after,
				FlowDiff::Update {
					after,
					..
				} => after,
				FlowDiff::Remove {
					before,
					..
				} => before,
			};

			// Get or initialize metadata to determine left/right
			// Use from_node if available to determine which input
			// this is
			let (metadata, is_left) =
				if let Some(from_node_id) = from_node {
					Self::get_or_init_metadata_with_node(
						ctx.txn,
						self.flow_id,
						self.node_id,
						self.join_instance_id,
						columns,
						&self.left_schema,
						&self.right_schema,
						from_node_id,
					)?
				} else {
					Self::get_or_init_metadata(
						ctx.txn,
						self.flow_id,
						self.node_id,
						self.join_instance_id,
						columns,
						&self.left_schema,
						&self.right_schema,
					)?
				};

			// Update metadata if this is the right source
			if !is_left && metadata.right_source.is_empty() {
				Self::update_metadata(
					ctx.txn,
					self.flow_id,
					self.node_id,
					self.join_instance_id,
					metadata.clone(),
					columns,
				)?;
			}

			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					after,
				} => {
					let diffs = self.process_insert(
						ctx, *source, row_ids, after,
						is_left, &metadata,
					)?;
					output_diffs.extend(diffs);
				}
				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					// Handle update as remove + insert
					let remove_diffs = self
						.process_remove(
							ctx, *source, row_ids,
							before, is_left,
						)?;
					let insert_diffs = self
						.process_insert(
							ctx, *source, row_ids,
							after, is_left,
							&metadata,
						)?;
					output_diffs.extend(remove_diffs);
					output_diffs.extend(insert_diffs);
				}
				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					let diffs = self.process_remove(
						ctx, *source, row_ids, before,
						is_left,
					)?;
					output_diffs.extend(diffs);
				}
			}
		}

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
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
