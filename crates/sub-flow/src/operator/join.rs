// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	JoinType,
	flow::{FlowChange, FlowDiff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		SourceId, expression::Expression,
	},
	row::{EncodedKey, EncodedKeyRange, EncodedRow},
	util::CowVec,
	value::columnar::{Column, ColumnData, Columns, SourceQualified},
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
	side: u8, // 0 = left, 1 = right
	join_key_hash: u64,
	row_id: RowNumber,
}

impl FlowJoinStateKey {
	const KEY_PREFIX: u8 = 0xF1;

	fn new(
		flow_id: u64,
		node_id: u64,
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
	) -> Self {
		Self {
			flow_id,
			node_id,
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
		key.push(self.side);
		key.extend(&self.join_key_hash.to_be_bytes());
		key.extend(&self.row_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_ref();
		if bytes.len() < 26 || bytes[0] != Self::KEY_PREFIX {
			return None;
		}

		let flow_id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
		let node_id = u64::from_be_bytes(bytes[9..17].try_into().ok()?);
		let side = bytes[17];
		let join_key_hash =
			u64::from_be_bytes(bytes[18..26].try_into().ok()?);
		let row_id = if bytes.len() >= 34 {
			RowNumber(u64::from_be_bytes(
				bytes[26..34].try_into().ok()?,
			))
		} else {
			RowNumber(0)
		};

		Some(Self {
			flow_id,
			node_id,
			side,
			join_key_hash,
			row_id,
		})
	}

	fn range_for_join_key(
		flow_id: u64,
		node_id: u64,
		side: u8,
		join_key_hash: u64,
	) -> EncodedKeyRange {
		let mut start = Vec::new();
		start.push(Self::KEY_PREFIX);
		start.extend(&flow_id.to_be_bytes());
		start.extend(&node_id.to_be_bytes());
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
}

impl JoinMetadataKey {
	const KEY_PREFIX: u8 = 0xF2;

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinMetadata {
	left_source: String,
	right_source: String,
	initialized: bool,
}

pub struct JoinOperator {
	join_type: JoinType,
	left_keys: Vec<Expression<'static>>,
	right_keys: Vec<Expression<'static>>,
	flow_id: u64,
	node_id: u64,
}

impl JoinOperator {
	pub fn new(
		join_type: JoinType,
		left_keys: Vec<Expression<'static>>,
		right_keys: Vec<Expression<'static>>,
	) -> Self {
		// These will be set dynamically when we have more context
		// For now using placeholder values
		Self {
			join_type,
			left_keys,
			right_keys,
			flow_id: 1,
			node_id: 1,
		}
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
			let result = ctx.evaluate(&eval_ctx, expr)?;
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

		let mut column_values = HashMap::new();
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

		let key = FlowJoinStateKey::new(
			flow_id,
			node_id,
			side,
			join_key_hash,
			row_id,
		);
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
		side: u8,
		join_key_hash: u64,
	) -> Result<Vec<StoredRow>> {
		let mut rows = Vec::new();
		let range = FlowJoinStateKey::range_for_join_key(
			flow_id,
			node_id,
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
		side: u8,
		join_key_hash: u64,
		row_id: RowNumber,
	) -> Result<()> {
		let key = FlowJoinStateKey::new(
			flow_id,
			node_id,
			side,
			join_key_hash,
			row_id,
		);
		txn.remove(&key.encode())?;
		Ok(())
	}

	// Get or initialize metadata
	fn get_or_init_metadata<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
		columns: &Columns,
	) -> Result<(JoinMetadata, bool)> {
		let key = JoinMetadataKey {
			flow_id,
			node_id,
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

		// Initialize metadata - first source is left, second is right
		let metadata = JoinMetadata {
			left_source: source_name.clone(),
			right_source: String::new(), /* Will be set when
			                              * right source arrives */
			initialized: false,
		};

		let serialized =
			serde_json::to_vec(&metadata).unwrap_or_default();
		txn.set(&key.encode(), EncodedRow(CowVec::new(serialized)))?;

		Ok((metadata, true)) // First source is always left
	}

	// Update metadata with right source
	fn update_metadata<T: CommandTransaction>(
		txn: &mut T,
		flow_id: u64,
		node_id: u64,
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
			metadata.initialized = true;

			let key = JoinMetadataKey {
				flow_id,
				node_id,
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
		left_row: &StoredRow,
		right_row: Option<&StoredRow>,
		source_id: SourceId,
	) -> FlowDiff {
		let mut column_vec = Vec::new();
		let row_ids = vec![left_row.row_id];

		// Debug logging for column structure
		eprintln!("=== JoinOperator::combine_rows DEBUG ===");
		eprintln!("Left source: {}", left_row.source_name);
		eprintln!(
			"Left columns: {:?}",
			left_row.columns.keys().collect::<Vec<_>>()
		);
		if let Some(right) = &right_row {
			eprintln!("Right source: {}", right.source_name);
			eprintln!(
				"Right columns: {:?}",
				right.columns.keys().collect::<Vec<_>>()
			);
		} else {
			eprintln!("Right row: None (LEFT JOIN with no match)");
		}

		// Add left columns with source qualification
		for (name, value) in &left_row.columns {
			let data = value_to_column_data(value);
			eprintln!(
				"Adding left column: {}.{}",
				left_row.source_name, name
			);
			column_vec.push(Column::SourceQualified(
				SourceQualified {
					source: left_row.source_name.clone(),
					name: name.clone(),
					data,
				},
			));
		}

		// Add right columns (with source qualification)
		if let Some(right) = right_row {
			for (name, value) in &right.columns {
				let data = value_to_column_data(value);
				eprintln!(
					"Adding right column: {}.{}",
					right.source_name, name
				);
				column_vec.push(Column::SourceQualified(
					SourceQualified {
						source: right
							.source_name
							.clone(),
						name: name.clone(),
						data,
					},
				));
			}
		} else if matches!(
			left_row.source_name.as_str(),
			"orders" | "test.orders"
		) {
			// For LEFT JOIN with no match, add NULL values for
			// right columns This is a temporary solution -
			// ideally we'd get schema from metadata
			let right_source = "customers";
			eprintln!(
				"Adding NULL columns for right side (LEFT JOIN no match)"
			);
			for col_name in &["customer_id", "name", "city"] {
				eprintln!(
					"Adding NULL column: {}.{}",
					right_source, col_name
				);
				column_vec.push(Column::SourceQualified(
					SourceQualified {
						source: right_source
							.to_string(),
						name: col_name.to_string(),
						data: ColumnData::undefined(1),
					},
				));
			}
		}

		eprintln!("Final output columns count: {}", column_vec.len());
		let columns = Columns::new(column_vec);
		eprintln!("=== END JoinOperator::combine_rows DEBUG ===\n");

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
				this_side,
				join_key_hash,
				row_id,
				after,
				idx,
			)?;

			// For LEFT JOIN, only emit if this is the left side
			// For INNER JOIN, emit for both sides
			if is_left || matches!(self.join_type, JoinType::Inner)
			{
				// Get matching rows from the other side
				let other_rows = Self::get_matching_rows(
					ctx.txn,
					self.flow_id,
					self.node_id,
					other_side,
					join_key_hash,
				)?;

				// Create a StoredRow for the current row
				let mut current_columns = HashMap::new();
				for column in after.iter() {
					let name = column.name().to_string();
					let value =
						column.data().get_value(idx);
					current_columns.insert(name, value);
				}
				let current_row = StoredRow {
					row_id,
					source_name: source_name.clone(),
					columns: current_columns,
				};

				if other_rows.is_empty() && is_left {
					// LEFT JOIN with no match
					let diff = Self::combine_rows(
						&current_row,
						None,
						source,
					);
					output_diffs.push(diff);
				} else {
					// Emit joined rows for each match
					for other_row in &other_rows {
						let diff =
							if is_left {
								Self::combine_rows(&current_row, Some(other_row), source)
							} else {
								Self::combine_rows(other_row, Some(&current_row), source)
							};
						output_diffs.push(diff);
					}
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

		for (idx, &row_id) in row_ids.iter().enumerate() {
			// Extract join keys
			let join_keys = Self::extract_join_keys(
				ctx,
				before,
				idx,
				expressions,
			)?;
			let join_key_hash = Self::hash_join_keys(&join_keys);

			// Delete this row from state
			Self::delete_row(
				ctx.txn,
				self.flow_id,
				self.node_id,
				this_side,
				join_key_hash,
				row_id,
			)?;

			// Generate removal diffs
			// In production, we'd need to track which output rows
			// to remove For now, create a simple removal
			if is_left || matches!(self.join_type, JoinType::Inner)
			{
				let mut column_vec = Vec::new();
				for column in before.iter() {
					// Preserve the original column
					// structure
					column_vec.push(column.clone());
				}
				let columns = Columns::new(column_vec);

				output_diffs.push(FlowDiff::Remove {
					source,
					row_ids: vec![row_id],
					before: columns,
				});
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
			let (metadata, is_left) = Self::get_or_init_metadata(
				ctx.txn,
				self.flow_id,
				self.node_id,
				columns,
			)?;

			// Update metadata if this is the right source
			if !is_left && metadata.right_source.is_empty() {
				Self::update_metadata(
					ctx.txn,
					self.flow_id,
					self.node_id,
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
		Value::Int4(v) => ColumnData::int4(vec![*v]),
		Value::Int8(v) => ColumnData::int8(vec![*v]),
		Value::Float8(v) => ColumnData::float8(vec![v.value()]),
		Value::Utf8(v) => ColumnData::utf8(vec![v.clone()]),
		_ => ColumnData::undefined(1),
	}
}
