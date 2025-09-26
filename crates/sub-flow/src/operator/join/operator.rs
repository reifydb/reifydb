use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	EncodedKey, Error, JoinType,
	flow::{FlowChange, FlowChangeOrigin, FlowDiff},
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	util::encoding::keycode::KeySerializer,
	value::row::{EncodedRowLayout, EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Blob, Params, Type, Value, internal_error};

use super::{JoinSide, JoinState, JoinStrategy, Schema};
use crate::operator::{
	Operator,
	stateful::{RawStatefulOperator, RowNumberProvider, SingleStateful, state_get, state_set},
	transform::TransformOperator,
};

static EMPTY_PARAMS: Params = Params::None;

pub struct JoinOperator {
	node: FlowNodeId,
	join_type: JoinType,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression<'static>>,
	right_exprs: Vec<Expression<'static>>,
	alias: Option<String>,
	layout: EncodedRowLayout,
	row_number_provider: RowNumberProvider,
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
		let strategy = JoinStrategy::from_join_type(join_type);
		let layout = Self::state_layout();
		let row_number_provider = RowNumberProvider::new(node);

		Self {
			node,
			join_type,
			strategy,
			left_node,
			right_node,
			left_exprs,
			right_exprs,
			alias,
			layout,
			row_number_provider,
		}
	}

	fn state_layout() -> EncodedRowLayout {
		EncodedRowLayout::new(&[Type::Blob])
	}

	pub(crate) fn compute_join_key(
		&self,
		row: &Row,
		exprs: &[Expression<'static>],
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<Hash128>> {
		let mut hasher = Vec::new();
		for expr in exprs.iter() {
			// For AccessSource expressions, extract just the column name and evaluate that
			let value = match expr {
				Expression::AccessSource(access_source) => {
					// Get the column name without the source
					let col_name = access_source.column.name.as_ref();

					// Find the column in the row by name
					let names = row.layout.names();
					let col_index = names.iter().position(|n| n == col_name);

					if let Some(idx) = col_index {
						row.layout.get_value(&row.encoded, idx)
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
					evaluator.evaluate(&ctx, expr)?
				}
			};

			// Check if the value is undefined - undefined values should never match in joins
			if matches!(value, Value::Undefined) {
				return Ok(None);
			}

			let bytes = encode_to_vec(&value, standard())
				.map_err(|e| Error(internal_error!("Failed to encode value for hash: {}", e)))?;

			hasher.extend_from_slice(&bytes);
		}

		let hash = xxh3_128(&hasher);
		Ok(Some(hash))
	}

	/// Generate a row number for an unmatched left join row
	pub(crate) fn unmatched_left_row<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		left: &Row,
	) -> crate::Result<Row> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left row
		serializer.extend_u64(left.number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique row number for this unmatched row
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, self, &composite_key)?;

		Ok(Row {
			number: result_row_number,
			encoded: left.encoded.clone(),
			layout: left.layout.clone(),
		})
	}

	/// Clean up all join results for a given left row
	/// This removes both matched and unmatched join results
	pub(crate) fn cleanup_left_row_joins<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		left_number: u64,
	) -> crate::Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		// Remove all mappings with this prefix
		self.row_number_provider.remove_by_prefix(txn, self, &prefix)
	}

	pub(crate) fn join_rows<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		left: &Row,
		right: &Row,
	) -> crate::Result<Row> {
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

		// Use RowNumberProvider to get a stable row number for this join result
		// Create a composite key from left and right row numbers
		// Structure: 'L' + left_number + 'R' + right_number for efficient prefix scans
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left row
		serializer.extend_u64(left.number.0);
		serializer.extend_u64(right.number.0);

		// Get or create a unique row number for this join result
		let composite_key = EncodedKey::new(serializer.finish());
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, self, &composite_key)?;

		Ok(Row {
			number: result_row_number,
			encoded: encoded_row,
			layout,
		})
	}

	fn load_schema<T: Transaction>(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<Schema> {
		// Load schema from a special key (empty key)
		let schema_key = EncodedKey::new(vec![0x00]); // Special key for schema
		match state_get(self.node, txn, &schema_key)? {
			Some(row) => {
				// Deserialize Schema from the row
				let blob = self.layout.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(Schema::new());
				}
				let config = standard();
				let (schema, _): (Schema, usize) = decode_from_slice(blob.as_ref(), config)
					.map_err(|e| Error(internal_error!("Failed to deserialize Schema: {}", e)))?;
				Ok(schema)
			}
			None => Ok(Schema::new()),
		}
	}

	fn save_schema<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		schema: &Schema,
	) -> crate::Result<()> {
		// Save schema to a special key (empty key)
		let schema_key = EncodedKey::new(vec![0x00]); // Special key for schema

		let config = standard();
		let serialized = encode_to_vec(schema, config)
			.map_err(|e| Error(internal_error!("Failed to serialize Schema: {}", e)))?;

		// Store as a blob in an EncodedRow
		let mut row = self.layout.allocate_row();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut row, 0, &blob);

		state_set(self.node, txn, &schema_key, row)?;
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

		// Load the schema and create the state
		let schema = self.load_schema(txn)?;
		let mut state = JoinState::new(self.node, schema);
		let mut result = Vec::new();

		// Determine which side this change is from
		let side = self
			.determine_side(&change)
			.ok_or_else(|| Error(internal_error!("Join operator received change from unknown node")))?;

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Update schema based on side
					match side {
						JoinSide::Left => state.schema.update_left_from_row(&post),
						JoinSide::Right => state.schema.update_right_from_row(&post),
					}

					// Compute join key based on side
					let key = match side {
						JoinSide::Left => {
							self.compute_join_key(&post, &self.left_exprs, evaluator)?
						}
						JoinSide::Right => {
							self.compute_join_key(&post, &self.right_exprs, evaluator)?
						}
					};

					let diffs =
						self.strategy.handle_insert(txn, &post, side, key, &mut state, self)?;
					result.extend(diffs);
				}
				FlowDiff::Remove {
					pre,
				} => {
					let key = match side {
						JoinSide::Left => {
							self.compute_join_key(&pre, &self.left_exprs, evaluator)?
						}
						JoinSide::Right => {
							self.compute_join_key(&pre, &self.right_exprs, evaluator)?
						}
					};
					let diffs =
						self.strategy.handle_remove(txn, &pre, side, key, &mut state, self)?;
					result.extend(diffs);
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Update schema if needed
					match side {
						JoinSide::Left => state.schema.update_left_from_row(&post),
						JoinSide::Right => state.schema.update_right_from_row(&post),
					}

					let (old_key, new_key) = match side {
						JoinSide::Left => (
							self.compute_join_key(&pre, &self.left_exprs, evaluator)?,
							self.compute_join_key(&post, &self.left_exprs, evaluator)?,
						),
						JoinSide::Right => (
							self.compute_join_key(&pre, &self.right_exprs, evaluator)?,
							self.compute_join_key(&post, &self.right_exprs, evaluator)?,
						),
					};
					let diffs = self.strategy.handle_update(
						txn, &pre, &post, side, old_key, new_key, &mut state, self,
					)?;
					result.extend(diffs);
				}
			}
		}

		// Save the updated schema
		self.save_schema(txn, &state.schema)?;

		Ok(FlowChange::internal(self.node, result))
	}
}
