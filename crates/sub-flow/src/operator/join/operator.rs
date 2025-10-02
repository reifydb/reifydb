use std::collections::HashSet;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	EncodedKey, Error, JoinType, Row,
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	util::encoding::keycode::KeySerializer,
	value::encoded::{EncodedValuesLayout, EncodedValuesNamedLayout},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator, execute::Executor};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::query::QueryString;
use reifydb_type::{Blob, Params, Type, Value, internal_error};

use super::{JoinSide, JoinState, JoinStrategy, Schema};
use crate::{
	flow::{FlowChange, FlowChangeOrigin, FlowDiff},
	operator::{
		Operator,
		stateful::{RawStatefulOperator, RowNumberProvider, SingleStateful, state_get, state_set},
		transform::TransformOperator,
	},
};

static EMPTY_PARAMS: Params = Params::None;

pub struct JoinOperator {
	node: FlowNodeId,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression<'static>>,
	pub(crate) right_exprs: Vec<Expression<'static>>,
	right_query: QueryString,
	alias: Option<String>,
	layout: EncodedValuesLayout,
	row_number_provider: RowNumberProvider,
	executor: Executor,
}

impl JoinOperator {
	pub fn new(
		node: FlowNodeId,
		join_type: JoinType,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
		left_exprs: Vec<Expression<'static>>,
		right_exprs: Vec<Expression<'static>>,
		right_query: QueryString,
		alias: Option<String>,
		storage_strategy: reifydb_core::JoinStrategy,
		executor: Executor,
	) -> Self {
		let strategy = JoinStrategy::from(storage_strategy, join_type, right_query.clone(), executor.clone());
		let layout = Self::state_layout();
		let row_number_provider = RowNumberProvider::new(node);

		Self {
			node,
			strategy,
			left_node,
			right_node,
			left_exprs,
			right_exprs,
			right_query,
			alias,
			layout,
			row_number_provider,
			executor,
		}
	}

	fn state_layout() -> EncodedValuesLayout {
		EncodedValuesLayout::new(&[Type::Blob])
	}

	pub(crate) fn compute_join_key(
		&self,
		row: &Row,
		exprs: &[Expression<'static>],
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<Hash128>> {
		// Pre-allocate with reasonable capacity
		let mut hasher = Vec::with_capacity(256);
		for expr in exprs.iter() {
			// For AccessSource expressions, extract just the column name and evaluate that
			let value = match expr {
				Expression::AccessSource(access_source) => {
					// Get the column name without the source
					let col_name = access_source.column.name.as_ref();

					// Find the column in the encoded by name
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
					// TODO: Investigate if we can avoid cloning the encoded here
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

	/// Generate a encoded number for an unmatched left join encoded
	pub(crate) fn unmatched_left_row<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		left: &Row,
	) -> crate::Result<Row> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left encoded
		serializer.extend_u64(left.number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique encoded number for this unmatched encoded
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, self, &composite_key)?;

		Ok(Row {
			number: result_row_number,
			encoded: left.encoded.clone(),
			layout: left.layout.clone(),
		})
	}

	/// Clean up all join results for a given left encoded
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
		// Combine the two rows into a single encoded
		// Prefix column names with alias to handle naming conflicts

		// Pre-calculate total capacity to avoid reallocations
		let total_fields = left.layout.fields.len() + right.layout.fields.len();
		let mut combined_values = Vec::with_capacity(total_fields);
		let mut combined_names = Vec::with_capacity(total_fields);
		let mut combined_types = Vec::with_capacity(total_fields);

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
		// Use HashSet<&str> to avoid cloning strings
		let left_name_set: HashSet<&str> = left_names.iter().map(|s| s.as_str()).collect();

		// Add right side columns - prefix with alias when there's a conflict
		let right_names = right.layout.names();
		for i in 0..right.layout.fields.len() {
			let value = right.layout.get_value(&right.encoded, i);
			combined_values.push(value);
			if i < right_names.len() {
				let col_name = &right_names[i];
				// Check if there's a naming conflict with left side
				let final_name = if left_name_set.contains(col_name.as_str()) {
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
		let layout = EncodedValuesNamedLayout::new(fields);

		// Allocate and populate the new encoded
		let mut encoded_row = layout.allocate_row();
		layout.set_values(&mut encoded_row, &combined_values);

		// Use RowNumberProvider to get a stable encoded number for this join result
		// Create a composite key from left and right encoded numbers
		// Structure: 'L' + left_number + 'R' + right_number for efficient prefix scans
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left encoded
		serializer.extend_u64(left.number.0);
		serializer.extend_u64(right.number.0);

		// Get or create a unique encoded number for this join result
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
				// Deserialize Schema from the encoded
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
	fn layout(&self) -> EncodedValuesLayout {
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
				return Ok(FlowChange::internal(self.node, change.version, Vec::new()));
			}
		}

		// Load the schema and create the state
		let schema = self.load_schema(txn)?;
		let mut state = JoinState::new(self.node, schema);
		// Pre-allocate result vector with estimated capacity
		let estimated_capacity = change.diffs.len() * 2; // Rough estimate
		let mut result = Vec::with_capacity(estimated_capacity);

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

					let diffs = self.strategy.handle_insert(
						txn,
						&post,
						side,
						key,
						&mut state,
						self,
						change.version,
					)?;
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
					let diffs = self.strategy.handle_remove(
						txn,
						&pre,
						side,
						key,
						&mut state,
						self,
						change.version,
					)?;
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
						txn,
						&pre,
						&post,
						side,
						old_key,
						new_key,
						&mut state,
						self,
						change.version,
					)?;
					result.extend(diffs);
				}
			}
		}

		// Save the updated schema
		self.save_schema(txn, &state.schema)?;

		Ok(FlowChange::internal(self.node, change.version, result))
	}
}
