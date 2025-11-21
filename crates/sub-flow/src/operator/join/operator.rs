use std::{collections::HashSet, sync::Arc};

use bincode::{config::standard, serde::encode_to_vec};
use reifydb_core::{
	interface::FlowNodeId, log_trace, util::encoding::keycode::KeySerializer, value::encoded::{EncodedValuesLayout, EncodedValuesNamedLayout},
	EncodedKey,
	Error,
	JoinType,
	Row,
};
use reifydb_engine::{execute::Executor, RowEvaluationContext, StandardRowEvaluator};
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_hash::{xxh3_128, Hash128};
use reifydb_rql::expression::Expression;
use reifydb_type::{internal, Params, RowNumber, Type, Value};

use super::{JoinSide, JoinState, JoinStrategy};
use crate::{
	operator::{
		stateful::{RawStatefulOperator, RowNumberProvider, SingleStateful}, transform::TransformOperator,
		Operator,
		Operators,
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;

pub struct JoinOperator {
	pub(crate) left_parent: Arc<Operators>,
	pub(crate) right_parent: Arc<Operators>,
	node: FlowNodeId,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression<'static>>,
	pub(crate) right_exprs: Vec<Expression<'static>>,
	alias: Option<String>,
	layout: EncodedValuesLayout,
	row_number_provider: RowNumberProvider,
	executor: Executor,
}

impl JoinOperator {
	pub fn new(
		left_parent: Arc<Operators>,
		right_parent: Arc<Operators>,
		node: FlowNodeId,
		join_type: JoinType,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
		left_exprs: Vec<Expression<'static>>,
		right_exprs: Vec<Expression<'static>>,
		alias: Option<String>,
		executor: Executor,
	) -> Self {
		let strategy = JoinStrategy::from(join_type);
		let layout = Self::state_layout();
		let row_number_provider = RowNumberProvider::new(node);

		Self {
			left_parent,
			right_parent,
			node,
			strategy,
			left_node,
			right_node,
			left_exprs,
			right_exprs,
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
		log_trace!(
			"[JOIN] compute_join_key: row_number={}, layout_names={:?}",
			row.number.0,
			row.layout.names()
		);

		// Pre-allocate with reasonable capacity
		let mut hasher = Vec::with_capacity(256);
		for (expr_idx, expr) in exprs.iter().enumerate() {
			// For AccessSource expressions, extract just the column name and evaluate that
			let value = match expr {
				Expression::AccessSource(access_source) => {
					// Get the column name without the source
					let col_name = access_source.column.name.as_ref();

					// Use the new name-based API to get the value
					let val = row
						.layout
						.get_value(&row.encoded, col_name)
						.unwrap_or(Value::Undefined);
					log_trace!(
						"[JOIN] compute_join_key: expr[{}] AccessSource col='{}' -> value={:?}",
						expr_idx,
						col_name,
						val
					);
					val
				}
				_ => {
					// For other expressions, use the evaluator
					// TODO: Investigate if we can avoid cloning the encoded here
					let ctx = RowEvaluationContext {
						row: row.clone(),
						target: None,
						params: &EMPTY_PARAMS,
					};
					let val = evaluator.evaluate(&ctx, expr)?;
					log_trace!(
						"[JOIN] compute_join_key: expr[{}] evaluated -> value={:?}",
						expr_idx,
						val
					);
					val
				}
			};

			// Check if the value is undefined - undefined values should never match in joins
			if matches!(value, Value::Undefined) {
				log_trace!(
					"[JOIN] compute_join_key: returning None due to Undefined value at expr[{}]",
					expr_idx
				);
				return Ok(None);
			}

			let bytes = encode_to_vec(&value, standard())
				.map_err(|e| Error(internal!("Failed to encode value for hash: {}", e)))?;

			hasher.extend_from_slice(&bytes);
		}

		let hash = xxh3_128(&hasher);
		log_trace!("[JOIN] compute_join_key: hash={:?}", hash);
		Ok(Some(hash))
	}

	/// Generate a encoded number for an unmatched left join encoded
	pub(crate) fn unmatched_left_row(&self, txn: &mut FlowTransaction, left: &Row) -> crate::Result<Row> {
		log_trace!(
			"[JOIN] unmatched_left_row: input row_number={}, layout_names={:?}",
			left.number.0,
			left.layout.names()
		);

		// Log all values in the input row
		for (idx, name) in left.layout.names().iter().enumerate() {
			let value = left.layout.get_value_by_idx(&left.encoded, idx);
			log_trace!("[JOIN] unmatched_left_row: input col[{}] '{}' = {:?}", idx, name, value);
		}

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left encoded
		serializer.extend_u64(left.number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique encoded number for this unmatched encoded
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		let result = Row {
			number: result_row_number,
			encoded: left.encoded.clone(),
			layout: left.layout.clone(),
		};

		log_trace!(
			"[JOIN] unmatched_left_row: output row_number={}, layout_names={:?}",
			result.number.0,
			result.layout.names()
		);

		Ok(result)
	}

	/// Clean up all join results for a given left encoded
	/// This removes both matched and unmatched join results
	pub(crate) fn cleanup_left_row_joins(&self, txn: &mut FlowTransaction, left_number: u64) -> crate::Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		// Remove all mappings with this prefix
		self.row_number_provider.remove_by_prefix(txn, &prefix)
	}

	pub(crate) fn join_rows(&self, txn: &mut FlowTransaction, left: &Row, right: &Row) -> crate::Result<Row> {
		log_trace!("[JOIN] join_rows: left row_number={}, right row_number={}", left.number.0, right.number.0);
		log_trace!("[JOIN] join_rows: left layout_names={:?}", left.layout.names());
		log_trace!("[JOIN] join_rows: right layout_names={:?}", right.layout.names());

		// Log left row values
		for (idx, name) in left.layout.names().iter().enumerate() {
			let value = left.layout.get_value_by_idx(&left.encoded, idx);
			log_trace!("[JOIN] join_rows: left col[{}] '{}' = {:?}", idx, name, value);
		}

		// Log right row values
		for (idx, name) in right.layout.names().iter().enumerate() {
			let value = right.layout.get_value_by_idx(&right.encoded, idx);
			log_trace!("[JOIN] join_rows: right col[{}] '{}' = {:?}", idx, name, value);
		}

		// Combine the two rows into a single encoded
		// Prefix column names with alias to handle naming conflicts

		// Pre-calculate total capacity to avoid reallocations
		let total_fields = left.layout.fields().fields.len() + right.layout.fields().fields.len();
		let mut combined_values = Vec::with_capacity(total_fields);
		let mut combined_names = Vec::with_capacity(total_fields);
		let mut combined_types = Vec::with_capacity(total_fields);

		// Add left side columns - never prefixed
		let left_names = left.layout.names();
		for i in 0..left.layout.fields().fields.len() {
			let value = left.layout.get_value_by_idx(&left.encoded, i);
			combined_values.push(value);
			if i < left_names.len() {
				combined_names.push(left_names[i].clone());
			}
			combined_types.push(left.layout.fields().fields[i].r#type);
		}

		// Collect left names into a set for conflict detection
		// Use HashSet<&str> to avoid cloning strings
		let left_name_set: HashSet<&str> = left_names.iter().map(|s| s.as_str()).collect();

		// Add right side columns - prefix with alias when there's a conflict
		let right_names = right.layout.names();
		for i in 0..right.layout.fields().fields.len() {
			let value = right.layout.get_value_by_idx(&right.encoded, i);
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
			combined_types.push(right.layout.fields().fields[i].r#type);
		}

		// Create combined layout
		let fields: Vec<(String, Type)> = combined_names.into_iter().zip(combined_types.into_iter()).collect();
		let layout = EncodedValuesNamedLayout::new(fields);

		// Allocate and populate the new encoded
		let mut encoded_row = layout.allocate();
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
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		let result = Row {
			number: result_row_number,
			encoded: encoded_row,
			layout,
		};

		log_trace!(
			"[JOIN] join_rows: output row_number={}, layout_names={:?}",
			result.number.0,
			result.layout.names()
		);

		// Log output row values
		for (idx, name) in result.layout.names().iter().enumerate() {
			let value = result.layout.get_value_by_idx(&result.encoded, idx);
			log_trace!("[JOIN] join_rows: output col[{}] '{}' = {:?}", idx, name, value);
		}

		Ok(result)
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

impl TransformOperator for JoinOperator {}

impl RawStatefulOperator for JoinOperator {}

impl SingleStateful for JoinOperator {
	fn layout(&self) -> EncodedValuesLayout {
		self.layout.clone()
	}
}

impl Operator for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		log_trace!(
			"[JOIN] apply: node={:?}, change.origin={:?}, diffs_count={}",
			self.node,
			change.origin,
			change.diffs.len()
		);

		// Check for self-referential calls (should never happen)
		if let FlowChangeOrigin::Internal(from_node) = &change.origin {
			if *from_node == self.node {
				log_trace!("[JOIN] apply: self-referential call, returning empty");
				return Ok(FlowChange::internal(self.node, change.version, Vec::new()));
			}
		}

		// Create the state
		let mut state = JoinState::new(self.node);
		// Pre-allocate result vector with estimated capacity
		let estimated_capacity = change.diffs.len() * 2; // Rough estimate
		let mut result = Vec::with_capacity(estimated_capacity);

		// Determine which side this change is from
		let side = self
			.determine_side(&change)
			.ok_or_else(|| Error(internal!("Join operator received change from unknown node")))?;

		log_trace!("[JOIN] apply: side={:?}", side);

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					log_trace!(
						"[JOIN] apply: Insert row_number={}, layout={:?}",
						post.number.0,
						post.layout.names()
					);

					// Compute join key based on side
					let key = match side {
						JoinSide::Left => {
							self.compute_join_key(&post, &self.left_exprs, evaluator)?
						}
						JoinSide::Right => {
							self.compute_join_key(&post, &self.right_exprs, evaluator)?
						}
					};

					log_trace!("[JOIN] apply: Insert computed key={:?}", key);

					let diffs =
						self.strategy.handle_insert(txn, &post, side, key, &mut state, self)?;
					log_trace!("[JOIN] apply: Insert produced {} diffs", diffs.len());
					result.extend(diffs);
				}
				FlowDiff::Remove {
					pre,
				} => {
					log_trace!(
						"[JOIN] apply: Remove row_number={}, layout={:?}",
						pre.number.0,
						pre.layout.names()
					);

					let key = match side {
						JoinSide::Left => {
							self.compute_join_key(&pre, &self.left_exprs, evaluator)?
						}
						JoinSide::Right => {
							self.compute_join_key(&pre, &self.right_exprs, evaluator)?
						}
					};

					log_trace!("[JOIN] apply: Remove computed key={:?}", key);

					let diffs = self.strategy.handle_remove(
						txn,
						&pre,
						side,
						key,
						&mut state,
						self,
						change.version,
					)?;
					log_trace!("[JOIN] apply: Remove produced {} diffs", diffs.len());
					result.extend(diffs);
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					log_trace!(
						"[JOIN] apply: Update pre_row={}, post_row={}, layout={:?}",
						pre.number.0,
						post.number.0,
						post.layout.names()
					);

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

					log_trace!("[JOIN] apply: Update old_key={:?}, new_key={:?}", old_key, new_key);

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
					log_trace!("[JOIN] apply: Update produced {} diffs", diffs.len());
					result.extend(diffs);
				}
			}
		}

		log_trace!("[JOIN] apply: total result diffs={}", result.len());
		Ok(FlowChange::internal(self.node, change.version, result))
	}

	// FIXME #244 The issue is that when we need to reconstruct an unmatched left row, we need the right side's
	// schema to create the combined layout To make that work it requires schema / layout information of the right
	// side this should unlock the test:
	// testsuite/flow/tests/scripts/backfill/18_multiple_joins_same_table.skip
	// testsuite/flow/tests/scripts/backfill/19_complex_multi_table.skip
	// testsuite/flow/tests/scripts/backfill/21_backfill_with_distinct.skip
	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		log_trace!(
			"[JOIN] get_rows called on node={:?}, requested rows={:?}, left_node={:?}, right_node={:?}",
			self.node,
			rows,
			self.left_node,
			self.right_node
		);

		let mut result = Vec::with_capacity(rows.len());

		for &row_number in rows {
			// Get the composite key for this row number (reverse lookup)
			if let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number)? {
				// Decode left and right row numbers from composite key
				// Format: 'L' (1 byte) + left_row_number (8 bytes) + optional right_row_number (8
				// bytes)
				let key_bytes = key.as_ref();

				// Note: 'L' is encoded with keycode's extend_u8 which inverts the byte
				if key_bytes.len() >= 9 && key_bytes[0] == !b'L' {
					// Decode u64 from keycode format (big-endian with bits flipped)
					let left_bytes: [u8; 8] = [
						!key_bytes[1],
						!key_bytes[2],
						!key_bytes[3],
						!key_bytes[4],
						!key_bytes[5],
						!key_bytes[6],
						!key_bytes[7],
						!key_bytes[8],
					];
					let left_num = u64::from_be_bytes(left_bytes);
					let left_row_number = RowNumber(left_num);

					// Get left row from parent
					let left_rows = self.left_parent.get_rows(txn, &[left_row_number])?;

					if let Some(Some(left_row)) = left_rows.into_iter().next() {
						if key_bytes.len() >= 17 {
							// Matched join - has right row number
							let right_bytes: [u8; 8] = [
								!key_bytes[9],
								!key_bytes[10],
								!key_bytes[11],
								!key_bytes[12],
								!key_bytes[13],
								!key_bytes[14],
								!key_bytes[15],
								!key_bytes[16],
							];
							let right_num = u64::from_be_bytes(right_bytes);
							let right_row_number = RowNumber(right_num);

							// Get right row from parent
							let right_rows =
								self.right_parent.get_rows(txn, &[right_row_number])?;

							if let Some(Some(right_row)) = right_rows.into_iter().next() {
								// Reconstruct the joined row
								let joined =
									self.join_rows(txn, &left_row, &right_row)?;
								result.push(Some(Row {
									number: row_number,
									encoded: joined.encoded,
									layout: joined.layout,
								}));
							} else {
								log_trace!(
									"[JOIN] get_rows: right row not found for row_number={}",
									row_number.0
								);
								result.push(None);
							}
						} else {
							// Unmatched left row
							let unmatched = self.unmatched_left_row(txn, &left_row)?;
							result.push(Some(Row {
								number: row_number,
								encoded: unmatched.encoded,
								layout: unmatched.layout,
							}));
						}
					} else {
						log_trace!(
							"[JOIN] get_rows: left row not found for row_number={}",
							row_number.0
						);
						result.push(None);
					}
				} else {
					log_trace!(
						"[JOIN] get_rows: invalid key format for row_number={}",
						row_number.0
					);
					result.push(None);
				}
			} else {
				log_trace!("[JOIN] get_rows: no key found for row_number={}", row_number.0);
				result.push(None);
			}
		}

		Ok(result)
	}
}
