use std::sync::Arc;

use bincode::{config::standard, serde::encode_to_vec};
use indexmap::IndexMap;
use reifydb_core::{
	EncodedKey, Error, JoinType, Row, interface::FlowNodeId, log_trace, util::encoding::keycode::KeySerializer,
	value::encoded::EncodedValuesLayout,
};
use reifydb_engine::{RowEvaluationContext, StandardRowEvaluator, execute::Executor};
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::expression::Expression;
use reifydb_type::{Params, RowNumber, Type, Value, internal};

use super::{JoinSide, JoinState, JoinStrategy, layout::JoinedLayoutBuilder};
use crate::{
	operator::{
		Operator, Operators,
		stateful::{RawStatefulOperator, RowNumberProvider, SingleStateful},
		transform::TransformOperator,
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

	/// Generate a row for an unmatched left join result
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
		serializer.extend_u8(b'L'); // 'L' prefix for left row
		serializer.extend_u64(left.number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique row number for this unmatched row
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

	/// Clean up all join results for a given left row
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

		// Build the combined layout and get a stable row number
		let builder = JoinedLayoutBuilder::new(left, right, &self.alias);
		let composite_key = Self::make_composite_key(left.number, right.number);
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		let result = builder.build_row(result_row_number, left, right);

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

	/// Create a composite key for a join result from left and right row numbers.
	fn make_composite_key(left_num: RowNumber, right_num: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_num.0);
		serializer.extend_u64(right_num.0);
		EncodedKey::new(serializer.finish())
	}

	/// Decode a u64 from keycode format (big-endian with bits flipped).
	/// The keycode format inverts all bits for proper byte-order sorting.
	fn decode_row_number_from_keycode(bytes: &[u8]) -> u64 {
		let arr: [u8; 8] =
			[!bytes[0], !bytes[1], !bytes[2], !bytes[3], !bytes[4], !bytes[5], !bytes[6], !bytes[7]];
		u64::from_be_bytes(arr)
	}

	/// Parse a composite key to extract left and optional right row numbers.
	/// Returns None if the key format is invalid.
	/// Key format: '!L' (1 byte inverted) + left_row_number (8 bytes) + optional right_row_number (8 bytes)
	fn parse_composite_key(key_bytes: &[u8]) -> Option<(RowNumber, Option<RowNumber>)> {
		// Check minimum length and 'L' prefix (inverted in keycode format)
		if key_bytes.len() < 9 || key_bytes[0] != !b'L' {
			return None;
		}

		let left_num = Self::decode_row_number_from_keycode(&key_bytes[1..9]);
		let right_num = if key_bytes.len() >= 17 {
			Some(RowNumber(Self::decode_row_number_from_keycode(&key_bytes[9..17])))
		} else {
			None
		};

		Some((RowNumber(left_num), right_num))
	}

	/// Batch version of join_rows - joins one left row with multiple right rows efficiently.
	/// Uses get_or_create_row_numbers_batch to minimize state operations.
	pub(crate) fn join_rows_batch(
		&self,
		txn: &mut FlowTransaction,
		left: &Row,
		right_rows: &[Row],
	) -> crate::Result<Vec<Row>> {
		if right_rows.is_empty() {
			return Ok(Vec::new());
		}

		// Build the combined layout once (same for all results)
		let builder = JoinedLayoutBuilder::new(left, &right_rows[0], &self.alias);

		// Build all composite keys upfront
		let composite_keys: Vec<EncodedKey> =
			right_rows.iter().map(|right| Self::make_composite_key(left.number, right.number)).collect();

		// Batch call to get_or_create_row_numbers
		let row_numbers =
			self.row_number_provider.get_or_create_row_numbers_batch(txn, composite_keys.iter())?;

		// Build all result rows
		let results = right_rows
			.iter()
			.zip(row_numbers.iter())
			.map(|(right, (row_number, _))| builder.build_row(*row_number, left, right))
			.collect();

		Ok(results)
	}

	/// Batch version that joins multiple left rows with one right row.
	pub(crate) fn join_rows_batch_right(
		&self,
		txn: &mut FlowTransaction,
		left_rows: &[Row],
		right: &Row,
	) -> crate::Result<Vec<Row>> {
		if left_rows.is_empty() {
			return Ok(Vec::new());
		}

		// Build the combined layout once
		let builder = JoinedLayoutBuilder::new(&left_rows[0], right, &self.alias);

		// Build all composite keys upfront
		let composite_keys: Vec<EncodedKey> =
			left_rows.iter().map(|left| Self::make_composite_key(left.number, right.number)).collect();

		// Batch call to get_or_create_row_numbers
		let row_numbers =
			self.row_number_provider.get_or_create_row_numbers_batch(txn, composite_keys.iter())?;

		// Build all result rows
		let results = left_rows
			.iter()
			.zip(row_numbers.iter())
			.map(|(left, (row_number, _))| builder.build_row(*row_number, left, right))
			.collect();

		Ok(results)
	}

	/// Full batch version that joins multiple left rows with multiple right rows (cartesian product).
	/// Uses get_or_create_row_numbers_batch to minimize state operations.
	pub(crate) fn join_rows_batch_full(
		&self,
		txn: &mut FlowTransaction,
		left_rows: &[Row],
		right_rows: &[Row],
	) -> crate::Result<Vec<Row>> {
		if left_rows.is_empty() || right_rows.is_empty() {
			return Ok(Vec::new());
		}

		// Build the combined layout once
		let builder = JoinedLayoutBuilder::new(&left_rows[0], &right_rows[0], &self.alias);

		// Build all composite keys and pairs for cartesian product
		let total_results = left_rows.len() * right_rows.len();
		let mut composite_keys = Vec::with_capacity(total_results);
		let mut pairs = Vec::with_capacity(total_results);

		for left in left_rows {
			for right in right_rows {
				composite_keys.push(Self::make_composite_key(left.number, right.number));
				pairs.push((left, right));
			}
		}

		// Single batch call to get_or_create_row_numbers for all results
		let row_numbers =
			self.row_number_provider.get_or_create_row_numbers_batch(txn, composite_keys.iter())?;

		// Build all result rows
		let results = pairs
			.into_iter()
			.zip(row_numbers.iter())
			.map(|((left, right), (row_number, _))| builder.build_row(*row_number, left, right))
			.collect();

		Ok(results)
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

		// Group diffs by key_hash for batch processing
		// We use IndexMap to preserve insertion order while still batching
		let mut inserts_by_key: IndexMap<Hash128, Vec<Row>> = IndexMap::new();
		let mut removes_by_key: IndexMap<Hash128, Vec<Row>> = IndexMap::new();
		let mut inserts_undefined: Vec<Row> = Vec::new();
		let mut removes_undefined: Vec<Row> = Vec::new();
		// Updates are processed individually due to key change complexity
		let mut updates: Vec<(Row, Row, Option<Hash128>, Option<Hash128>)> = Vec::new();

		// Phase 1: Compute keys and group diffs
		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let key = match side {
						JoinSide::Left => {
							self.compute_join_key(&post, &self.left_exprs, evaluator)?
						}
						JoinSide::Right => {
							self.compute_join_key(&post, &self.right_exprs, evaluator)?
						}
					};
					if let Some(key_hash) = key {
						inserts_by_key.entry(key_hash).or_default().push(post);
					} else {
						inserts_undefined.push(post);
					}
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
					if let Some(key_hash) = key {
						removes_by_key.entry(key_hash).or_default().push(pre);
					} else {
						removes_undefined.push(pre);
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
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
					updates.push((pre, post, old_key, new_key));
				}
			}
		}

		// Phase 2: Process batched inserts
		for (key_hash, rows) in inserts_by_key {
			log_trace!("[JOIN] apply: batch insert {} rows for key={:?}", rows.len(), key_hash);
			let diffs = self.strategy.handle_insert_batch(txn, &rows, side, &key_hash, &mut state, self)?;
			log_trace!("[JOIN] apply: batch insert produced {} diffs", diffs.len());
			result.extend(diffs);
		}

		// Process inserts with undefined keys individually
		for post in inserts_undefined {
			let diffs = self.strategy.handle_insert(txn, &post, side, None, &mut state, self)?;
			result.extend(diffs);
		}

		// Phase 3: Process batched removes
		for (key_hash, rows) in removes_by_key {
			log_trace!("[JOIN] apply: batch remove {} rows for key={:?}", rows.len(), key_hash);
			let diffs = self.strategy.handle_remove_batch(
				txn,
				&rows,
				side,
				&key_hash,
				&mut state,
				self,
				change.version,
			)?;
			log_trace!("[JOIN] apply: batch remove produced {} diffs", diffs.len());
			result.extend(diffs);
		}

		// Process removes with undefined keys individually
		for pre in removes_undefined {
			let diffs =
				self.strategy.handle_remove(txn, &pre, side, None, &mut state, self, change.version)?;
			result.extend(diffs);
		}

		// Phase 4: Process updates individually (key change complexity requires this)
		for (pre, post, old_key, new_key) in updates {
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
			let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number)? else {
				log_trace!("[JOIN] get_rows: no key found for row_number={}", row_number.0);
				result.push(None);
				continue;
			};

			// Parse the composite key to extract left and optional right row numbers
			let Some((left_row_number, right_row_number)) = Self::parse_composite_key(key.as_ref()) else {
				log_trace!("[JOIN] get_rows: invalid key format for row_number={}", row_number.0);
				result.push(None);
				continue;
			};

			// Get left row from parent
			let left_rows = self.left_parent.get_rows(txn, &[left_row_number])?;
			let Some(Some(left_row)) = left_rows.into_iter().next() else {
				log_trace!("[JOIN] get_rows: left row not found for row_number={}", row_number.0);
				result.push(None);
				continue;
			};

			if let Some(right_row_num) = right_row_number {
				// Matched join - has right row number
				let right_rows = self.right_parent.get_rows(txn, &[right_row_num])?;
				if let Some(Some(right_row)) = right_rows.into_iter().next() {
					// Reconstruct the joined row
					let joined = self.join_rows(txn, &left_row, &right_row)?;
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
		}

		Ok(result)
	}
}
