// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{rc::Rc, sync::LazyLock};

use indexmap::IndexMap;
use reifydb_core::{
	common::JoinType,
	encoded::{key::EncodedKey, layout::EncodedValuesLayout},
	interface::catalog::flow::FlowNodeId,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{Column, columns::Columns},
};
use reifydb_engine::{
	evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
	execute::Executor,
	stack::Stack,
};
use reifydb_hash::{Hash128, xxh::xxh3_128};
use reifydb_rql::expression::Expression;
use reifydb_sdk::flow::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	internal,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber, r#type::Type},
};

use super::{
	column::JoinedColumnsBuilder,
	state::{JoinSide, JoinState},
	strategy::JoinStrategy,
};
use crate::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

pub struct JoinOperator {
	pub(crate) left_parent: Rc<Operators>,
	pub(crate) right_parent: Rc<Operators>,
	node: FlowNodeId,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression>,
	pub(crate) right_exprs: Vec<Expression>,
	alias: Option<String>,
	layout: EncodedValuesLayout,
	row_number_provider: RowNumberProvider,
	executor: Executor,
	column_evaluator: StandardColumnEvaluator,
}

impl JoinOperator {
	pub fn new(
		left_parent: Rc<Operators>,
		right_parent: Rc<Operators>,
		node: FlowNodeId,
		join_type: JoinType,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
		left_exprs: Vec<Expression>,
		right_exprs: Vec<Expression>,
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
			column_evaluator: StandardColumnEvaluator::default(),
		}
	}

	fn state_layout() -> EncodedValuesLayout {
		EncodedValuesLayout::testing(&[Type::Blob])
	}

	/// Compute join keys for all rows in Columns
	/// Returns Vec<Option<Hash128>> - one per row, None for rows with undefined key values
	pub(crate) fn compute_join_keys(
		&self,
		columns: &Columns,
		exprs: &[Expression],
	) -> reifydb_type::Result<Vec<Option<Hash128>>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let ctx = ColumnEvaluationContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			stack: &EMPTY_STACK,
			is_aggregate_context: false,
		};

		// Evaluate all expressions on the entire batch
		// For AccessSource expressions, use direct column lookup (mimic old StandardRowEvaluator)
		let mut expr_columns = Vec::with_capacity(exprs.len());
		for expr in exprs.iter() {
			let col = match expr {
				Expression::AccessSource(access_source) => {
					// Direct column lookup by name - this is what StandardRowEvaluator did
					let col_name = access_source.column.name.as_ref();
					columns.column(col_name)
						.cloned()
						.unwrap_or_else(|| Column::undefined(col_name, row_count))
				}
				_ => self.column_evaluator.evaluate(&ctx, expr)?,
			};
			expr_columns.push(col);
		}

		// Compute hash for each row
		let mut hashes = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let mut hasher = Vec::with_capacity(256);
			let mut has_undefined = false;

			for col in &expr_columns {
				let value = col.data().get_value(row_idx);

				// Check if the value is undefined - undefined values should never match in joins
				if matches!(value, Value::Undefined) {
					has_undefined = true;
					break;
				}

				let bytes = postcard::to_stdvec(&value)
					.map_err(|e| Error(internal!("Failed to encode value for hash: {}", e)))?;
				hasher.extend_from_slice(&bytes);
			}

			if has_undefined {
				hashes.push(None);
			} else {
				hashes.push(Some(xxh3_128(&hasher)));
			}
		}

		Ok(hashes)
	}

	/// Generate columns for an unmatched left join result.
	/// Creates combined columns with left values and Undefined values for right columns.
	pub(crate) fn unmatched_left_columns(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
	) -> reifydb_type::Result<Columns> {
		let left_row_number = left.row_numbers[left_idx];

		// Create composite key for this unmatched row
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_row_number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique row number for this unmatched row
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		// Get the right side schema
		let right_schema = self.right_parent.pull(txn, &[])?;

		// Build using JoinedColumnsBuilder
		let builder = JoinedColumnsBuilder::new(left, &right_schema, &self.alias);
		Ok(builder.unmatched_left(result_row_number, left, left_idx, &right_schema))
	}

	/// Generate columns for multiple unmatched left join results.
	pub(crate) fn unmatched_left_columns_batch(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_indices: &[usize],
	) -> reifydb_type::Result<Columns> {
		if left_indices.is_empty() {
			return Ok(Columns::empty());
		}

		// Build composite keys for all unmatched rows
		let composite_keys: Vec<EncodedKey> = left_indices
			.iter()
			.map(|&idx| {
				let left_row_number = left.row_numbers[idx];
				let mut serializer = KeySerializer::new();
				serializer.extend_u8(b'L');
				serializer.extend_u64(left_row_number.0);
				EncodedKey::new(serializer.finish())
			})
			.collect();

		// Batch get/create row numbers
		let row_numbers_with_flags =
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		// Get the right side schema
		let right_schema = self.right_parent.pull(txn, &[])?;

		// Build using JoinedColumnsBuilder
		let builder = JoinedColumnsBuilder::new(left, &right_schema, &self.alias);
		Ok(builder.unmatched_left_batch(&row_numbers, left, left_indices, &right_schema))
	}

	/// Clean up all join results for a given left row
	/// This removes both matched and unmatched join results
	pub(crate) fn cleanup_left_row_joins(
		&self,
		txn: &mut FlowTransaction,
		left_number: u64,
	) -> reifydb_type::Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		// Remove all mappings with this prefix
		self.row_number_provider.remove_by_prefix(txn, &prefix)
	}

	/// Join a single left row with a single right row, returning combined Columns.
	pub(crate) fn join_columns(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		right_idx: usize,
	) -> reifydb_type::Result<Columns> {
		let left_row_number = left.row_numbers[left_idx];
		let right_row_number = right.row_numbers[right_idx];

		let composite_key = Self::make_composite_key(left_row_number, right_row_number);
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		// Join directly at indices without extracting rows
		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_at_indices(result_row_number, left, left_idx, right, right_idx))
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

	/// Join one left row with all right rows.
	/// Returns combined Columns with one row per right row.
	pub(crate) fn join_columns_one_to_many(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
	) -> reifydb_type::Result<Columns> {
		let right_count = right.row_count();
		if right_count == 0 {
			return Ok(Columns::empty());
		}

		let left_row_number = left.row_numbers[left_idx];

		// Build all composite keys
		let composite_keys: Vec<EncodedKey> = (0..right_count)
			.map(|right_idx| {
				let right_row_number = right.row_numbers[right_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		// Batch get/create row numbers
		let row_numbers_with_flags =
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_one_to_many(&row_numbers, left, left_idx, right))
	}

	/// Join all left rows with one right row.
	/// Returns combined Columns with one row per left row.
	pub(crate) fn join_columns_many_to_one(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		right: &Columns,
		right_idx: usize,
	) -> reifydb_type::Result<Columns> {
		let left_count = left.row_count();
		if left_count == 0 {
			return Ok(Columns::empty());
		}

		let right_row_number = right.row_numbers[right_idx];

		// Build all composite keys
		let composite_keys: Vec<EncodedKey> = (0..left_count)
			.map(|left_idx| {
				let left_row_number = left.row_numbers[left_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		// Batch get/create row numbers
		let row_numbers_with_flags =
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_many_to_one(&row_numbers, left, right, right_idx))
	}

	/// Join left rows at specified indices with all right rows (cartesian product).
	/// Returns combined Columns with left_indices.len() * right.row_count() rows.
	pub(crate) fn join_columns_cartesian(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
	) -> reifydb_type::Result<Columns> {
		let left_count = left_indices.len();
		let right_count = right.row_count();
		if left_count == 0 || right_count == 0 {
			return Ok(Columns::empty());
		}

		// Build all composite keys for cartesian product
		let total_results = left_count * right_count;
		let mut composite_keys = Vec::with_capacity(total_results);

		for &left_idx in left_indices {
			let left_row_number = left.row_numbers[left_idx];
			for right_idx in 0..right_count {
				let right_row_number = right.row_numbers[right_idx];
				composite_keys.push(Self::make_composite_key(left_row_number, right_row_number));
			}
		}

		// Batch get/create row numbers
		let row_numbers_with_flags =
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_cartesian(&row_numbers, left, left_indices, right))
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
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		// Check for self-referential calls (should never happen)
		if let FlowChangeOrigin::Internal(from_node) = &change.origin {
			if *from_node == self.node {
				return Ok(FlowChange::internal(self.node, change.version, Vec::new()));
			}
		}

		// Create the state
		let mut state = JoinState::new(self.node);
		// Pre-allocate result vector with estimated capacity
		let estimated_capacity = change.diffs.len() * 2;
		let mut result = Vec::with_capacity(estimated_capacity);

		// Determine which side this change is from
		let side = self
			.determine_side(&change)
			.ok_or_else(|| Error(internal!("Join operator received change from unknown node")))?;

		let exprs = match side {
			JoinSide::Left => &self.left_exprs,
			JoinSide::Right => &self.right_exprs,
		};

		// Process each diff inline, grouping by key within each diff
		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Compute keys for all rows in this Columns batch
					let keys = self.compute_join_keys(&post, exprs)?;
					let row_count = post.row_count();

					// Group indices by key hash
					let mut inserts_by_key: IndexMap<Hash128, Vec<usize>> = IndexMap::new();
					let mut inserts_undefined: Vec<usize> = Vec::new();

					for row_idx in 0..row_count {
						if let Some(key_hash) = keys[row_idx] {
							inserts_by_key.entry(key_hash).or_default().push(row_idx);
						} else {
							inserts_undefined.push(row_idx);
						}
					}

					// Process inserts with defined keys (batched by key)
					for (key_hash, indices) in inserts_by_key {
						let diffs = self.strategy.handle_insert(
							txn, &post, &indices, side, &key_hash, &mut state, self,
						)?;
						result.extend(diffs);
					}

					// Process inserts with undefined keys individually
					for idx in inserts_undefined {
						let diffs = self.strategy.handle_insert_undefined(
							txn, &post, idx, side, &mut state, self,
						)?;
						result.extend(diffs);
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Compute keys for all rows
					let keys = self.compute_join_keys(&pre, exprs)?;
					let row_count = pre.row_count();

					// Group indices by key hash
					let mut removes_by_key: IndexMap<Hash128, Vec<usize>> = IndexMap::new();
					let mut removes_undefined: Vec<usize> = Vec::new();

					for row_idx in 0..row_count {
						if let Some(key_hash) = keys[row_idx] {
							removes_by_key.entry(key_hash).or_default().push(row_idx);
						} else {
							removes_undefined.push(row_idx);
						}
					}

					// Process removes with defined keys (batched by key)
					for (key_hash, indices) in removes_by_key {
						let diffs = self.strategy.handle_remove(
							txn,
							&pre,
							&indices,
							side,
							&key_hash,
							&mut state,
							self,
							change.version,
						)?;
						result.extend(diffs);
					}

					// Process removes with undefined keys individually
					for idx in removes_undefined {
						let diffs = self.strategy.handle_remove_undefined(
							txn,
							&pre,
							idx,
							side,
							&mut state,
							self,
							change.version,
						)?;
						result.extend(diffs);
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Compute keys for pre and post
					let old_keys = self.compute_join_keys(&pre, exprs)?;
					let new_keys = self.compute_join_keys(&post, exprs)?;
					let row_count = post.row_count();

					// Group updates by (old_key, new_key) pair
					// Only updates with same key pair can be batched
					let mut updates_by_key: IndexMap<(Hash128, Hash128), Vec<usize>> =
						IndexMap::new();
					let mut updates_undefined: Vec<usize> = Vec::new();

					for row_idx in 0..row_count {
						match (old_keys[row_idx], new_keys[row_idx]) {
							(Some(old_key), Some(new_key)) => {
								updates_by_key
									.entry((old_key, new_key))
									.or_default()
									.push(row_idx);
							}
							_ => {
								// Any undefined key (old or new) is processed
								// individually
								updates_undefined.push(row_idx);
							}
						}
					}

					// Process updates with defined keys (batched by key pair)
					for ((old_key, new_key), indices) in updates_by_key {
						let diffs = self.strategy.handle_update(
							txn,
							&pre,
							&post,
							&indices,
							side,
							&old_key,
							&new_key,
							&mut state,
							self,
							change.version,
						)?;
						result.extend(diffs);
					}

					// Process updates with undefined keys individually
					for row_idx in updates_undefined {
						let diffs = self.strategy.handle_update_undefined(
							txn,
							&pre,
							&post,
							row_idx,
							side,
							&mut state,
							self,
							change.version,
						)?;
						result.extend(diffs);
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	// FIXME #244 The issue is that when we need to reconstruct an unmatched left row, we need the right side's
	// schema to create the combined layout To make that work it requires schema / layout information of the right
	// side this should unlock the test:
	// testsuite/flow/tests/scripts/backfill/18_multiple_joins_same_table.skip
	// testsuite/flow/tests/scripts/backfill/19_complex_multi_table.skip
	// testsuite/flow/tests/scripts/backfill/21_backfill_with_distinct.skip
	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		let mut found_columns: Vec<Columns> = Vec::new();

		for &row_number in rows {
			// Get the composite key for this row number (reverse lookup)
			let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number)? else {
				continue;
			};

			// Parse the composite key to extract left and optional right row numbers
			let Some((left_row_number, right_row_number)) = Self::parse_composite_key(key.as_ref()) else {
				continue;
			};

			// Get left columns from parent (no Row conversion)
			let left_cols = self.left_parent.pull(txn, &[left_row_number])?;
			if left_cols.is_empty() {
				continue;
			}

			if let Some(right_row_num) = right_row_number {
				// Matched join - has right row number
				let right_cols = self.right_parent.pull(txn, &[right_row_num])?;
				if !right_cols.is_empty() {
					// Use JoinedColumnsBuilder to create joined columns
					let builder = JoinedColumnsBuilder::new(&left_cols, &right_cols, &self.alias);
					let mut joined = builder.join_single(row_number, &left_cols, &right_cols);
					// Override the row number to match what was requested
					joined.row_numbers = CowVec::new(vec![row_number]);
					found_columns.push(joined);
				}
			} else {
				// Unmatched left row - use builder.unmatched_left
				let right_schema = self.right_parent.pull(txn, &[])?;
				let builder = JoinedColumnsBuilder::new(&left_cols, &right_schema, &self.alias);
				let mut unmatched = builder.unmatched_left(row_number, &left_cols, 0, &right_schema);
				// Override the row number to match what was requested
				unmatched.row_numbers = CowVec::new(vec![row_number]);
				found_columns.push(unmatched);
			}
		}

		// Combine found rows
		if found_columns.is_empty() {
			// Get schema from both parents and combine them
			let left_schema = self.left_parent.pull(txn, &[])?;
			let right_schema = self.right_parent.pull(txn, &[])?;

			// Use JoinedColumnsBuilder to get properly aliased names
			let builder = JoinedColumnsBuilder::new(&left_schema, &right_schema, &self.alias);
			let right_names = builder.right_column_names();

			// Add left columns as-is
			let mut all_columns: Vec<Column> = left_schema.columns.into_iter().collect();

			// Add right columns with pre-computed aliased names
			for (col, aliased_name) in right_schema.columns.into_iter().zip(right_names.iter()) {
				all_columns.push(Column {
					name: Fragment::internal(aliased_name),
					data: col.data,
				});
			}

			Ok(Columns {
				row_numbers: CowVec::new(Vec::new()),
				columns: CowVec::new(all_columns),
			})
		} else if found_columns.len() == 1 {
			Ok(found_columns.remove(0))
		} else {
			let mut result = found_columns.remove(0);
			for cols in found_columns {
				result.row_numbers.make_mut().extend(cols.row_numbers.iter().copied());
				for (i, col) in cols.columns.into_iter().enumerate() {
					result.columns.make_mut()[i].extend(col).expect("schema mismatch in join pull");
				}
			}
			Ok(result)
		}
	}
}
