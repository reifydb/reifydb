use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use indexmap::IndexMap;
use reifydb_core::{
	EncodedKey, Error, JoinType, Row,
	interface::FlowNodeId,
	util::encoding::keycode::KeySerializer,
	value::{
		column::{Column, Columns},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_engine::{ColumnEvaluationContext, StandardColumnEvaluator, execute::Executor, stack::Stack};
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::expression::Expression;
use reifydb_type::{Fragment, Params, RowNumber, Type, Value, internal};
use tracing::trace_span;

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
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

pub struct JoinOperator {
	pub(crate) left_parent: Arc<Operators>,
	pub(crate) right_parent: Arc<Operators>,
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
		left_parent: Arc<Operators>,
		right_parent: Arc<Operators>,
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
		EncodedValuesLayout::new(&[Type::Blob])
	}

	/// Compute join keys for all rows in Columns
	/// Returns Vec<Option<Hash128>> - one per row, None for rows with undefined key values
	pub(crate) fn compute_join_keys(
		&self,
		columns: &Columns,
		exprs: &[Expression],
	) -> crate::Result<Vec<Option<Hash128>>> {
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

	/// Generate a row for an unmatched left join result
	/// This creates a combined row with left values and Undefined values for right columns
	pub(crate) async fn unmatched_left_row(&self, txn: &mut FlowTransaction, left: &Row) -> crate::Result<Row> {
		// Skip expensive trace logging in hot path
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L'); // 'L' prefix for left row
		serializer.extend_u64(left.number.0);
		let composite_key = EncodedKey::new(serializer.finish());

		// Get or create a unique row number for this unmatched row
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key).await?;

		// Get the right side schema to create proper combined layout
		let right_schema = self.right_parent.pull(txn, &[]).await?;

		// Build combined layout with left columns + aliased right columns (all undefined)
		let left_field_count = left.layout.fields().fields.len();
		let right_field_count = right_schema.columns.len();
		let total_fields = left_field_count + right_field_count;

		let mut combined_names = Vec::with_capacity(total_fields);
		let mut combined_types = Vec::with_capacity(total_fields);
		let mut combined_values = Vec::with_capacity(total_fields);

		// Add left side columns
		for i in 0..left_field_count {
			let name = left
				.layout
				.get_name(i)
				.expect("EncodedValuesNamedLayout missing name for left field")
				.to_string();
			combined_names.push(name);
			combined_types.push(left.layout.fields().fields[i].r#type);
			combined_values.push(left.layout.get_value_by_idx(&left.encoded, i));
		}

		// Add right side columns with alias prefix and Undefined values
		let alias_str = self.alias.as_deref().unwrap_or("other");
		for col in right_schema.columns.iter() {
			let col_name = col.name().text();
			let prefixed_name = format!("{}_{}", alias_str, col_name);

			// Handle conflicts (same logic as JoinedLayoutBuilder)
			let mut final_name = prefixed_name.clone();
			if combined_names.contains(&final_name) {
				let mut counter = 2;
				loop {
					let candidate = format!("{}_{}", prefixed_name, counter);
					if !combined_names.contains(&candidate) {
						final_name = candidate;
						break;
					}
					counter += 1;
				}
			}

			combined_names.push(final_name);
			combined_types.push(col.data().get_type());
			combined_values.push(Value::Undefined);
		}

		// Create the combined layout and encode the values
		let fields: Vec<(String, Type)> = combined_names.into_iter().zip(combined_types.into_iter()).collect();
		let layout = reifydb_core::value::encoded::EncodedValuesNamedLayout::new(fields);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &combined_values);

		let result = Row {
			number: result_row_number,
			encoded,
			layout,
		};

		Ok(result)
	}

	/// Clean up all join results for a given left row
	/// This removes both matched and unmatched join results
	pub(crate) async fn cleanup_left_row_joins(
		&self,
		txn: &mut FlowTransaction,
		left_number: u64,
	) -> crate::Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		// Remove all mappings with this prefix
		self.row_number_provider.remove_by_prefix(txn, &prefix).await
	}

	pub(crate) async fn join_rows(&self, txn: &mut FlowTransaction, left: &Row, right: &Row) -> crate::Result<Row> {
		// Build the combined layout and get a stable row number
		let builder = JoinedLayoutBuilder::new(left, right, &self.alias);
		let composite_key = Self::make_composite_key(left.number, right.number);
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key).await?;

		let result = builder.build_row(result_row_number, left, right);

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

	/// Join one left row with multiple right rows efficiently.
	/// Uses get_or_create_row_numbers to minimize state operations.
	pub(crate) async fn join_rows_multiple_right(
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter()).await?;

		// Build all result rows
		let results = right_rows
			.iter()
			.zip(row_numbers.iter())
			.map(|(right, (row_number, _))| builder.build_row(*row_number, left, right))
			.collect();

		Ok(results)
	}

	/// Join multiple left rows with one right row.
	pub(crate) async fn join_rows_multiple_left(
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter()).await?;

		// Build all result rows
		let results = left_rows
			.iter()
			.zip(row_numbers.iter())
			.map(|(left, (row_number, _))| builder.build_row(*row_number, left, right))
			.collect();

		Ok(results)
	}

	/// Join multiple left rows with multiple right rows (cartesian product).
	/// Uses get_or_create_row_numbers to minimize state operations.
	pub(crate) async fn join_rows_cartesian(
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter()).await?;

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

#[async_trait]
impl Operator for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		// Check for self-referential calls (should never happen)
		if let FlowChangeOrigin::Internal(from_node) = &change.origin {
			if *from_node == self.node {
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

		// Group diffs by key_hash
		// We use IndexMap to preserve insertion order while still grouping
		let mut inserts_by_key: IndexMap<Hash128, Vec<Row>> = IndexMap::new();
		let mut removes_by_key: IndexMap<Hash128, Vec<Row>> = IndexMap::new();
		let mut inserts_undefined: Vec<Row> = Vec::new();
		let mut removes_undefined: Vec<Row> = Vec::new();
		// Updates are processed individually due to key change complexity
		let mut updates: Vec<(Row, Row, Option<Hash128>, Option<Hash128>)> = Vec::new();

		// Phase 1: Compute keys and group diffs
		let _phase1_span = trace_span!("join::phase1_group_diffs", diff_count = change.diffs.len()).entered();

		let exprs = match side {
			JoinSide::Left => &self.left_exprs,
			JoinSide::Right => &self.right_exprs,
		};

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Batch compute keys for all rows in this Columns
					let keys = self.compute_join_keys(&post, exprs)?;
					let row_count = post.row_count();

					for row_idx in 0..row_count {
						let row = post.extract_row(row_idx).to_single_row();
						if let Some(key_hash) = keys[row_idx] {
							inserts_by_key.entry(key_hash).or_default().push(row);
						} else {
							inserts_undefined.push(row);
						}
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Batch compute keys for all rows in this Columns
					let keys = self.compute_join_keys(&pre, exprs)?;
					let row_count = pre.row_count();

					for row_idx in 0..row_count {
						let row = pre.extract_row(row_idx).to_single_row();
						if let Some(key_hash) = keys[row_idx] {
							removes_by_key.entry(key_hash).or_default().push(row);
						} else {
							removes_undefined.push(row);
						}
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Batch compute keys for pre and post
					let old_keys = self.compute_join_keys(&pre, exprs)?;
					let new_keys = self.compute_join_keys(&post, exprs)?;
					let row_count = post.row_count();

					for row_idx in 0..row_count {
						let pre_row = pre.extract_row(row_idx).to_single_row();
						let post_row = post.extract_row(row_idx).to_single_row();
						updates.push((pre_row, post_row, old_keys[row_idx], new_keys[row_idx]));
					}
				}
			}
		}

		drop(_phase1_span);

		// Phase 2: Process batched inserts
		for (key_hash, rows) in inserts_by_key {
			let diffs = self
				.strategy
				.handle_insert_multiple(txn, &rows, side, &key_hash, &mut state, self)
				.await?;
			result.extend(diffs);
		}

		// Process inserts with undefined keys individually
		for post in inserts_undefined {
			let diffs = self.strategy.handle_insert(txn, &post, side, None, &mut state, self).await?;
			result.extend(diffs);
		}

		// Phase 3: Process batched removes
		{
			let _phase3_span =
				trace_span!("join::phase3_removes", batch_count = removes_by_key.len()).entered();
		}
		for (key_hash, rows) in removes_by_key {
			let diffs = self
				.strategy
				.handle_remove_multiple(txn, &rows, side, &key_hash, &mut state, self, change.version)
				.await?;
			result.extend(diffs);
		}

		// Process removes with undefined keys individually
		for pre in removes_undefined {
			let diffs = self
				.strategy
				.handle_remove(txn, &pre, side, None, &mut state, self, change.version)
				.await?;
			result.extend(diffs);
		}

		// Phase 4: Process updates individually (key change complexity requires this)
		{
			let _phase4_span = trace_span!("join::phase4_updates", update_count = updates.len()).entered();
		}
		for (pre, post, old_key, new_key) in updates {
			let diffs = self
				.strategy
				.handle_update(
					txn,
					&pre,
					&post,
					side,
					old_key,
					new_key,
					&mut state,
					self,
					change.version,
				)
				.await?;
			result.extend(diffs);
		}

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	// FIXME #244 The issue is that when we need to reconstruct an unmatched left row, we need the right side's
	// schema to create the combined layout To make that work it requires schema / layout information of the right
	// side this should unlock the test:
	// testsuite/flow/tests/scripts/backfill/18_multiple_joins_same_table.skip
	// testsuite/flow/tests/scripts/backfill/19_complex_multi_table.skip
	// testsuite/flow/tests/scripts/backfill/21_backfill_with_distinct.skip
	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		let mut found_columns: Vec<Columns> = Vec::new();

		for &row_number in rows {
			// Get the composite key for this row number (reverse lookup)
			let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number).await? else {
				continue;
			};

			// Parse the composite key to extract left and optional right row numbers
			let Some((left_row_number, right_row_number)) = Self::parse_composite_key(key.as_ref()) else {
				continue;
			};

			// Get left row from parent
			let left_cols = self.left_parent.pull(txn, &[left_row_number]).await?;
			if left_cols.is_empty() {
				continue;
			}
			let left_row = left_cols.to_single_row();

			if let Some(right_row_num) = right_row_number {
				// Matched join - has right row number
				let right_cols = self.right_parent.pull(txn, &[right_row_num]).await?;
				if !right_cols.is_empty() {
					let right_row = right_cols.to_single_row();
					// Reconstruct the joined row
					let joined = self.join_rows(txn, &left_row, &right_row).await?;
					let joined_with_number = Row {
						number: row_number,
						encoded: joined.encoded,
						layout: joined.layout,
					};
					found_columns.push(Columns::from_row(&joined_with_number));
				}
			} else {
				// Unmatched left row
				let unmatched = self.unmatched_left_row(txn, &left_row).await?;
				let unmatched_with_number = Row {
					number: row_number,
					encoded: unmatched.encoded,
					layout: unmatched.layout,
				};
				found_columns.push(Columns::from_row(&unmatched_with_number));
			}
		}

		// Combine found rows
		if found_columns.is_empty() {
			// Get schema from both parents and combine them
			let left_schema = self.left_parent.pull(txn, &[]).await?;
			let right_schema = self.right_parent.pull(txn, &[]).await?;

			// Add left columns as-is
			let mut all_columns: Vec<_> = left_schema.columns.into_iter().collect();
			let left_names: Vec<String> = all_columns.iter().map(|c| c.name.as_ref().to_string()).collect();

			// Add right columns WITH alias prefix (matching JoinedLayoutBuilder logic)
			let alias_str = self.alias.as_deref().unwrap_or("other");
			for col in right_schema.columns.into_iter() {
				let prefixed_name = format!("{}_{}", alias_str, col.name.as_ref());

				// Handle conflicts (same logic as JoinedLayoutBuilder)
				let mut final_name = prefixed_name.clone();
				if left_names.contains(&final_name)
					|| all_columns.iter().any(|c| c.name.as_ref() == final_name)
				{
					let mut counter = 2;
					loop {
						let candidate = format!("{}_{}", prefixed_name, counter);
						if !left_names.contains(&candidate)
							&& !all_columns.iter().any(|c| c.name.as_ref() == candidate)
						{
							final_name = candidate;
							break;
						}
						counter += 1;
					}
				}

				all_columns.push(Column {
					name: Fragment::internal(&final_name),
					data: col.data,
				});
			}

			Ok(Columns {
				row_numbers: reifydb_core::util::CowVec::new(Vec::new()),
				columns: reifydb_core::util::CowVec::new(all_columns),
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
