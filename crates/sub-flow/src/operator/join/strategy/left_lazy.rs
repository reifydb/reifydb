use reifydb_core::{
	FrameColumnData,
	interface::{Command, ExecuteCommand, Identity, Transaction},
	value::row::{EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;
use reifydb_type::{Params, ROW_NUMBER_COLUMN_NAME, RowNumber, Type};

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinSideEntry, JoinState, SerializedRow, operator::JoinOperator},
};

pub(crate) struct LeftLazyJoin {
	pub(crate) query: QueryString,
	pub(crate) executor: Executor,
}

impl LeftLazyJoin {
	/// Query the right side and return rows that match the join condition
	fn query_right_side<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		key_hash: Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<Row>> {
		// Execute the query without parameters
		// The query may have its own filter (e.g., "from table | filter condition")
		// but we don't inject parameters from the left row
		let query = Command {
			rql: self.query.as_str(),
			params: Params::None,
			identity: &Identity::root(), // TODO: Should use proper identity from context
		};

		// Execute the query to get all right-side rows
		let results = self.executor.execute_command(txn, query)?;

		let mut right_rows = Vec::new();

		// Process query results - each frame contains rows to join with
		for frame in results {
			// Get row count from columns
			let row_count = if let Some(first_column) = frame.columns.first() {
				first_column.data.len()
			} else {
				0
			};

			// Extract schema from frame columns for each frame (don't rely on state)
			let mut frame_names = Vec::new();
			let mut frame_types = Vec::new();

			for column in frame.columns.iter() {
				frame_names.push(column.name.clone());
				// Infer type from the column data - handle ALL variants
				let column_type = match &column.data {
					FrameColumnData::Bool(_) => Type::Boolean,
					FrameColumnData::Int1(_) => Type::Int1,
					FrameColumnData::Int2(_) => Type::Int2,
					FrameColumnData::Int4(_) => Type::Int4,
					FrameColumnData::Int8(_) => Type::Int8,
					FrameColumnData::Uint1(_) => Type::Uint1,
					FrameColumnData::Uint2(_) => Type::Uint2,
					FrameColumnData::Uint4(_) => Type::Uint4,
					FrameColumnData::Uint8(_) => Type::Uint8,
					FrameColumnData::Uint16(_) => Type::Uint16,
					FrameColumnData::Float4(_) => Type::Float4,
					FrameColumnData::Float8(_) => Type::Float8,
					FrameColumnData::Utf8(_) => Type::Utf8,
					FrameColumnData::RowNumber(_) => Type::RowNumber,
					_ => Type::Undefined,
				};
				frame_types.push(column_type);
			}

			// Update state schema if empty (for backward compatibility)
			if state.schema.right_types.is_empty() && !frame.columns.is_empty() {
				state.schema.right_names = frame_names.clone();
				state.schema.right_types = frame_types.clone();
			}

			// Process rows in order to ensure consistency
			for row_idx in 0..row_count {
				// Get the actual row number from frame.row_numbers
				let row_number = frame.row_numbers.get(row_idx).copied().unwrap();

				// Extract values for this row from all columns
				let mut values = Vec::new();
				for column in frame.columns.iter() {
					values.push(column.data.get_value(row_idx));
				}

				// Create a Row with the proper structure using the frame's schema
				let fields: Vec<(String, Type)> = frame_names
					.iter()
					.zip(frame_types.iter())
					.map(|(name, typ)| (name.clone(), typ.clone()))
					.collect();

				debug_assert!(!fields.is_empty());

				let right_layout = EncodedRowNamedLayout::new(fields);
				let mut encoded_row = right_layout.allocate_row();
				right_layout.set_values(&mut encoded_row, &values);

				let right_row = Row {
					number: row_number,
					encoded: encoded_row,
					layout: right_layout,
				};

				// Compute the join key hash for this right row
				let evaluator = StandardRowEvaluator::new();
				let right_key_hash =
					operator.compute_join_key(&right_row, &operator.right_exprs, &evaluator)?;

				// Only include this row if it matches the left row's key
				if let Some(hash) = right_key_hash {
					if hash == key_hash {
						right_rows.push(right_row);
					}
				}
			}
		}

		Ok(right_rows)
	}

	pub(crate) fn handle_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				if let Some(key_hash) = key_hash {
					// Add to left entries
					let serialized = SerializedRow::from_row(post);
					let mut entry =
						state.left.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
							rows: Vec::new(),
						})?;
					entry.rows.push(serialized);
					state.left.set(txn, &key_hash, &entry)?;

					let right_rows = self.query_right_side(txn, key_hash, state, operator)?;
					if !right_rows.is_empty() {
						for right_row in &right_rows {
							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, post, right_row)?,
							});
						}
					} else {
						// Left join: emit left row even without match
						// Use row number provider for consistency
						let unmatched_row = operator.unmatched_left_row(txn, post)?;
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
					}
				} else {
					// Undefined key in left join still emits the row
					// Use row number provider for consistency
					let unmatched_row = operator.unmatched_left_row(txn, post)?;
					result.push(FlowDiff::Insert {
						post: unmatched_row,
					});
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Query right rows and check if any exist OTHER than the one being inserted
					let existing_right_rows =
						self.query_right_side(txn, key_hash, state, operator)?;
					// This is the first right row if either no rows exist, or only the current row
					// exists
					let is_first_right_row = existing_right_rows.is_empty()
						|| (existing_right_rows.len() == 1
							&& existing_right_rows[0].number == post.number);

					// Join with matching left rows
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						// If first right row, remove previously emitted unmatched left rows
						if is_first_right_row {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								result.push(FlowDiff::Remove {
									pre: unmatched_row,
								});
							}
						}

						// Add properly joined rows
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, &left_row, post)?,
							});
						}
					}
				}
				// Right side inserts with undefined keys don't produce output
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				if let Some(key_hash) = key_hash {
					// Remove from left entries
					if let Some(mut left_entry) = state.left.get(txn, &key_hash)? {
						left_entry.rows.retain(|r| r.number != pre.number);

						operator.cleanup_left_row_joins(txn, pre.number.0)?;

						// Remove all joins involving this row
						let right_rows =
							self.query_right_side(txn, key_hash, state, operator)?;
						if !right_rows.is_empty() {
							for right_row in &right_rows {
								result.push(FlowDiff::Remove {
									pre: operator.join_rows(txn, pre, right_row)?,
								});
							}
						} else {
							// Remove the unmatched left join row
							// Use row number provider for consistency
							let unmatched_row = operator.unmatched_left_row(txn, pre)?;
							result.push(FlowDiff::Remove {
								pre: unmatched_row,
							});
						}

						// Clean up empty entries
						if left_entry.rows.is_empty() {
							state.left.remove(txn, &key_hash)?;
						} else {
							state.left.set(txn, &key_hash, &left_entry)?;
						}
					}
				} else {
					// Undefined key - remove the unmatched row
					// Use row number provider for consistency
					let unmatched_row = operator.unmatched_left_row(txn, pre)?;
					result.push(FlowDiff::Remove {
						pre: unmatched_row,
					});

					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Query current right rows to check if any remain after removing this one
					let remaining_right_rows =
						self.query_right_side(txn, key_hash, state, operator)?;
					let has_other_right_rows =
						remaining_right_rows.iter().any(|r| r.number != pre.number);

					// Remove all joins involving this row
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);

							result.push(FlowDiff::Remove {
								pre: operator.join_rows(txn, &left_row, pre)?,
							});
						}

						// If this was the last right row, re-emit left rows as unmatched
						if !has_other_right_rows {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								result.push(FlowDiff::Insert {
									post: unmatched_row,
								});
							}
						}
					}
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		side: JoinSide,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if old_key == new_key {
			// Key didn't change, update in place
			match side {
				JoinSide::Left => {
					if let Some(key) = old_key {
						if let Some(mut left_entry) = state.left.get(txn, &key)? {
							// Update the row
							for row in &mut left_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.left.set(txn, &key, &left_entry)?;

							// Emit updates for all joined rows
							let right_rows =
								self.query_right_side(txn, key, state, operator)?;
							if !right_rows.is_empty() {
								for right_row in &right_rows {
									result.push(FlowDiff::Update {
										pre: operator.join_rows(
											txn, pre, right_row,
										)?,
										post: operator.join_rows(
											txn, post, right_row,
										)?,
									});
								}
							} else {
								// No matching right rows - update unmatched left row
								let unmatched_pre =
									operator.unmatched_left_row(txn, pre)?;
								let unmatched_post =
									operator.unmatched_left_row(txn, post)?;
								result.push(FlowDiff::Update {
									pre: unmatched_pre,
									post: unmatched_post,
								});
							}
						}
					} else {
						// Both keys are undefined - update the row
						let unmatched_pre = operator.unmatched_left_row(txn, pre)?;
						let unmatched_post = operator.unmatched_left_row(txn, post)?;
						result.push(FlowDiff::Update {
							pre: unmatched_pre,
							post: unmatched_post,
						});
					}
				}
				JoinSide::Right => {
					if let Some(key) = old_key {
						// In lazy mode, we don't track right-side state
						// We just emit updates for all joined rows with left side

						// Emit updates for all joined rows
						if let Some(left_entry) = state.left.get(txn, &key)? {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);

								result.push(FlowDiff::Update {
									pre: operator.join_rows(txn, &left_row, pre)?,
									post: operator
										.join_rows(txn, &left_row, post)?,
								});
							}
						}
					}
				}
			}
		} else {
			// Key changed - treat as remove + insert
			let remove_diffs = self.handle_remove(txn, pre, side, old_key, state, operator)?;
			result.extend(remove_diffs);

			let insert_diffs = self.handle_insert(txn, post, side, new_key, state, operator)?;
			result.extend(insert_diffs);
		}

		Ok(result)
	}
}
