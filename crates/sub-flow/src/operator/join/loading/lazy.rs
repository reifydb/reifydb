use std::{sync::Arc, time::Instant};

use reifydb_core::{
	frame::FrameColumnData,
	interface::{Command, ExecuteCommand, ExecuteQuery, Identity, Query, Transaction},
	value::row::{EncodedRowLayout, EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;
use reifydb_type::Type;

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSideEntry, JoinState, RowParams, Schema, SerializedRow, operator::JoinOperator},
};

/// Lazy loading strategy - queries data on-demand
/// TODO: Currently using same implementation as Eager, will be updated to query on-demand
#[derive(Clone)]
pub(crate) struct LazyLoading {
	query: QueryString,
	executor: Executor,
}

impl LazyLoading {
	pub(crate) fn new(query: QueryString, executor: Executor) -> Self {
		Self {
			query,
			executor,
		}
	}

	/// Query the right side using the left row's values as parameters
	/// Returns the matching right-side rows
	fn query_right_side<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		left_row: &Row,
		key_hash: Hash128,
		state: &mut JoinState,
	) -> crate::Result<Vec<Row>> {
		// Convert the left row to encoded format with named layout
		let (layout, encoded_row) = Schema::row_to_encoded(left_row);

		// Create parameters from the left row
		let row_params = RowParams::from_encoded_row(layout, encoded_row);

		// Debug: print the query string being executed
		eprintln!("DEBUG: Executing lazy loading query: '{}'", self.query.as_str());
		eprintln!("DEBUG: With params: {:?}", row_params.to_params());

		let query = Command {
			rql: self.query.as_str(),
			params: row_params.to_params(),
			identity: &Identity::root(), // TODO: Should use proper identity from context
		};

		// Execute the query to get matching right-side rows
		let results = self.executor.execute_command(txn, query)?;

		// Debug output to verify execution
		dbg!(&self.query);
		dbg!(&row_params.to_params());

		let mut right_rows = Vec::new();

		// Process query results - each frame contains rows to join with
		for frame in results {
			// Frame is columnar, need to iterate through row indices
			if let Some(first_column) = frame.first() {
				let row_count = first_column.data.len();

				// If we don't have schema info yet, extract it from the frame columns
				if state.schema.right_types.is_empty() && !frame.is_empty() {
					// Extract types and names from frame columns
					for column in frame.iter() {
						state.schema.right_names.push(column.name.clone());
						// Infer type from the column data
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
							FrameColumnData::Utf8(_) => Type::Utf8,
							_ => Type::Undefined,
						};
						state.schema.right_types.push(column_type);
					}
					// Schema will be persisted when state is saved
				}

				for row_idx in 0..row_count {
					// Extract values for this row from all columns
					let mut values = Vec::new();
					for column in frame.iter() {
						values.push(column.data.get_value(row_idx));
					}

					// Create a Row with the proper structure
					// We need to encode these values into the right side's layout
					let fields: Vec<(String, Type)> = state
						.schema
						.right_names
						.iter()
						.zip(state.schema.right_types.iter())
						.map(|(name, typ)| (name.clone(), typ.clone()))
						.collect();

					// Skip if we don't have fields (shouldn't happen after the check above)
					if fields.is_empty() {
						continue;
					}

					let right_layout = EncodedRowNamedLayout::new(fields);
					let mut encoded_row = right_layout.allocate_row();
					right_layout.set_values(&mut encoded_row, &values);

					let right_row = Row {
						number: reifydb_type::RowNumber(0), // Will be assigned by join
						encoded: encoded_row,
						layout: right_layout,
					};

					right_rows.push(right_row);
				}
			}
		}

		Ok(right_rows)
	}

	pub(crate) fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let start = Instant::now();
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			// Add to left entries in state
			let serialized = SerializedRow::from_row(post);
			let mut entry = state.left.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
				rows: Vec::new(),
			})?;
			entry.rows.push(serialized);
			state.left.set(txn, &key_hash, &entry)?;

			// Query the right side using the left row
			let right_rows = self.query_right_side(txn, post, key_hash, state)?;

			// Create joined rows for each matching right row
			for right_row in right_rows {
				result.push(FlowDiff::Insert {
					post: operator.join_rows(txn, post, &right_row)?,
				});
			}
		}

		println!("DEBUG: TOOK: {}", start.elapsed().as_micros());
		Ok(result)
	}

	pub(crate) fn handle_right_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			// Don't cache the right row in lazy loading
			// But still join with any existing left rows
			if let Some(left_entry) = state.left.get(txn, &key_hash)? {
				for left_row_ser in &left_entry.rows {
					let left_row = left_row_ser.to_left_row(&state.schema);
					result.push(FlowDiff::Insert {
						post: operator.join_rows(txn, &left_row, post)?,
					});
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_left_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			// Query fresh right-side rows to ensure we remove the correct joins
			let right_rows = self.query_right_side(txn, pre, key_hash, state)?;

			// Generate remove diffs for all matching right rows
			for right_row in right_rows {
				result.push(FlowDiff::Remove {
					pre: operator.join_rows(txn, pre, &right_row)?,
				});
			}

			// Remove from left entries
			if let Some(mut left_entry) = state.left.get(txn, &key_hash)? {
				let serialized = SerializedRow::from_row(pre);
				if let Some(pos) = left_entry.rows.iter().position(|r| r == &serialized) {
					left_entry.rows.remove(pos);
					if left_entry.rows.is_empty() {
						state.left.remove(txn, &key_hash)?;
					} else {
						state.left.set(txn, &key_hash, &left_entry)?;
					}
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_right_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			// Don't maintain right-side cache in lazy loading
			// But still generate remove diffs for any existing left rows
			if let Some(left_entry) = state.left.get(txn, &key_hash)? {
				for left_row_ser in &left_entry.rows {
					let left_row = left_row_ser.to_left_row(&state.schema);
					result.push(FlowDiff::Remove {
						pre: operator.join_rows(txn, &left_row, pre)?,
					});
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_left_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		// If key changed, handle as remove + insert
		if old_key != new_key {
			result.extend(self.handle_left_remove(txn, pre, old_key, state, operator)?);
			result.extend(self.handle_left_insert(txn, post, new_key, state, operator)?);
		} else if let Some(key_hash) = old_key {
			// Key unchanged, update in place
			if let Some(mut left_entry) = state.left.get(txn, &key_hash)? {
				let old_serialized = SerializedRow::from_row(pre);
				if let Some(pos) = left_entry.rows.iter().position(|r| r == &old_serialized) {
					left_entry.rows[pos] = SerializedRow::from_row(post);
					state.left.set(txn, &key_hash, &left_entry)?;

					// Query the right side for fresh data using the updated left row
					let right_rows = self.query_right_side(txn, post, key_hash, state)?;

					// Generate updates for matching right rows
					// First remove old joins, then add new ones
					for right_row in &right_rows {
						// For updates, we need to generate Update diffs
						result.push(FlowDiff::Update {
							pre: operator.join_rows(txn, pre, right_row)?,
							post: operator.join_rows(txn, post, right_row)?,
						});
					}
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_right_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		// If key changed, handle as remove + insert
		if old_key != new_key {
			result.extend(self.handle_right_remove(txn, pre, old_key, state, operator)?);
			result.extend(self.handle_right_insert(txn, post, new_key, state, operator)?);
		} else if let Some(key_hash) = old_key {
			// Key unchanged, generate updates for matching left rows
			// Don't maintain right-side cache in lazy loading
			if let Some(left_entry) = state.left.get(txn, &key_hash)? {
				for left_row_ser in &left_entry.rows {
					let left_row = left_row_ser.to_left_row(&state.schema);
					result.push(FlowDiff::Update {
						pre: operator.join_rows(txn, &left_row, pre)?,
						post: operator.join_rows(txn, &left_row, post)?,
					});
				}
			}
		}

		Ok(result)
	}
}
