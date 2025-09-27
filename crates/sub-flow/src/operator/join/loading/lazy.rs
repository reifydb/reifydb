use reifydb_core::{interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSideEntry, JoinState, SerializedRow, operator::JoinOperator},
};

/// Lazy loading strategy - queries data on-demand
/// TODO: Currently using same implementation as Eager, will be updated to query on-demand
#[derive(Debug, Clone)]
pub(crate) struct LazyLoading {
	query: QueryString,
}

impl LazyLoading {
	pub(crate) fn new(query: QueryString) -> Self {
		Self {
			query,
		}
	}

	// For now, using same implementation as EagerLoading
	// TODO: Implement actual lazy loading with on-demand queries
	pub(crate) fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		dbg!(&self.query);

		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			// Add to left entries in state
			let serialized = SerializedRow::from_row(post);
			let mut entry = state.left.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
				rows: Vec::new(),
			})?;
			entry.rows.push(serialized);
			state.left.set(txn, &key_hash, &entry)?;

			// Return matching right rows for join operation
			if let Some(right_entry) = state.right.get(txn, &key_hash)? {
				for right_row_ser in &right_entry.rows {
					let right_row = right_row_ser.to_right_row(&state.schema);
					result.push(FlowDiff::Insert {
						post: operator.join_rows(txn, post, &right_row)?,
					});
				}
			}
		}

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
			// Add to right entries in state
			let serialized = SerializedRow::from_row(post);
			let mut entry = state.right.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
				rows: Vec::new(),
			})?;
			entry.rows.push(serialized);
			state.right.set(txn, &key_hash, &entry)?;

			// Return matching left rows for join operation
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
			// Get matching right rows before removing
			if let Some(right_entry) = state.right.get(txn, &key_hash)? {
				for right_row_ser in &right_entry.rows {
					let right_row = right_row_ser.to_right_row(&state.schema);
					result.push(FlowDiff::Remove {
						pre: operator.join_rows(txn, pre, &right_row)?,
					});
				}
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
			// Get matching left rows before removing
			if let Some(left_entry) = state.left.get(txn, &key_hash)? {
				for left_row_ser in &left_entry.rows {
					let left_row = left_row_ser.to_left_row(&state.schema);
					result.push(FlowDiff::Remove {
						pre: operator.join_rows(txn, &left_row, pre)?,
					});
				}
			}

			// Remove from right entries
			if let Some(mut right_entry) = state.right.get(txn, &key_hash)? {
				let serialized = SerializedRow::from_row(pre);
				if let Some(pos) = right_entry.rows.iter().position(|r| r == &serialized) {
					right_entry.rows.remove(pos);
					if right_entry.rows.is_empty() {
						state.right.remove(txn, &key_hash)?;
					} else {
						state.right.set(txn, &key_hash, &right_entry)?;
					}
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

					// Generate updates for matching right rows
					if let Some(right_entry) = state.right.get(txn, &key_hash)? {
						for right_row_ser in &right_entry.rows {
							let right_row = right_row_ser.to_right_row(&state.schema);
							result.push(FlowDiff::Update {
								pre: operator.join_rows(txn, pre, &right_row)?,
								post: operator.join_rows(txn, post, &right_row)?,
							});
						}
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
			// Key unchanged, update in place
			if let Some(mut right_entry) = state.right.get(txn, &key_hash)? {
				let old_serialized = SerializedRow::from_row(pre);
				if let Some(pos) = right_entry.rows.iter().position(|r| r == &old_serialized) {
					right_entry.rows[pos] = SerializedRow::from_row(post);
					state.right.set(txn, &key_hash, &right_entry)?;

					// Generate updates for matching left rows
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
			}
		}

		Ok(result)
	}
}
