use reifydb_core::Row;
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use super::eager::{
	add_to_state_entry, emit_joined_rows_left_to_right, emit_joined_rows_right_to_left,
	emit_remove_joined_rows_left, emit_remove_joined_rows_right, emit_update_joined_rows_left,
	emit_update_joined_rows_right, get_left_rows, is_first_right_row, remove_from_state_entry, update_row_in_entry,
};
use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

pub(crate) struct LeftEagerJoin;

impl LeftEagerJoin {
	pub(crate) fn handle_insert(
		&self,
		txn: &mut StandardCommandTransaction,
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
					add_to_state_entry(txn, &mut state.left, &key_hash, post)?;

					// Join with matching right rows
					let joined_rows = emit_joined_rows_left_to_right(
						txn,
						post,
						&state.right,
						&key_hash,
						state,
						operator,
					)?;

					if !joined_rows.is_empty() {
						result.extend(joined_rows);
					} else {
						// Left join: emit left encoded even without match
						let unmatched_row = operator.unmatched_left_row(txn, post)?;
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
					}
				} else {
					// Undefined key in left join still emits the encoded
					let unmatched_row = operator.unmatched_left_row(txn, post)?;
					result.push(FlowDiff::Insert {
						post: unmatched_row,
					});
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					let is_first = is_first_right_row(txn, &state.right, &key_hash)?;

					// Add to right entries
					add_to_state_entry(txn, &mut state.right, &key_hash, post)?;

					// Join with matching left rows
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						// If first right encoded, remove previously emitted unmatched left rows
						if is_first {
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
						let joined_rows = emit_joined_rows_right_to_left(
							txn,
							post,
							&state.left,
							&key_hash,
							state,
							operator,
						)?;
						result.extend(joined_rows);
					}
				}
				// Right side inserts with undefined keys don't produce output
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_remove(
		&self,
		txn: &mut StandardCommandTransaction,
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
					// Check if left entry exists
					if state.left.contains_key(txn, &key_hash)? {
						operator.cleanup_left_row_joins(txn, pre.number.0)?;

						// Remove all joins involving this encoded
						let removed_joins = emit_remove_joined_rows_left(
							txn,
							pre,
							&state.right,
							&key_hash,
							state,
							operator,
						)?;

						if !removed_joins.is_empty() {
							result.extend(removed_joins);
						} else {
							// Remove the unmatched left join encoded
							let unmatched_row = operator.unmatched_left_row(txn, pre)?;
							result.push(FlowDiff::Remove {
								pre: unmatched_row,
							});
						}

						// Remove from left entries and clean up if empty
						remove_from_state_entry(txn, &mut state.left, &key_hash, pre)?;
					}
				} else {
					// Undefined key - remove the unmatched encoded
					let unmatched_row = operator.unmatched_left_row(txn, pre)?;
					result.push(FlowDiff::Remove {
						pre: unmatched_row,
					});

					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Check if right entry exists
					if state.right.contains_key(txn, &key_hash)? {
						// Remove all joins involving this encoded
						let removed_joins = emit_remove_joined_rows_right(
							txn,
							pre,
							&state.left,
							&key_hash,
							state,
							operator,
						)?;
						result.extend(removed_joins);

						// Remove from right entries
						let became_empty =
							remove_from_state_entry(txn, &mut state.right, &key_hash, pre)?;

						// If this was the last right encoded, re-emit left rows as unmatched
						if became_empty {
							let left_rows =
								get_left_rows(txn, &state.left, &key_hash, state)?;
							for left_row in &left_rows {
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

	pub(crate) fn handle_update(
		&self,
		txn: &mut StandardCommandTransaction,
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
						// Update the encoded in state
						if update_row_in_entry(txn, &mut state.left, &key, pre, post)? {
							// Emit updates for all joined rows
							let updates = emit_update_joined_rows_left(
								txn,
								pre,
								post,
								&state.right,
								&key,
								state,
								operator,
							)?;

							if !updates.is_empty() {
								result.extend(updates);
							} else {
								// No matching right rows - update unmatched left
								// encoded
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
						// Both keys are undefined - update the encoded
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
						// Update the encoded in state
						if update_row_in_entry(txn, &mut state.right, &key, pre, post)? {
							// Emit updates for all joined rows
							let updates = emit_update_joined_rows_right(
								txn,
								pre,
								post,
								&state.left,
								&key,
								state,
								operator,
							)?;
							result.extend(updates);
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
