use reifydb_core::{Row, interface::Transaction};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use super::eager::{
	add_to_state_entry, emit_joined_rows_left_to_right, emit_joined_rows_right_to_left,
	emit_remove_joined_rows_left, emit_remove_joined_rows_right, emit_update_joined_rows_left,
	emit_update_joined_rows_right, remove_from_state_entry, update_row_in_entry,
};
use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

pub(crate) struct InnerEagerJoin;

impl InnerEagerJoin {
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

		if let Some(key_hash) = key_hash {
			match side {
				JoinSide::Left => {
					// Add to left entries
					add_to_state_entry(txn, &mut state.left, &key_hash, post)?;

					// Only emit if there are matching right rows (inner join)
					let joined_rows = emit_joined_rows_left_to_right(
						txn,
						post,
						&state.right,
						&key_hash,
						state,
						operator,
					)?;
					result.extend(joined_rows);
				}
				JoinSide::Right => {
					// Add to right entries
					add_to_state_entry(txn, &mut state.right, &key_hash, post)?;

					// Only emit if there are matching left rows (inner join)
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
		}
		// Undefined keys produce no output in inner join

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

		if let Some(key_hash) = key_hash {
			match side {
				JoinSide::Left => {
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
						result.extend(removed_joins);

						// Remove from left entries and clean up if empty
						remove_from_state_entry(txn, &mut state.left, &key_hash, pre)?;
					}
				}
				JoinSide::Right => {
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

						// Remove from right entries and clean up if empty
						remove_from_state_entry(txn, &mut state.right, &key_hash, pre)?;
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
			if let Some(key) = old_key {
				match side {
					JoinSide::Left => {
						// Update the encoded in state
						if update_row_in_entry(txn, &mut state.left, &key, pre, post)? {
							// Emit updates for all joined rows (only if right rows exist)
							let updates = emit_update_joined_rows_left(
								txn,
								pre,
								post,
								&state.right,
								&key,
								state,
								operator,
							)?;
							result.extend(updates);
						}
					}
					JoinSide::Right => {
						// Update the encoded in state
						if update_row_in_entry(txn, &mut state.right, &key, pre, post)? {
							// Emit updates for all joined rows (only if left rows exist)
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
