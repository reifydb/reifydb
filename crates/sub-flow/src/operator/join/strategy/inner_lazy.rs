use reifydb_core::{CommitVersion, Row};
use reifydb_engine::{StandardCommandTransaction, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use super::{
	eager::{add_to_state_entry, remove_from_state_entry, update_row_in_entry},
	lazy::query_right_side,
};
use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

pub(crate) struct InnerLazyJoin {
	pub(crate) query: QueryString,
	pub(crate) executor: Executor,
}

impl InnerLazyJoin {
	pub(crate) fn handle_insert(
		&self,
		txn: &mut StandardCommandTransaction,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			match side {
				JoinSide::Left => {
					// Add to left entries
					add_to_state_entry(txn, &mut state.left, &key_hash, post)?;

					// Query right side for matching rows
					let right_rows = query_right_side(
						txn,
						&self.query,
						&self.executor,
						key_hash,
						state,
						operator,
						version,
					)?;

					// Only emit if there are matches (inner join)
					if !right_rows.is_empty() {
						for right_row in &right_rows {
							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, post, right_row)?,
							});
						}
					}
					// No else clause - inner join doesn't emit unmatched rows
				}
				JoinSide::Right => {
					// Join with matching left rows
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);
							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, &left_row, post)?,
							});
						}
					}
					// No else clause - inner join doesn't emit unmatched rows
				}
			}
		}
		// Undefined keys produce no output in inner join

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
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if let Some(key_hash) = key_hash {
			match side {
				JoinSide::Left => {
					// Check if left entry exists
					if state.left.contains_key(txn, &key_hash)? {
						operator.cleanup_left_row_joins(txn, pre.number.0)?;

						// Remove all joins involving this encoded
						let right_rows = query_right_side(
							txn,
							&self.query,
							&self.executor,
							key_hash,
							state,
							operator,
							version,
						)?;

						if !right_rows.is_empty() {
							for right_row in &right_rows {
								result.push(FlowDiff::Remove {
									pre: operator.join_rows(txn, pre, right_row)?,
								});
							}
						}
						// No else clause - inner join has no unmatched rows to remove

						// Remove from left entries and clean up if empty
						remove_from_state_entry(txn, &mut state.left, &key_hash, pre)?;
					}
				}
				JoinSide::Right => {
					// Remove all joins involving this encoded
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);
							result.push(FlowDiff::Remove {
								pre: operator.join_rows(txn, &left_row, pre)?,
							});
						}
					}
					// No need to re-emit unmatched rows for inner join
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
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		let mut result = Vec::new();

		if old_key == new_key {
			// Key didn't change, update in place
			if let Some(key) = old_key {
				match side {
					JoinSide::Left => {
						// Update the encoded in state
						if update_row_in_entry(txn, &mut state.left, &key, pre, post)? {
							// Emit updates for all joined rows
							let right_rows = query_right_side(
								txn,
								&self.query,
								&self.executor,
								key,
								state,
								operator,
								version,
							)?;

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
							}
							// No else clause - inner join has no unmatched rows to update
						}
					}
					JoinSide::Right => {
						// In lazy mode, we don't track right-side state
						// We just emit updates for all joined rows with left side
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
			let remove_diffs = self.handle_remove(txn, pre, side, old_key, state, operator, version)?;
			result.extend(remove_diffs);

			let insert_diffs = self.handle_insert(txn, post, side, new_key, state, operator, version)?;
			result.extend(insert_diffs);
		}

		Ok(result)
	}
}
