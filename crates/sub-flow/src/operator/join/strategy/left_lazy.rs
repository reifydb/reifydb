use reifydb_core::{CommitVersion, Row};
use reifydb_engine::{StandardCommandTransaction, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use super::{
	eager::{add_to_state_entry, remove_from_state_entry, update_row_in_entry},
	lazy::{has_other_right_rows, query_right_side},
};
use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

pub(crate) struct LeftLazyJoin {
	pub(crate) query: QueryString,
	pub(crate) executor: Executor,
}

impl LeftLazyJoin {
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

		match side {
			JoinSide::Left => {
				if let Some(key_hash) = key_hash {
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

					if !right_rows.is_empty() {
						for right_row in &right_rows {
							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, post, right_row)?,
							});
						}
					} else {
						// Left join: emit left encoded even without match
						let unmatched_row = operator.unmatched_left_row(txn, post)?;
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
						// Track that we emitted an undefined join for this left encoded
						state.undefined_emitted.insert(txn, post.number)?;
					}
				} else {
					// Undefined key in left join still emits the encoded
					let unmatched_row = operator.unmatched_left_row(txn, post)?;
					result.push(FlowDiff::Insert {
						post: unmatched_row,
					});
					// Track undefined emission for rows with undefined keys too
					state.undefined_emitted.insert(txn, post.number)?;
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Join with matching left rows
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						// Check each left encoded to see if it has an undefined join we need to
						// remove
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);
							// If this left encoded had an undefined join, remove it (only
							// once)
							if state.undefined_emitted.remove(txn, left_row.number)? {
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

		match side {
			JoinSide::Left => {
				if let Some(key_hash) = key_hash {
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
						} else {
							// Remove the unmatched left join encoded
							let unmatched_row = operator.unmatched_left_row(txn, pre)?;
							result.push(FlowDiff::Remove {
								pre: unmatched_row,
							});
						}

						// Remove from left entries and clean up if empty
						remove_from_state_entry(txn, &mut state.left, &key_hash, pre)?;
						// Clean up tracking for this left encoded
						state.undefined_emitted.remove(txn, pre.number)?;
					}
				} else {
					// Undefined key - remove the unmatched encoded
					let unmatched_row = operator.unmatched_left_row(txn, pre)?;
					result.push(FlowDiff::Remove {
						pre: unmatched_row,
					});

					operator.cleanup_left_row_joins(txn, pre.number.0)?;
					// Clean up tracking for this left encoded
					state.undefined_emitted.remove(txn, pre.number)?;
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Check if there are other right rows besides this one
					let has_other_rows = has_other_right_rows(
						txn,
						&self.query,
						&self.executor,
						key_hash,
						state,
						operator,
						pre,
						version,
					)?;

					// Remove all joins involving this encoded
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);
							result.push(FlowDiff::Remove {
								pre: operator.join_rows(txn, &left_row, pre)?,
							});
						}

						// If this was the last right encoded, re-emit left rows as unmatched
						if !has_other_rows {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								result.push(FlowDiff::Insert {
									post: unmatched_row,
								});
								// Track that we re-emitted undefined for this left
								// encoded
								state.undefined_emitted.insert(txn, left_row.number)?;
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
		version: CommitVersion,
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
