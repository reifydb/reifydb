use reifydb_core::{CommitVersion, Row, log_trace};
use reifydb_flow_operator_sdk::FlowDiff;
use reifydb_hash::Hash128;

use super::hash::{
	add_to_state_entry, emit_joined_rows_batch_left, emit_joined_rows_batch_right, emit_joined_rows_left_to_right,
	emit_joined_rows_right_to_left, emit_remove_joined_rows_batch_left, emit_remove_joined_rows_batch_right,
	emit_remove_joined_rows_left, emit_remove_joined_rows_right, emit_update_joined_rows_left,
	emit_update_joined_rows_right, get_left_rows, is_first_right_row, remove_from_state_entry, update_row_in_entry,
};
use crate::{
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
	transaction::FlowTransaction,
};

pub(crate) struct LeftHashJoin;

impl LeftHashJoin {
	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		log_trace!(
			"[LEFT_JOIN] handle_insert: side={:?}, key_hash={:?}, post_row={}",
			side,
			key_hash,
			post.number.0
		);

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				log_trace!("[LEFT_JOIN] handle_insert: processing Left side");
				if let Some(key_hash) = key_hash {
					// Add to left entries
					add_to_state_entry(txn, &mut state.left, &key_hash, post)?;

					// Join with matching right rows
					let joined_rows = emit_joined_rows_left_to_right(
						txn,
						post,
						&state.right,
						&key_hash,
						operator,
						&operator.right_parent,
					)?;

					log_trace!(
						"[LEFT_JOIN] handle_insert: Left joined_rows count={}",
						joined_rows.len()
					);

					if !joined_rows.is_empty() {
						result.extend(joined_rows);
					} else {
						// Left join: emit left encoded even without match
						log_trace!(
							"[LEFT_JOIN] handle_insert: Left no matches, emitting unmatched_left_row"
						);
						let unmatched_row = operator.unmatched_left_row(txn, post)?;
						log_trace!(
							"[LEFT_JOIN] handle_insert: unmatched_row layout={:?}",
							unmatched_row.layout.names()
						);
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
					}
				} else {
					// Undefined key in left join still emits the encoded
					log_trace!(
						"[LEFT_JOIN] handle_insert: Left undefined key, emitting unmatched_left_row"
					);
					let unmatched_row = operator.unmatched_left_row(txn, post)?;
					result.push(FlowDiff::Insert {
						post: unmatched_row,
					});
				}
			}
			JoinSide::Right => {
				log_trace!("[LEFT_JOIN] handle_insert: processing Right side");
				if let Some(key_hash) = key_hash {
					let is_first = is_first_right_row(txn, &state.right, &key_hash)?;
					log_trace!("[LEFT_JOIN] handle_insert: Right is_first={}", is_first);

					// Add to right entries
					add_to_state_entry(txn, &mut state.right, &key_hash, post)?;

					// Join with matching left rows
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						log_trace!(
							"[LEFT_JOIN] handle_insert: Right found {} left rows to join",
							left_entry.rows.len()
						);

						// If first right encoded, remove previously emitted unmatched left rows
						if is_first {
							log_trace!(
								"[LEFT_JOIN] handle_insert: Right first match, removing unmatched lefts"
							);
							let left_rows =
								operator.left_parent.get_rows(txn, &left_entry.rows)?;

							for left_row_opt in left_rows {
								if let Some(left_row) = left_row_opt {
									let unmatched_row = operator
										.unmatched_left_row(txn, &left_row)?;
									log_trace!(
										"[LEFT_JOIN] handle_insert: Removing unmatched left row={}",
										unmatched_row.number.0
									);
									result.push(FlowDiff::Remove {
										pre: unmatched_row,
									});
								}
							}
						}

						// Add properly joined rows
						let joined_rows = emit_joined_rows_right_to_left(
							txn,
							post,
							&state.left,
							&key_hash,
							operator,
							&operator.left_parent,
						)?;
						log_trace!(
							"[LEFT_JOIN] handle_insert: Right emitting {} joined rows",
							joined_rows.len()
						);
						result.extend(joined_rows);
					} else {
						log_trace!("[LEFT_JOIN] handle_insert: Right no matching left entries");
					}
				}
				// Right side inserts with undefined keys don't produce output
			}
		}

		log_trace!("[LEFT_JOIN] handle_insert: returning {} diffs", result.len());
		Ok(result)
	}

	pub(crate) fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		log_trace!(
			"[LEFT_JOIN] handle_remove: side={:?}, key_hash={:?}, pre_row={}",
			side,
			key_hash,
			pre.number.0
		);

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				log_trace!("[LEFT_JOIN] handle_remove: processing Left side");
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
							operator,
							&operator.right_parent,
						)?;

						log_trace!(
							"[LEFT_JOIN] handle_remove: Left removed_joins count={}",
							removed_joins.len()
						);

						if !removed_joins.is_empty() {
							result.extend(removed_joins);
						} else {
							// Remove the unmatched left join encoded
							log_trace!(
								"[LEFT_JOIN] handle_remove: Left no joins, removing unmatched"
							);
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
					log_trace!("[LEFT_JOIN] handle_remove: Left undefined key, removing unmatched");
					let unmatched_row = operator.unmatched_left_row(txn, pre)?;
					result.push(FlowDiff::Remove {
						pre: unmatched_row,
					});

					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				}
			}
			JoinSide::Right => {
				log_trace!("[LEFT_JOIN] handle_remove: processing Right side");
				if let Some(key_hash) = key_hash {
					// Check if right entry exists
					if state.right.contains_key(txn, &key_hash)? {
						// Remove all joins involving this encoded
						let removed_joins = emit_remove_joined_rows_right(
							txn,
							pre,
							&state.left,
							&key_hash,
							operator,
							&operator.left_parent,
						)?;
						log_trace!(
							"[LEFT_JOIN] handle_remove: Right removed_joins count={}",
							removed_joins.len()
						);
						result.extend(removed_joins);

						// Remove from right entries
						let became_empty =
							remove_from_state_entry(txn, &mut state.right, &key_hash, pre)?;

						log_trace!(
							"[LEFT_JOIN] handle_remove: Right became_empty={}",
							became_empty
						);

						// If this was the last right encoded, re-emit left rows as unmatched
						if became_empty {
							log_trace!(
								"[LEFT_JOIN] handle_remove: Right last row removed, re-emitting unmatched lefts"
							);
							let left_rows = get_left_rows(
								txn,
								&state.left,
								&key_hash,
								&operator.left_parent,
								version,
							)?;
							for left_row in &left_rows {
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								log_trace!(
									"[LEFT_JOIN] handle_remove: Re-emitting unmatched left row={}",
									unmatched_row.number.0
								);
								result.push(FlowDiff::Insert {
									post: unmatched_row,
								});
							}
						}
					}
				}
			}
		}

		log_trace!("[LEFT_JOIN] handle_remove: returning {} diffs", result.len());
		Ok(result)
	}

	pub(crate) fn handle_update(
		&self,
		txn: &mut FlowTransaction,
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
							let updates = emit_update_joined_rows_left(
								txn,
								pre,
								post,
								&state.right,
								&key,
								operator,
								&operator.right_parent,
								version,
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
								operator,
								&operator.left_parent,
								version,
							)?;
							result.extend(updates);
						}
					}
				}
			}
		} else {
			// Key changed - treat as remove + insert
			let remove_diffs = self.handle_remove(txn, pre, side, old_key, state, operator, version)?;
			result.extend(remove_diffs);

			let insert_diffs = self.handle_insert(txn, post, side, new_key, state, operator)?;
			result.extend(insert_diffs);
		}

		Ok(result)
	}

	pub(crate) fn handle_insert_batch(
		&self,
		txn: &mut FlowTransaction,
		rows: &[Row],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				// Add all rows to state first
				for row in rows {
					add_to_state_entry(txn, &mut state.left, key_hash, row)?;
				}

				// Check if there are matching right rows
				let joined_rows = emit_joined_rows_batch_left(
					txn,
					rows,
					&state.right,
					key_hash,
					operator,
					&operator.right_parent,
				)?;

				if !joined_rows.is_empty() {
					result.extend(joined_rows);
				} else {
					// No matches - emit unmatched left rows for all
					for row in rows {
						let unmatched_row = operator.unmatched_left_row(txn, row)?;
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
					}
				}
			}
			JoinSide::Right => {
				let is_first = is_first_right_row(txn, &state.right, key_hash)?;

				// Add all rows to state first
				for row in rows {
					add_to_state_entry(txn, &mut state.right, key_hash, row)?;
				}

				// If first right row(s), remove previously emitted unmatched left rows
				if is_first {
					if let Some(left_entry) = state.left.get(txn, key_hash)? {
						let left_rows = operator.left_parent.get_rows(txn, &left_entry.rows)?;
						for left_row_opt in left_rows {
							if let Some(left_row) = left_row_opt {
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								result.push(FlowDiff::Remove {
									pre: unmatched_row,
								});
							}
						}
					}
				}

				// Emit all joined rows in one batch
				let joined_rows = emit_joined_rows_batch_right(
					txn,
					rows,
					&state.left,
					key_hash,
					operator,
					&operator.left_parent,
				)?;
				result.extend(joined_rows);
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_remove_batch(
		&self,
		txn: &mut FlowTransaction,
		rows: &[Row],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				// Clean up row number mappings for all left rows
				for row in rows {
					operator.cleanup_left_row_joins(txn, row.number.0)?;
				}

				// First emit all remove diffs in one batch
				let removed_joins = emit_remove_joined_rows_batch_left(
					txn,
					rows,
					&state.right,
					key_hash,
					operator,
					&operator.right_parent,
				)?;

				if !removed_joins.is_empty() {
					result.extend(removed_joins);
				} else {
					// No joined rows to remove - remove unmatched left rows
					for row in rows {
						let unmatched_row = operator.unmatched_left_row(txn, row)?;
						result.push(FlowDiff::Remove {
							pre: unmatched_row,
						});
					}
				}

				// Then remove all rows from state
				for row in rows {
					remove_from_state_entry(txn, &mut state.left, key_hash, row)?;
				}
			}
			JoinSide::Right => {
				// First emit all remove diffs in one batch
				let removed_joins = emit_remove_joined_rows_batch_right(
					txn,
					rows,
					&state.left,
					key_hash,
					operator,
					&operator.left_parent,
				)?;
				result.extend(removed_joins);

				// Check if this will make right entries empty
				let will_become_empty = if let Some(entry) = state.right.get(txn, key_hash)? {
					entry.rows.len() <= rows.len()
				} else {
					false
				};

				// Remove all rows from state
				for row in rows {
					remove_from_state_entry(txn, &mut state.right, key_hash, row)?;
				}

				// If right side became empty, re-emit left rows as unmatched
				if will_become_empty && !state.right.contains_key(txn, key_hash)? {
					let left_rows = get_left_rows(
						txn,
						&state.left,
						key_hash,
						&operator.left_parent,
						version,
					)?;
					for left_row in &left_rows {
						let unmatched_row = operator.unmatched_left_row(txn, left_row)?;
						result.push(FlowDiff::Insert {
							post: unmatched_row,
						});
					}
				}
			}
		}

		Ok(result)
	}
}
