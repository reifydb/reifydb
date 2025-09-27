use reifydb_core::{flow::FlowDiff, interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::operator::join::{
	JoinSide, JoinSideEntry, JoinState, SerializedRow,
	loading::{EagerLoading, LazyLoading},
	operator::JoinOperator,
};

// Trait for loading strategy operations
trait LoadingOps {
	fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;

	fn handle_right_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;

	fn handle_left_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;

	fn handle_right_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;

	fn handle_left_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;

	fn handle_right_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>>;
}

impl LoadingOps for EagerLoading {
	fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_insert(txn, post, key_hash, state, operator)
	}

	fn handle_right_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_insert(txn, post, key_hash, state, operator)
	}

	fn handle_left_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_remove(txn, pre, key_hash, state, operator)
	}

	fn handle_right_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_remove(txn, pre, key_hash, state, operator)
	}

	fn handle_left_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_update(txn, pre, post, old_key, new_key, state, operator)
	}

	fn handle_right_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_update(txn, pre, post, old_key, new_key, state, operator)
	}
}

impl LoadingOps for LazyLoading {
	fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_insert(txn, post, key_hash, state, operator)
	}

	fn handle_right_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_insert(txn, post, key_hash, state, operator)
	}

	fn handle_left_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_remove(txn, pre, key_hash, state, operator)
	}

	fn handle_right_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_remove(txn, pre, key_hash, state, operator)
	}

	fn handle_left_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_left_update(txn, pre, post, old_key, new_key, state, operator)
	}

	fn handle_right_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		self.handle_right_update(txn, pre, post, old_key, new_key, state, operator)
	}
}

#[derive(Debug, Clone)]
pub(crate) struct LeftJoin;

impl LeftJoin {
	pub(crate) fn handle_insert_with_loading<T: Transaction, L>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		loading: &L,
	) -> crate::Result<Vec<FlowDiff>>
	where
		L: LoadingOps,
	{
		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				// Use loading strategy to handle the insert
				let joined_rows = loading.handle_left_insert(txn, post, key_hash, state, operator)?;

				// For left join: if no matches found, emit unmatched row
				if joined_rows.is_empty() && key_hash.is_some() {
					result.push(FlowDiff::Insert {
						post: operator.unmatched_left_row(txn, post)?,
					});
				} else if joined_rows.is_empty() && key_hash.is_none() {
					// Undefined key in left join still emits the row
					result.push(FlowDiff::Insert {
						post: operator.unmatched_left_row(txn, post)?,
					});
				} else {
					result.extend(joined_rows);
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Check if this is the first right row for this key
					let is_first_right_row = !state.right.contains_key(txn, &key_hash)?;

					// Use loading strategy to handle the insert
					let joined_rows = loading.handle_right_insert(
						txn,
						post,
						Some(key_hash),
						state,
						operator,
					)?;
					result.extend(joined_rows);

					// If first right row for this key, remove previously emitted unmatched left
					// rows
					if is_first_right_row {
						if let Some(left_entry) = state.left.get(txn, &key_hash)? {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);
								let unmatched_row =
									operator.unmatched_left_row(txn, &left_row)?;
								result.push(FlowDiff::Remove {
									pre: unmatched_row,
								});
							}
						}
					}
				}
				// Right side inserts with undefined keys don't produce output
			}
		}

		Ok(result)
	}

	pub(crate) fn handle_remove_with_loading<T: Transaction, L>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		loading: &L,
	) -> crate::Result<Vec<FlowDiff>>
	where
		L: LoadingOps,
	{
		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				if let Some(key_hash) = key_hash {
					// Check if there are right rows before removing
					let has_right_rows = state.right.contains_key(txn, &key_hash)?;

					// Use loading strategy to handle the remove
					let removed_joins =
						loading.handle_left_remove(txn, pre, Some(key_hash), state, operator)?;

					if has_right_rows {
						// There were matched rows to remove
						result.extend(removed_joins);
					} else {
						// No right rows, so remove the unmatched left row
						result.push(FlowDiff::Remove {
							pre: operator.unmatched_left_row(txn, pre)?,
						});
					}

					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				} else {
					// Undefined key - remove the unmatched row
					result.push(FlowDiff::Remove {
						pre: operator.unmatched_left_row(txn, pre)?,
					});
					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				}
			}
			JoinSide::Right => {
				if let Some(key_hash) = key_hash {
					// Get the right entry to check if this is the last row
					let is_last_right_row = if let Some(right_entry) =
						state.right.get(txn, &key_hash)?
					{
						right_entry.rows.len() == 1 && right_entry.rows[0].number == pre.number
					} else {
						false
					};

					// Use loading strategy to handle the remove
					let removed_joins =
						loading.handle_right_remove(txn, pre, Some(key_hash), state, operator)?;
					result.extend(removed_joins);

					// If this was the last right row, re-emit left rows as unmatched
					if is_last_right_row {
						if let Some(left_entry) = state.left.get(txn, &key_hash)? {
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

	pub(crate) fn handle_update_with_loading<T: Transaction, L>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		side: JoinSide,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		loading: &L,
	) -> crate::Result<Vec<FlowDiff>>
	where
		L: LoadingOps,
	{
		if old_key == new_key {
			// Key didn't change, handle in-place update
			match side {
				JoinSide::Left => {
					if let Some(key_hash) = old_key {
						// Check if there are right rows
						let has_right_rows = state.right.contains_key(txn, &key_hash)?;

						// Use loading strategy to handle the update
						let updated_joins = loading.handle_left_update(
							txn, pre, post, old_key, new_key, state, operator,
						)?;

						if has_right_rows {
							// There are matched rows to update
							Ok(updated_joins)
						} else {
							// No right rows, so update the unmatched left row
							Ok(vec![FlowDiff::Update {
								pre: operator.unmatched_left_row(txn, pre)?,
								post: operator.unmatched_left_row(txn, post)?,
							}])
						}
					} else {
						// Both keys are undefined - update the unmatched row
						Ok(vec![FlowDiff::Update {
							pre: operator.unmatched_left_row(txn, pre)?,
							post: operator.unmatched_left_row(txn, post)?,
						}])
					}
				}
				JoinSide::Right => {
					// Use loading strategy to handle the update
					loading.handle_right_update(txn, pre, post, old_key, new_key, state, operator)
				}
			}
		} else {
			// Key changed - treat as remove + insert
			let mut result = Vec::new();
			result.extend(
				self.handle_remove_with_loading(txn, pre, side, old_key, state, operator, loading)?
			);
			result.extend(
				self.handle_insert_with_loading(txn, post, side, new_key, state, operator, loading)?
			);
			Ok(result)
		}
	}

	// Keep the old methods for now but mark them as deprecated
	#[deprecated(note = "Use handle_insert_with_loading instead")]
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

					// Join with matching right rows
					if let Some(right_entry) = state.right.get(txn, &key_hash)? {
						for right_row_ser in &right_entry.rows {
							let right_row = right_row_ser.to_right_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, post, &right_row)?,
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
					let is_first_right_row = !state.right.contains_key(txn, &key_hash)?;

					// Add to right entries
					let serialized = SerializedRow::from_row(post);
					let mut entry =
						state.right.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
							rows: Vec::new(),
						})?;
					entry.rows.push(serialized);
					state.right.set(txn, &key_hash, &entry)?;

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
						if let Some(right_entry) = state.right.get(txn, &key_hash)? {
							for right_row_ser in &right_entry.rows {
								let right_row =
									right_row_ser.to_right_row(&state.schema);

								result.push(FlowDiff::Remove {
									pre: operator
										.join_rows(txn, pre, &right_row)?,
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
					// Remove from right entries
					if let Some(mut right_entry) = state.right.get(txn, &key_hash)? {
						right_entry.rows.retain(|r| r.number != pre.number);

						// Remove all joins involving this row
						if let Some(left_entry) = state.left.get(txn, &key_hash)? {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);

								result.push(FlowDiff::Remove {
									pre: operator.join_rows(txn, &left_row, pre)?,
								});
							}

							// If this was the last right row, re-emit left rows as
							// unmatched
							if right_entry.rows.is_empty() {
								for left_row_ser in &left_entry.rows {
									let left_row =
										left_row_ser.to_left_row(&state.schema);
									let unmatched_row = operator
										.unmatched_left_row(txn, &left_row)?;
									result.push(FlowDiff::Insert {
										post: unmatched_row,
									});
								}
							}
						}

						// Clean up empty entries
						if right_entry.rows.is_empty() {
							state.right.remove(txn, &key_hash)?;
						} else {
							state.right.set(txn, &key_hash, &right_entry)?;
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
							if let Some(right_entry) = state.right.get(txn, &key)? {
								for right_row_ser in &right_entry.rows {
									let right_row = right_row_ser
										.to_right_row(&state.schema);

									result.push(FlowDiff::Update {
										pre: operator.join_rows(
											txn, pre, &right_row,
										)?,
										post: operator.join_rows(
											txn, post, &right_row,
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
						if let Some(mut right_entry) = state.right.get(txn, &key)? {
							// Update the row
							for row in &mut right_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.right.set(txn, &key, &right_entry)?;

							// Emit updates for all joined rows
							if let Some(left_entry) = state.left.get(txn, &key)? {
								for left_row_ser in &left_entry.rows {
									let left_row =
										left_row_ser.to_left_row(&state.schema);

									result.push(FlowDiff::Update {
										pre: operator.join_rows(
											txn, &left_row, pre,
										)?,
										post: operator.join_rows(
											txn, &left_row, post,
										)?,
									});
								}
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
