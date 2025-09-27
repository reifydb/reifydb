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
pub(crate) struct InnerJoin;

impl InnerJoin {
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
		match side {
			JoinSide::Left => {
				// For inner join, only emit rows if there are matches
				loading.handle_left_insert(txn, post, key_hash, state, operator)
			}
			JoinSide::Right => {
				// For inner join, only emit rows if there are matches
				loading.handle_right_insert(txn, post, key_hash, state, operator)
			}
		}
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
		match side {
			JoinSide::Left => {
				// For inner join, remove all matching joined rows
				let result = loading.handle_left_remove(txn, pre, key_hash, state, operator)?;
				if key_hash.is_some() {
					operator.cleanup_left_row_joins(txn, pre.number.0)?;
				}
				Ok(result)
			}
			JoinSide::Right => {
				// For inner join, remove all matching joined rows
				loading.handle_right_remove(txn, pre, key_hash, state, operator)
			}
		}
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
					loading.handle_left_update(txn, pre, post, old_key, new_key, state, operator)
				}
				JoinSide::Right => {
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

		if let Some(key_hash) = key_hash {
			match side {
				JoinSide::Left => {
					// Add to left entries
					let serialized = SerializedRow::from_row(post);
					let mut entry =
						state.left.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
							rows: Vec::new(),
						})?;
					entry.rows.push(serialized);
					state.left.set(txn, &key_hash, &entry)?;

					// Only emit if there are matching right rows (inner join)
					if let Some(right_entry) = state.right.get(txn, &key_hash)? {
						for right_row_ser in &right_entry.rows {
							let right_row = right_row_ser.to_right_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, post, &right_row)?,
							});
						}
					}
					// No else clause - inner join doesn't emit unmatched rows
				}
				JoinSide::Right => {
					// Add to right entries
					let serialized = SerializedRow::from_row(post);
					let mut entry =
						state.right.get_or_insert_with(txn, &key_hash, || JoinSideEntry {
							rows: Vec::new(),
						})?;
					entry.rows.push(serialized);
					state.right.set(txn, &key_hash, &entry)?;

					// Only emit if there are matching left rows (inner join)
					if let Some(left_entry) = state.left.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(txn, &left_row, post)?,
							});
						}
					}
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
						}

						// Clean up empty entries
						if left_entry.rows.is_empty() {
							state.left.remove(txn, &key_hash)?;
						} else {
							state.left.set(txn, &key_hash, &left_entry)?;
						}
					}
				}
				JoinSide::Right => {
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
			if let Some(key) = old_key {
				match side {
					JoinSide::Left => {
						if let Some(mut left_entry) = state.left.get(txn, &key)? {
							// Update the row
							for row in &mut left_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.left.set(txn, &key, &left_entry)?;

							// Emit updates for all joined rows (only if right rows exist)
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
							}
						}
					}
					JoinSide::Right => {
						if let Some(mut right_entry) = state.right.get(txn, &key)? {
							// Update the row
							for row in &mut right_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.right.set(txn, &key, &right_entry)?;

							// Emit updates for all joined rows (only if left rows exist)
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
