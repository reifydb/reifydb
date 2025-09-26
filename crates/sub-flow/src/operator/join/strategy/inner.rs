use reifydb_core::{flow::FlowDiff, interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::operator::join::{JoinSide, JoinSideEntry, JoinState, SerializedRow, operator::JoinOperator};

#[derive(Debug, Clone)]
pub(crate) struct InnerJoin;

impl InnerJoin {
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
					let mut entry = state.left_store.get_or_insert_with(txn, &key_hash, || {
						JoinSideEntry {
							rows: Vec::new(),
						}
					})?;
					entry.rows.push(serialized);
					state.left_store.set(txn, &key_hash, &entry)?;

					// Only emit if there are matching right rows (inner join)
					if let Some(right_entry) = state.right_store.get(txn, &key_hash)? {
						for right_row_ser in &right_entry.rows {
							let right_row = right_row_ser.to_right_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(post, &right_row),
							});
						}
					}
					// No else clause - inner join doesn't emit unmatched rows
				}
				JoinSide::Right => {
					// Add to right entries
					let serialized = SerializedRow::from_row(post);
					let mut entry = state.right_store.get_or_insert_with(txn, &key_hash, || {
						JoinSideEntry {
							rows: Vec::new(),
						}
					})?;
					entry.rows.push(serialized);
					state.right_store.set(txn, &key_hash, &entry)?;

					// Only emit if there are matching left rows (inner join)
					if let Some(left_entry) = state.left_store.get(txn, &key_hash)? {
						for left_row_ser in &left_entry.rows {
							let left_row = left_row_ser.to_left_row(&state.schema);

							result.push(FlowDiff::Insert {
								post: operator.join_rows(&left_row, post),
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
					if let Some(mut left_entry) = state.left_store.get(txn, &key_hash)? {
						left_entry.rows.retain(|r| r.number != pre.number);

						// Remove all joins involving this row
						if let Some(right_entry) = state.right_store.get(txn, &key_hash)? {
							for right_row_ser in &right_entry.rows {
								let right_row =
									right_row_ser.to_right_row(&state.schema);

								result.push(FlowDiff::Remove {
									pre: operator.join_rows(pre, &right_row),
								});
							}
						}

						// Clean up empty entries
						if left_entry.rows.is_empty() {
							state.left_store.remove(txn, &key_hash)?;
						} else {
							state.left_store.set(txn, &key_hash, &left_entry)?;
						}
					}
				}
				JoinSide::Right => {
					if let Some(mut right_entry) = state.right_store.get(txn, &key_hash)? {
						right_entry.rows.retain(|r| r.number != pre.number);

						// Remove all joins involving this row
						if let Some(left_entry) = state.left_store.get(txn, &key_hash)? {
							for left_row_ser in &left_entry.rows {
								let left_row = left_row_ser.to_left_row(&state.schema);

								result.push(FlowDiff::Remove {
									pre: operator.join_rows(&left_row, pre),
								});
							}
						}

						// Clean up empty entries
						if right_entry.rows.is_empty() {
							state.right_store.remove(txn, &key_hash)?;
						} else {
							state.right_store.set(txn, &key_hash, &right_entry)?;
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
						if let Some(mut left_entry) = state.left_store.get(txn, &key)? {
							// Update the row
							for row in &mut left_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.left_store.set(txn, &key, &left_entry)?;

							// Emit updates for all joined rows (only if right rows exist)
							if let Some(right_entry) = state.right_store.get(txn, &key)? {
								for right_row_ser in &right_entry.rows {
									let right_row = right_row_ser
										.to_right_row(&state.schema);

									result.push(FlowDiff::Update {
										pre: operator
											.join_rows(pre, &right_row),
										post: operator
											.join_rows(post, &right_row),
									});
								}
							}
						}
					}
					JoinSide::Right => {
						if let Some(mut right_entry) = state.right_store.get(txn, &key)? {
							// Update the row
							for row in &mut right_entry.rows {
								if row.number == pre.number {
									*row = SerializedRow::from_row(post);
									break;
								}
							}
							state.right_store.set(txn, &key, &right_entry)?;

							// Emit updates for all joined rows (only if left rows exist)
							if let Some(left_entry) = state.left_store.get(txn, &key)? {
								for left_row_ser in &left_entry.rows {
									let left_row =
										left_row_ser.to_left_row(&state.schema);

									result.push(FlowDiff::Update {
										pre: operator.join_rows(&left_row, pre),
										post: operator
											.join_rows(&left_row, post),
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
