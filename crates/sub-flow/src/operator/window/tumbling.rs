// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use reifydb_core::{
	common::{WindowSize, WindowType},
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;

use super::{WindowEvent, WindowOperator};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Determine which window an event belongs to for tumbling windows
	pub fn get_tumbling_window_id(&self, timestamp: u64) -> u64 {
		match (&self.window_type, &self.size) {
			(WindowType::Time(_), WindowSize::Duration(duration)) => {
				let window_size_ms = duration.as_millis() as u64;
				// Tumbling window - align to window boundaries from epoch
				let window_start = (timestamp / window_size_ms) * window_size_ms;
				window_start / window_size_ms
			}
			(WindowType::Count, WindowSize::Count(count)) => {
				// to track event counts per group
				timestamp / *count
			}
			_ => {
				// Mismatched window type and size
				0
			}
		}
	}

	/// Set window start time for tumbling windows (aligned to window boundaries)
	pub fn set_tumbling_window_start(&self, timestamp: u64) -> u64 {
		match &self.size {
			WindowSize::Duration(duration) => {
				let window_size_ms = duration.as_millis() as u64;
				(timestamp / window_size_ms) * window_size_ms
			}
			_ => timestamp,
		}
	}

	/// Check if tumbling window should be moved to a new window due to time boundaries
	pub fn should_start_new_tumbling_window(&self, current_window_start: u64, event_timestamp: u64) -> bool {
		match &self.size {
			WindowSize::Duration(duration) => {
				let window_size_ms = duration.as_millis() as u64;
				let event_window_start = (event_timestamp / window_size_ms) * window_size_ms;
				event_window_start != current_window_start
			}
			_ => false,
		}
	}

	/// Check if a tumbling window should be expired (closed)
	pub fn should_expire_tumbling_window(&self, state: &super::WindowState, current_timestamp: u64) -> bool {
		match (&self.window_type, &self.size, &self.slide) {
			(WindowType::Time(_), WindowSize::Duration(duration), None) => {
				let window_size_ms = duration.as_millis() as u64;
				let expire_time = state.window_start + window_size_ms;
				current_timestamp >= expire_time
			}
			_ => false,
		}
	}
}

/// Process inserts for tumbling windows
fn process_tumbling_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
) -> reifydb_type::Result<Vec<Diff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	let group_hashes = operator.compute_group_keys(columns)?;

	let groups = columns.partition_by_keys(&group_hashes);

	for (group_hash, group_columns) in groups {
		let group_result = process_tumbling_group_insert(operator, txn, &group_columns, group_hash)?;
		result.extend(group_result);
	}

	Ok(result)
}

/// Process inserts for a single group in tumbling windows
fn process_tumbling_group_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	group_hash: Hash128,
) -> reifydb_type::Result<Vec<Diff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	let timestamps = operator.extract_timestamps(columns)?;

	for row_idx in 0..row_count {
		let timestamp = timestamps[row_idx];
		let (event_timestamp, window_id) = match &operator.window_type {
			WindowType::Time(_) => {
				let window_id = operator.get_tumbling_window_id(timestamp);
				(timestamp, window_id)
			}
			WindowType::Count => {
				// window ID based on global event count
				let event_timestamp = operator.current_timestamp();
				let global_count = operator.get_and_increment_global_count(txn, group_hash)?;
				let window_size = if let WindowSize::Count(count) = &operator.size {
					*count
				} else {
					3 // fallback
				};
				let window_id = global_count / window_size;
				(event_timestamp, window_id)
			}
		};

		let window_key = operator.create_window_key(group_hash, window_id);
		let mut window_state = operator.load_window_state(txn, &window_key)?;

		let single_row_columns = columns.extract_row(row_idx);
		let row = single_row_columns.to_single_row();

		let event = WindowEvent::from_row(&row, event_timestamp);
		window_state.events.push(event);
		window_state.event_count += 1;

		if window_state.window_start == 0 {
			window_state.window_start = operator.set_tumbling_window_start(event_timestamp);
		}

		// Always emit result for count-based windows - Insert for first, Update for subsequent
		if let Some((aggregated_row, is_new)) =
			operator.apply_aggregations(txn, &window_key, &window_state.events)?
		{
			if is_new {
				result.push(Diff::Insert {
					post: Columns::from_row(&aggregated_row),
				});
			} else {
				// Window already exists - emit Update

				let previous_events = &window_state.events[..window_state.events.len() - 1];
				if let Some((previous_aggregated, _)) =
					operator.apply_aggregations(txn, &window_key, previous_events)?
				{
					result.push(Diff::Update {
						pre: Columns::from_row(&previous_aggregated),
						post: Columns::from_row(&aggregated_row),
					});
				}
			}
		}

		operator.save_window_state(txn, &window_key, &window_state)?;
	}

	Ok(result)
}

/// Apply changes for tumbling windows
pub fn apply_tumbling_window(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: Change,
) -> reifydb_type::Result<Change> {
	let mut result = Vec::new();
	let current_timestamp = operator.current_timestamp();

	// First, process any expired windows
	let expired_diffs = operator.process_expired_windows(txn, current_timestamp)?;
	result.extend(expired_diffs);

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
			} => {
				let insert_result = process_tumbling_insert(operator, txn, post)?;
				result.extend(insert_result);
			}
			Diff::Update {
				pre: _,
				post,
			} => {
				let update_result = process_tumbling_insert(operator, txn, post)?;
				result.extend(update_result);
			}
			Diff::Remove {
				pre: _,
			} => {
				// Window operators typically don't handle removes in streaming scenarios
				// This would require complex retraction logic
			}
		}
	}

	Ok(Change::from_flow(operator.node, change.version, result))
}
