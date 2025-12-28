// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::{WindowSize, WindowType, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_hash::Hash128;
use reifydb_sdk::{FlowChange, FlowDiff};

use super::{WindowEvent, WindowOperator};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Check if rolling window should evict old events
	pub fn should_evict_rolling_window(&self, state: &super::WindowState, current_timestamp: u64) -> bool {
		match (&self.window_type, &self.size) {
			(WindowType::Time(_), WindowSize::Duration(duration)) => {
				if state.events.is_empty() {
					return false;
				}
				let window_size_ms = duration.as_millis() as u64;
				let oldest_timestamp = state.events[0].timestamp;
				current_timestamp - oldest_timestamp > window_size_ms
			}
			(WindowType::Count, WindowSize::Count(count)) => state.event_count > *count,
			_ => false,
		}
	}

	/// Evict old events from rolling window to maintain size limit
	pub fn evict_old_events(&self, state: &mut super::WindowState, current_timestamp: u64) {
		match (&self.window_type, &self.size) {
			(WindowType::Time(_), WindowSize::Duration(duration)) => {
				let window_size_ms = duration.as_millis() as u64;
				let cutoff_time = current_timestamp - window_size_ms;

				// Remove events older than the window size
				let original_len = state.events.len();
				state.events.retain(|event| event.timestamp > cutoff_time);
				let evicted_count = original_len - state.events.len();
				state.event_count = state.event_count.saturating_sub(evicted_count as u64);
			}
			(WindowType::Count, WindowSize::Count(count)) => {
				// Keep only the most recent 'count' events
				if state.events.len() > *count as usize {
					let excess = state.events.len() - *count as usize;
					state.events.drain(0..excess);
					state.event_count = *count;
				}
			}
			_ => {}
		}
	}
}

/// Process inserts for rolling windows
async fn process_rolling_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	evaluator: &StandardColumnEvaluator,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	let current_timestamp = operator.current_timestamp();

	// Batch compute group hashes for all rows
	let group_hashes = operator.compute_group_keys(columns)?;

	// Partition columns by group hash
	let groups = columns.partition_by_keys(&group_hashes);

	// Process each group
	for (group_hash, group_columns) in groups {
		let group_result = process_rolling_group_insert(
			operator,
			txn,
			&group_columns,
			group_hash,
			current_timestamp,
			evaluator,
		)
		.await?;
		result.extend(group_result);
	}

	Ok(result)
}

/// Process inserts for a single group in rolling windows
async fn process_rolling_group_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	group_hash: Hash128,
	current_timestamp: u64,
	evaluator: &StandardColumnEvaluator,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	// Extract timestamps for all rows in this group
	let timestamps = operator.extract_timestamps(columns)?;

	// For rolling windows, we use a single window ID per group (always 0)
	let window_id = 0u64;
	let window_key = operator.create_window_key(group_hash, window_id);
	let mut window_state = operator.load_window_state(txn, &window_key).await?;

	// Process each row
	for row_idx in 0..row_count {
		let event_timestamp = timestamps[row_idx];

		// Calculate previous aggregation BEFORE adding the new event
		let previous_aggregation = if window_state.events.len() >= operator.min_events {
			operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator).await?
		} else {
			None
		};

		// Extract this single row and convert to Row for WindowEvent storage
		let single_row_columns = columns.extract_row(row_idx);
		let row = single_row_columns.to_single_row();

		// Add new event to window
		let event = WindowEvent::from_row(&row, event_timestamp);
		window_state.events.push(event);
		window_state.event_count += 1;

		// Set window start if this is the first event
		if window_state.window_start == 0 {
			window_state.window_start = event_timestamp;
		}

		// Evict old events to maintain rolling window size
		operator.evict_old_events(&mut window_state, current_timestamp);

		// Always trigger rolling windows (they continuously update)
		if window_state.events.len() >= operator.min_events {
			if let Some((aggregated_row, is_new)) =
				operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator).await?
			{
				if is_new {
					// First time this rolling window appears
					result.push(FlowDiff::Insert {
						post: Columns::from_row(&aggregated_row),
					});
				} else {
					// Rolling window exists, emit Update with previous state
					if let Some((previous_row, _)) = previous_aggregation {
						result.push(FlowDiff::Update {
							pre: Columns::from_row(&previous_row),
							post: Columns::from_row(&aggregated_row),
						});
					} else {
						// Fallback to Insert if we can't get previous state
						result.push(FlowDiff::Insert {
							post: Columns::from_row(&aggregated_row),
						});
					}
				}
			}
		}
	}

	// Save window state once after processing all rows in the group
	operator.save_window_state(txn, &window_key, &window_state)?;

	Ok(result)
}

/// Apply changes for rolling windows
pub async fn apply_rolling_window(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: FlowChange,
	evaluator: &StandardColumnEvaluator,
) -> crate::Result<FlowChange> {
	let mut result = Vec::new();

	// Process each incoming change (each diff may contain multiple rows)
	for diff in change.diffs.iter() {
		match diff {
			FlowDiff::Insert {
				post,
			} => {
				let insert_result = process_rolling_insert(operator, txn, post, evaluator).await?;
				result.extend(insert_result);
			}
			FlowDiff::Update {
				pre: _,
				post,
			} => {
				// For rolling windows, updates are treated as inserts of new values
				let update_result = process_rolling_insert(operator, txn, post, evaluator).await?;
				result.extend(update_result);
			}
			FlowDiff::Remove {
				pre: _,
			} => {
				// Rolling windows typically don't handle removes in streaming scenarios
				// This would require complex retraction logic
			}
		}
	}

	Ok(FlowChange::internal(operator.node, change.version, result))
}
