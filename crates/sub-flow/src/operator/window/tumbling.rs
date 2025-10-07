// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{WindowSize, WindowType};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use super::{WindowEvent, WindowOperator};
use crate::flow::{FlowChange, FlowDiff};

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
				// For count-based windows, we use a simple incrementing window ID
				// This is a simplified implementation - real implementation would need
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

/// Apply changes for tumbling windows
pub fn apply_tumbling_window(
	operator: &WindowOperator,
	txn: &mut StandardCommandTransaction,
	change: FlowChange,
	evaluator: &StandardRowEvaluator,
) -> crate::Result<FlowChange> {
	let mut result = Vec::new();
	let current_timestamp = operator.current_timestamp();

	// First, process any expired windows
	let expired_diffs = operator.process_expired_windows(txn, current_timestamp)?;
	result.extend(expired_diffs);

	// Process each incoming change
	for diff in change.diffs.iter() {
		match diff {
			FlowDiff::Insert {
				post,
			} => {
				let group_hash = operator.compute_group_key(&post, evaluator)?;
				let (timestamp, window_id) = match &operator.window_type {
					WindowType::Time(_) => {
						let event_timestamp = operator.extract_timestamp_from_row(&post)?;
						let window_id = operator.get_tumbling_window_id(event_timestamp);
						(event_timestamp, window_id)
					}
					WindowType::Count => {
						// For count-based windows, use current processing time and calculate
						// window ID based on global event count
						let event_timestamp = operator.current_timestamp();
						let global_count =
							operator.get_and_increment_global_count(txn, group_hash)?;
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

				// Add event to window
				let event = WindowEvent::from_row(&post, timestamp);
				window_state.events.push(event);
				window_state.event_count += 1;

				if window_state.window_start == 0 {
					// Set window start aligned to window boundary for tumbling windows
					window_state.window_start = operator.set_tumbling_window_start(timestamp);
				}

				// Always emit result for count-based windows - Insert for first, Update for subsequent
				if let Some((aggregated_row, is_new)) =
					operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)?
				{
					if is_new {
						// First time we see this window - emit Insert
						result.push(FlowDiff::Insert {
							post: aggregated_row,
						});
					} else {
						// Window already exists - emit Update
						// We need to compute the previous aggregation (without the current
						// event)
						let previous_events =
							&window_state.events[..window_state.events.len() - 1];
						if let Some((previous_aggregated, _)) = operator.apply_aggregations(
							txn,
							&window_key,
							previous_events,
							evaluator,
						)? {
							result.push(FlowDiff::Update {
								pre: previous_aggregated,
								post: aggregated_row,
							});
						}
					}
				}

				operator.save_window_state(txn, &window_key, &window_state)?;
			}
			FlowDiff::Update {
				pre: _,
				post,
			} => {
				// For windows, updates are treated as insert of new value
				// Real implementation might need to handle retractions
				let group_hash = operator.compute_group_key(&post, evaluator)?;
				let (event_timestamp, window_id) = match &operator.window_type {
					WindowType::Time(_) => {
						let event_timestamp = operator.extract_timestamp_from_row(&post)?;
						let window_id = operator.get_tumbling_window_id(event_timestamp);
						(event_timestamp, window_id)
					}
					WindowType::Count => {
						// For count-based windows, use current processing time and calculate
						// window ID based on global event count
						let event_timestamp = operator.current_timestamp();
						let global_count =
							operator.get_and_increment_global_count(txn, group_hash)?;
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

				let event = WindowEvent::from_row(&post, event_timestamp);
				window_state.events.push(event);
				window_state.event_count += 1;

				if window_state.window_start == 0 {
					window_state.window_start = operator.set_tumbling_window_start(event_timestamp);
				}

				// Always emit result for count-based windows - Insert for first, Update for subsequent
				if let Some((aggregated_row, is_new)) =
					operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)?
				{
					if is_new {
						// First time we see this window - emit Insert
						result.push(FlowDiff::Insert {
							post: aggregated_row,
						});
					} else {
						// Window already exists - emit Update
						// We need to compute the previous aggregation (without the current
						// event)
						let previous_events =
							&window_state.events[..window_state.events.len() - 1];
						if let Some((previous_aggregated, _)) = operator.apply_aggregations(
							txn,
							&window_key,
							previous_events,
							evaluator,
						)? {
							result.push(FlowDiff::Update {
								pre: previous_aggregated,
								post: aggregated_row,
							});
						}
					}
				}

				operator.save_window_state(txn, &window_key, &window_state)?;
			}
			FlowDiff::Remove {
				pre: _,
			} => {
				// Window operators typically don't handle removes in streaming scenarios
				// This would require complex retraction logic
			}
		}
	}

	Ok(FlowChange::internal(operator.node, change.version, result))
}
