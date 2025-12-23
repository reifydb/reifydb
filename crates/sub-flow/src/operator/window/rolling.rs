// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::{WindowSize, WindowType};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};

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

/// Apply changes for rolling windows
pub async fn apply_rolling_window(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: FlowChange,
	evaluator: &StandardRowEvaluator,
) -> crate::Result<FlowChange> {
	let mut result = Vec::new();
	let current_timestamp = operator.current_timestamp();

	// Process each incoming change
	for diff in change.diffs.iter() {
		match diff {
			FlowDiff::Insert {
				post,
			} => {
				let group_hash = operator.compute_group_key(&post, evaluator)?;

				// For rolling windows, we use a single window ID per group (always 0)
				let window_id = 0u64;
				let window_key = operator.create_window_key(group_hash, window_id);
				let mut window_state = operator.load_window_state(txn, &window_key).await?;

				// Extract timestamp for the event
				let event_timestamp = match &operator.window_type {
					WindowType::Time(_) => operator.extract_timestamp_from_row(&post)?,
					WindowType::Count => current_timestamp,
				};

				// Calculate previous aggregation BEFORE adding the new event
				let previous_aggregation = if window_state.events.len() >= operator.min_events {
					operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)
						.await?
				} else {
					None
				};

				// Add new event to window
				let event = WindowEvent::from_row(&post, event_timestamp);
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
					if let Some((aggregated_row, is_new)) = operator
						.apply_aggregations(txn, &window_key, &window_state.events, evaluator)
						.await?
					{
						if is_new {
							// First time this rolling window appears
							result.push(FlowDiff::Insert {
								post: aggregated_row,
							});
						} else {
							// Rolling window exists, emit Update with previous state
							if let Some((previous_row, _)) = previous_aggregation {
								result.push(FlowDiff::Update {
									pre: previous_row,
									post: aggregated_row,
								});
							} else {
								// Fallback to Insert if we can't get previous state
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}
					}
				}

				operator.save_window_state(txn, &window_key, &window_state)?;
			}
			FlowDiff::Update {
				pre: _,
				post,
			} => {
				// For rolling windows, updates are treated as inserts of new values
				let group_hash = operator.compute_group_key(&post, evaluator)?;
				let window_id = 0u64;
				let window_key = operator.create_window_key(group_hash, window_id);
				let mut window_state = operator.load_window_state(txn, &window_key).await?;

				let event_timestamp = match &operator.window_type {
					WindowType::Time(_) => operator.extract_timestamp_from_row(&post)?,
					WindowType::Count => current_timestamp,
				};

				// Calculate previous aggregation BEFORE adding the new event
				let previous_aggregation = if window_state.events.len() >= operator.min_events {
					operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)
						.await?
				} else {
					None
				};

				let event = WindowEvent::from_row(&post, event_timestamp);
				window_state.events.push(event);
				window_state.event_count += 1;

				if window_state.window_start == 0 {
					window_state.window_start = event_timestamp;
				}

				// Evict old events to maintain rolling window size
				operator.evict_old_events(&mut window_state, current_timestamp);

				if window_state.events.len() >= operator.min_events {
					if let Some((aggregated_row, is_new)) = operator
						.apply_aggregations(txn, &window_key, &window_state.events, evaluator)
						.await?
					{
						if is_new {
							result.push(FlowDiff::Insert {
								post: aggregated_row,
							});
						} else {
							if let Some((previous_row, _)) = previous_aggregation {
								result.push(FlowDiff::Update {
									pre: previous_row,
									post: aggregated_row,
								});
							} else {
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}
					}
				}

				operator.save_window_state(txn, &window_key, &window_state)?;
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
