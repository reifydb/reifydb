// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{WindowSize, WindowSlide, WindowType};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use super::{WindowEvent, WindowOperator};
use crate::flow::{FlowChange, FlowDiff};

impl WindowOperator {
	/// Determine which windows an event belongs to for sliding windows
	pub fn get_sliding_window_ids(&self, row_index: u64) -> Vec<u64> {
		match (&self.window_type, &self.size, &self.slide) {
			(
				WindowType::Time(_),
				WindowSize::Duration(duration),
				Some(WindowSlide::Duration(slide_duration)),
			) => {
				let window_size_ms = duration.as_millis() as u64;
				let slide_ms = slide_duration.as_millis() as u64;
				let base_window = row_index / window_size_ms;

				if slide_ms >= window_size_ms {
					// Non-overlapping windows
					vec![base_window]
				} else {
					// Overlapping windows - event belongs to multiple windows
					let mut windows = Vec::new();
					let num_windows = window_size_ms / slide_ms;
					for i in 0..num_windows {
						let window_start = base_window.saturating_sub(i);
						if row_index >= window_start * window_size_ms {
							windows.push(window_start);
						}
					}
					windows
				}
			}
			(WindowType::Time(_), WindowSize::Duration(duration), Some(WindowSlide::Count(_))) => {
				// Time windows with count-based slide not supported yet
				let window_size_ms = duration.as_millis() as u64;
				let base_window = row_index / window_size_ms;
				vec![base_window]
			}
			(WindowType::Count, WindowSize::Count(count), Some(WindowSlide::Count(slide_count))) => {
				// Count-based sliding windows
				// For count=3, slide=2:
				// Window 0: rows 1,2,3 (row indices 0,1,2)
				// Window 1: rows 3,4,5 (row indices 2,3,4)
				// Window 2: rows 5,6,7 (row indices 4,5,6)

				let row_idx = row_index.saturating_sub(1); // Convert to 0-based index
				let mut windows = Vec::new();

				// A row at 0-based position N belongs to window W if:
				// W * slide <= N < W * slide + count
				// Rearranging: (N - count + 1) / slide <= W <= N / slide

				if row_idx < *count {
					// Early rows may not fill enough windows yet
					let max_window = row_idx / *slide_count;
					for window_id in 0..=max_window {
						let window_start = window_id * *slide_count;
						let window_end = window_start + *count;

						if row_idx >= window_start && row_idx < window_end {
							windows.push(window_id);
						}
					}
				} else {
					// For later rows, calculate the range of windows this row belongs to
					let min_window = (row_idx + 1).saturating_sub(*count) / *slide_count;
					let max_window = row_idx / *slide_count;

					for window_id in min_window..=max_window {
						let window_start = window_id * *slide_count;
						let window_end = window_start + *count;

						if row_idx >= window_start && row_idx < window_end {
							windows.push(window_id);
						}
					}
				}

				windows
			}
			_ => {
				// Fallback for unsupported combinations
				vec![0]
			}
		}
	}

	/// Set window start time for sliding windows (use event timestamp)
	pub fn set_sliding_window_start(&self, timestamp: u64) -> u64 {
		timestamp
	}
}

/// Apply changes for sliding windows
pub fn apply_sliding_window(
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
				let (timestamp, window_ids) = match &operator.window_type {
					WindowType::Time(_) => {
						let event_timestamp = operator.extract_timestamp_from_row(&post)?;
						let window_ids = operator.get_sliding_window_ids(event_timestamp);
						(event_timestamp, window_ids)
					}
					WindowType::Count => {
						// For count-based windows, use current processing time and simple
						// window ID
						let event_timestamp = operator.current_timestamp();
						let window_ids = vec![0]; // Simple count-based window ID
						(event_timestamp, window_ids)
					}
				};

				for window_id in window_ids {
					let window_key = operator.create_window_key(group_hash, window_id);
					let mut window_state = operator.load_window_state(txn, &window_key)?;

					// Add event to window
					let event = WindowEvent::from_row(&post, timestamp);
					window_state.events.push(event);
					window_state.event_count += 1;

					if window_state.window_start == 0 {
						// Set window start using event timestamp for sliding windows
						window_state.window_start =
							operator.set_sliding_window_start(timestamp);
					}

					// Check if window should be triggered (get fresh timestamp for each check)
					let trigger_check_time = operator.current_timestamp();
					let should_trigger =
						operator.should_trigger_window(&window_state, trigger_check_time);

					if should_trigger {
						window_state.is_triggered = true;
						// Apply aggregations and emit result
						if let Some((aggregated_row, is_new)) = operator.apply_aggregations(
							txn,
							&window_key,
							&window_state.events,
							evaluator,
						)? {
							if is_new {
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							} else {
								// This shouldn't happen for sliding windows with
								// is_triggered logic
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}
					}

					operator.save_window_state(txn, &window_key, &window_state)?;
				}
			}
			FlowDiff::Update {
				pre: _,
				post,
			} => {
				// For windows, updates are treated as insert of new value
				// Real implementation might need to handle retractions
				let group_hash = operator.compute_group_key(&post, evaluator)?;
				let (event_timestamp, window_ids) = match &operator.window_type {
					WindowType::Time(_) => {
						let event_timestamp = operator.extract_timestamp_from_row(&post)?;
						let window_ids = operator.get_sliding_window_ids(event_timestamp);
						(event_timestamp, window_ids)
					}
					WindowType::Count => {
						// For count-based windows, use current processing time and simple
						// window ID
						let event_timestamp = operator.current_timestamp();
						let window_ids = vec![0]; // Simple count-based window ID
						(event_timestamp, window_ids)
					}
				};

				for window_id in window_ids {
					let window_key = operator.create_window_key(group_hash, window_id);
					let mut window_state = operator.load_window_state(txn, &window_key)?;

					let event = WindowEvent::from_row(&post, event_timestamp);
					window_state.events.push(event);
					window_state.event_count += 1;

					if window_state.window_start == 0 {
						window_state.window_start =
							operator.set_sliding_window_start(event_timestamp);
					}

					let trigger_check_time = operator.current_timestamp();
					if operator.should_trigger_window(&window_state, trigger_check_time) {
						window_state.is_triggered = true;

						if let Some((aggregated_row, is_new)) = operator.apply_aggregations(
							txn,
							&window_key,
							&window_state.events,
							evaluator,
						)? {
							if is_new {
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							} else {
								// This shouldn't happen for sliding windows with
								// is_triggered logic
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}
					}

					operator.save_window_state(txn, &window_key, &window_state)?;
				}
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
