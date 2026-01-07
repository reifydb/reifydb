// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use reifydb_core::{WindowSize, WindowSlide, WindowType, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_hash::Hash128;
use reifydb_sdk::{FlowChange, FlowDiff};

use super::{WindowEvent, WindowOperator};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Determine which windows an event belongs to for sliding windows
	/// For time-based windows, pass the event timestamp
	/// For count-based windows, pass the row index (0-based)
	pub fn get_sliding_window_ids(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		match (&self.window_type, &self.size, &self.slide) {
			(
				WindowType::Time(_),
				WindowSize::Duration(duration),
				Some(WindowSlide::Duration(slide_duration)),
			) => {
				let window_size_ms = duration.as_millis() as u64;
				let slide_ms = slide_duration.as_millis() as u64;

				let timestamp = timestamp_or_row_index;

				if slide_ms >= window_size_ms {
					// Non-overlapping windows - each event belongs to exactly one window
					let window_id = timestamp / slide_ms;
					vec![window_id]
				} else {
					// Overlapping windows - event belongs to multiple windows
					let mut windows = Vec::new();

					// For sliding windows, we need to find all windows that contain this timestamp
					// A window with ID w starts at w * slide_ms and ends at w * slide_ms +
					// window_size_ms So timestamp T is in window w if: w * slide_ms <= T < w *
					// slide_ms + window_size_ms Rearranging: (T - window_size_ms + 1) /
					// slide_ms <= w <= T / slide_ms

					let min_window_id = if timestamp >= window_size_ms {
						(timestamp - window_size_ms + 1) / slide_ms
					} else {
						0
					};
					let max_window_id = timestamp / slide_ms;

					for window_id in min_window_id..=max_window_id {
						let window_start = window_id * slide_ms;
						let window_end = window_start + window_size_ms;

						if timestamp >= window_start && timestamp < window_end {
							windows.push(window_id);
						}
					}
					windows
				}
			}
			(WindowType::Time(_), WindowSize::Duration(duration), Some(WindowSlide::Count(_))) => {
				// Time windows with count-based slide not supported yet
				let window_size_ms = duration.as_millis() as u64;
				let timestamp = timestamp_or_row_index;
				let base_window = timestamp / window_size_ms;
				vec![base_window]
			}
			(WindowType::Count, WindowSize::Count(count), Some(WindowSlide::Count(slide_count))) => {
				// Count-based sliding windows
				// For count=3, slide=2 with 1-based row numbering:
				// Window 0: rows 1,2,3 (global_count 0,1,2)
				// Window 1: rows 3,4,5 (global_count 2,3,4)
				// Window 2: rows 5,6,7 (global_count 4,5,6)

				let global_count = timestamp_or_row_index; // 0-based global count from get_and_increment_global_count
				let mut windows = Vec::new();

				// Convert to 1-based row number for window calculations
				let row_number = global_count + 1; // 1-based row number as expected by test

				// A row N (1-based) belongs to window W if:
				// W * slide_count + 1 <= N <= W * slide_count + count
				// Rearranging: (N - count) / slide_count <= W <= (N - 1) / slide_count

				// Find the range of windows this row belongs to
				// Mathematical definition: row N belongs to window W if:
				// W * slide_count <= N-1 < W * slide_count + count (using 0-based indexing)
				// Converting to 1-based: W * slide_count + 1 <= N <= W * slide_count + count
				let min_window = if row_number > *count {
					(row_number - *count) / *slide_count
				} else {
					0
				};
				let max_window = (row_number - 1) / *slide_count;

				for window_id in min_window..=max_window {
					let window_start_row = window_id * *slide_count + 1; // 1-based
					let window_end_row = window_start_row + *count - 1; // 1-based, inclusive

					// Standard sliding window membership check
					let belongs_to_window =
						row_number >= window_start_row && row_number <= window_end_row;

					if belongs_to_window {
						windows.push(window_id);
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

	/// Set window start time for sliding windows (aligned to slide boundaries)
	pub fn set_sliding_window_start(&self, timestamp: u64, window_id: u64) -> u64 {
		match (&self.window_type, &self.size, &self.slide) {
			(WindowType::Time(_), WindowSize::Duration(_), Some(WindowSlide::Duration(slide_duration))) => {
				// For sliding windows, window start is aligned to slide boundaries
				let slide_ms = slide_duration.as_millis() as u64;
				let window_start = window_id * slide_ms;
				window_start
			}
			_ => {
				// Fallback: use timestamp as-is
				timestamp
			}
		}
	}
}

/// Process inserts for sliding windows
fn process_sliding_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	evaluator: &StandardColumnEvaluator,
) -> reifydb_type::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	// Batch compute group hashes for all rows
	let group_hashes = operator.compute_group_keys(columns)?;

	// Partition columns by group hash
	let groups = columns.partition_by_keys(&group_hashes);

	// Process each group
	for (group_hash, group_columns) in groups {
		let group_result = process_sliding_group_insert(operator, txn, &group_columns, group_hash, evaluator)?;
		result.extend(group_result);
	}

	Ok(result)
}

/// Process inserts for a single group in sliding windows
fn process_sliding_group_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	group_hash: Hash128,
	evaluator: &StandardColumnEvaluator,
) -> reifydb_type::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	// Extract timestamps for all rows in this group
	let timestamps = operator.extract_timestamps(columns)?;

	// For sliding windows, process each row (it may go to multiple windows)
	for row_idx in 0..row_count {
		let timestamp = timestamps[row_idx];
		let (event_timestamp, window_ids) = match &operator.window_type {
			WindowType::Time(_) => {
				let window_ids = operator.get_sliding_window_ids(timestamp);
				(timestamp, window_ids)
			}
			WindowType::Count => {
				// For count-based windows, use current processing time and calculate
				// proper sliding window IDs based on event index
				let event_timestamp = operator.current_timestamp();
				let group_count = operator.get_and_increment_global_count(txn, group_hash)?;
				let window_ids = operator.get_sliding_window_ids(group_count);
				(event_timestamp, window_ids)
			}
		};

		// Extract this single row
		let single_row_columns = columns.extract_row(row_idx);
		let row = single_row_columns.to_single_row();

		for window_id in window_ids {
			let window_key = operator.create_window_key(group_hash, window_id);
			let mut window_state = operator.load_window_state(txn, &window_key)?;

			// Calculate previous aggregation BEFORE adding the new event (for Update diff)
			// Only calculate if previous state had enough events for aggregation
			let previous_aggregation = if window_state.events.len() >= operator.min_events {
				operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)?
			} else {
				None
			};

			// Add event to window
			let event = WindowEvent::from_row(&row, event_timestamp);
			window_state.events.push(event);
			window_state.event_count += 1;

			if window_state.window_start == 0 {
				// Set window start using event timestamp for sliding windows
				window_state.window_start =
					operator.set_sliding_window_start(event_timestamp, window_id);
			}

			// Check if window should be triggered (get fresh timestamp for each check)
			let trigger_check_time = operator.current_timestamp();
			let should_trigger = operator.should_trigger_window(&window_state, trigger_check_time);

			if should_trigger {
				// Apply aggregations and emit result
				if let Some((aggregated_row, is_new)) =
					operator.apply_aggregations(txn, &window_key, &window_state.events, evaluator)?
				{
					if is_new {
						// First time this window appears
						result.push(FlowDiff::Insert {
							post: Columns::from_row(&aggregated_row),
						});
					} else {
						// Window exists, need to emit Update with previous state
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

			operator.save_window_state(txn, &window_key, &window_state)?;
		}
	}

	Ok(result)
}

/// Apply changes for sliding windows
pub fn apply_sliding_window(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: FlowChange,
	evaluator: &StandardColumnEvaluator,
) -> reifydb_type::Result<FlowChange> {
	let mut result = Vec::new();
	let current_timestamp = operator.current_timestamp();

	// First, process any expired windows
	let expired_diffs = operator.process_expired_windows(txn, current_timestamp)?;
	result.extend(expired_diffs);

	// Process each incoming change (each diff may contain multiple rows)
	for diff in change.diffs.iter() {
		match diff {
			FlowDiff::Insert {
				post,
			} => {
				let insert_result = process_sliding_insert(operator, txn, post, evaluator)?;
				result.extend(insert_result);
			}
			FlowDiff::Update {
				pre: _,
				post,
			} => {
				// For windows, updates are treated as insert of new value
				let update_result = process_sliding_insert(operator, txn, post, evaluator)?;
				result.extend(update_result);
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
