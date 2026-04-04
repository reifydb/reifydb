// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{Result, value::datetime::DateTime};

use super::{WindowEvent, WindowLayout, WindowOperator};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Determine which windows an event belongs to for sliding windows
	pub fn get_sliding_window_ids(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Duration(duration),
				slide: WindowSize::Duration(slide_duration),
			} => {
				let window_size_ms = duration.as_millis() as u64;
				let slide_ms = slide_duration.as_millis() as u64;
				let timestamp = timestamp_or_row_index;

				if slide_ms >= window_size_ms {
					vec![timestamp / slide_ms]
				} else {
					let min_window_id = if timestamp >= window_size_ms {
						(timestamp - window_size_ms + 1) / slide_ms
					} else {
						0
					};
					let max_window_id = timestamp / slide_ms;
					(min_window_id..=max_window_id)
						.filter(|&wid| {
							let start = wid * slide_ms;
							timestamp >= start && timestamp < start + window_size_ms
						})
						.collect()
				}
			}
			WindowKind::Sliding {
				size: WindowSize::Count(count),
				slide: WindowSize::Count(slide_count),
			} => {
				let row_number = timestamp_or_row_index + 1; // 1-based
				let min_window = if row_number > *count {
					(row_number - *count) / *slide_count
				} else {
					0
				};
				let max_window = (row_number - 1) / *slide_count;
				(min_window..=max_window)
					.filter(|&wid| {
						let start_row = wid * *slide_count + 1;
						let end_row = start_row + *count - 1;
						row_number >= start_row && row_number <= end_row
					})
					.collect()
			}
			_ => vec![0],
		}
	}

	/// Set window start time for sliding windows (aligned to slide boundaries)
	pub fn set_sliding_window_start(&self, timestamp: u64, window_id: u64) -> u64 {
		match &self.kind {
			WindowKind::Sliding {
				slide: WindowSize::Duration(slide_duration),
				..
			} => {
				let slide_ms = slide_duration.as_millis() as u64;
				window_id * slide_ms
			}
			_ => timestamp,
		}
	}
}

/// Process inserts for a single group in sliding windows
fn process_sliding_group_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	group_hash: Hash128,
	changed_at: DateTime,
) -> Result<Vec<Diff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	let timestamps = operator.resolve_event_timestamps(columns, row_count)?;

	for (row_idx, &timestamp) in timestamps.iter().enumerate() {
		let (event_timestamp, window_ids) = if operator.is_count_based() {
			let event_timestamp = operator.current_timestamp();
			let group_count = operator.get_and_increment_global_count(txn, group_hash)?;
			(event_timestamp, operator.get_sliding_window_ids(group_count))
		} else {
			(timestamp, operator.get_sliding_window_ids(timestamp))
		};

		let single_row_columns = columns.extract_row(row_idx);
		let projected = operator.project_columns(&single_row_columns);
		let row = projected.to_single_row();
		let row_layout = WindowLayout::from_row(&row);

		for window_id in window_ids {
			let window_key = operator.create_window_key(group_hash, window_id);
			let mut window_state = operator.load_window_state(txn, &window_key)?;

			if window_state.window_layout.is_none() {
				window_state.window_layout = Some(row_layout.clone());
			}
			let layout = window_state.layout().clone();

			let previous_aggregation = if !window_state.events.is_empty() {
				operator.apply_aggregations(
					txn,
					&window_key,
					&layout,
					&window_state.events,
					changed_at,
				)?
			} else {
				None
			};

			let event = WindowEvent::from_row(&row, event_timestamp);
			let event_row_number = event.row_number;
			window_state.events.push(event);
			window_state.event_count += 1;
			window_state.last_event_time = event_timestamp;

			if window_state.window_start == 0 {
				window_state.window_start =
					operator.set_sliding_window_start(event_timestamp, window_id);
			}

			if let Some((aggregated_row, is_new)) = operator.apply_aggregations(
				txn,
				&window_key,
				&layout,
				&window_state.events,
				changed_at,
			)? {
				result.push(WindowOperator::emit_aggregation_diff(
					&aggregated_row,
					is_new,
					previous_aggregation,
				));
			}

			operator.save_window_state(txn, &window_key, &window_state)?;
			operator.store_row_index(txn, group_hash, event_row_number, window_id)?;
		}
	}

	Ok(result)
}

/// Apply changes for sliding windows
pub fn apply_sliding_window(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let changed_at = change.changed_at;
	let diffs = operator.apply_window_change(txn, &change, true, |op, txn, columns| {
		op.process_insert(txn, columns, changed_at, process_sliding_group_insert)
	})?;
	Ok(Change::from_flow(operator.node, change.version, diffs, change.changed_at))
}
