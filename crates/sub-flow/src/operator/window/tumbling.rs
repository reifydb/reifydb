// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
use reifydb_core::{
	common::{WindowKind, WindowMeasure},
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::Result;

use super::{WindowEvent, WindowLayout, WindowOperator};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Determine which window an event belongs to for tumbling windows
	pub fn get_tumbling_window_id(&self, timestamp: u64) -> u64 {
		match &self.kind {
			WindowKind::Tumbling {
				size: WindowMeasure::Duration(duration),
			} => {
				let window_size_ms = duration.as_millis() as u64;
				(timestamp / window_size_ms) * window_size_ms / window_size_ms
			}
			WindowKind::Tumbling {
				size: WindowMeasure::Count(count),
			} => timestamp / *count,
			_ => 0,
		}
	}

	/// Set window start time for tumbling windows (aligned to window boundaries)
	pub fn set_tumbling_window_start(&self, timestamp: u64) -> u64 {
		if let Some(duration) = self.size_duration() {
			let window_size_ms = duration.as_millis() as u64;
			(timestamp / window_size_ms) * window_size_ms
		} else {
			timestamp
		}
	}
}

/// Process inserts for a single group in tumbling windows
fn process_tumbling_group_insert(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	columns: &Columns,
	group_hash: Hash128,
) -> Result<Vec<Diff>> {
	let mut result = Vec::new();
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(result);
	}

	let timestamps = operator.resolve_event_timestamps(columns, row_count)?;

	for row_idx in 0..row_count {
		let timestamp = timestamps[row_idx];
		let (event_timestamp, window_id) = if operator.is_count_based() {
			let event_timestamp = operator.current_timestamp();
			let global_count = operator.get_and_increment_global_count(txn, group_hash)?;
			let window_size = operator.size_count().unwrap_or(3);
			(event_timestamp, global_count / window_size)
		} else {
			(timestamp, operator.get_tumbling_window_id(timestamp))
		};

		let window_key = operator.create_window_key(group_hash, window_id);
		let mut window_state = operator.load_window_state(txn, &window_key)?;

		let single_row_columns = columns.extract_row(row_idx);
		let row = single_row_columns.to_single_row();

		if window_state.window_layout.is_none() {
			window_state.window_layout = Some(WindowLayout::from_row(&row));
		}
		let layout = window_state.layout().clone();

		let previous_aggregation = if !window_state.events.is_empty() {
			operator.apply_aggregations(txn, &window_key, &layout, &window_state.events)?
		} else {
			None
		};

		let event = WindowEvent::from_row(&row, event_timestamp);
		let event_row_number = event.row_number;
		window_state.events.push(event);
		window_state.event_count += 1;
		window_state.last_event_time = event_timestamp;

		if window_state.window_start == 0 {
			window_state.window_start = operator.set_tumbling_window_start(event_timestamp);
		}

		if let Some((aggregated_row, is_new)) =
			operator.apply_aggregations(txn, &window_key, &layout, &window_state.events)?
		{
			result.push(WindowOperator::emit_aggregation_diff(
				&aggregated_row,
				is_new,
				previous_aggregation,
			));
		}

		operator.save_window_state(txn, &window_key, &window_state)?;
		operator.store_row_index(txn, group_hash, event_row_number, window_id)?;
	}

	Ok(result)
}

/// Apply changes for tumbling windows
pub fn apply_tumbling_window(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let diffs = operator.apply_window_change(txn, &change, true, |op, txn, columns| {
		op.process_insert(txn, columns, process_tumbling_group_insert)
	})?;
	Ok(Change::from_flow(operator.node, change.version, diffs))
}
