// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::Result;

use super::{WindowEvent, WindowLayout, WindowOperator, WindowState};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	/// Evict old events from rolling window to maintain size limit
	pub fn evict_old_events(&self, state: &mut WindowState, current_timestamp: u64) {
		match &self.kind {
			WindowKind::Rolling {
				size: WindowSize::Duration(duration),
			} => {
				let window_size_ms = duration.as_millis() as u64;
				let cutoff_time = current_timestamp.saturating_sub(window_size_ms);
				let original_len = state.events.len();
				state.events.retain(|event| event.timestamp > cutoff_time);
				let evicted_count = original_len - state.events.len();
				state.event_count = state.event_count.saturating_sub(evicted_count as u64);
			}
			WindowKind::Rolling {
				size: WindowSize::Count(count),
			} => {
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

/// Process inserts for a single group in rolling windows.
/// Rolling windows use a single window (id=0) per group and load state once per group.
fn process_rolling_group_insert(
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

	let current_timestamp = operator.current_timestamp();
	let timestamps = operator.resolve_event_timestamps(columns, row_count)?;

	let window_id = 0u64;
	let window_key = operator.create_window_key(group_hash, window_id);
	let mut window_state = operator.load_window_state(txn, &window_key)?;

	for row_idx in 0..row_count {
		let event_timestamp = timestamps[row_idx];

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
			window_state.window_start = event_timestamp;
		}

		operator.store_row_index(txn, group_hash, event_row_number, window_id)?;

		let eviction_ts = if operator.ts.is_some() {
			event_timestamp
		} else {
			current_timestamp
		};
		operator.evict_old_events(&mut window_state, eviction_ts);

		if !window_state.events.is_empty() {
			if let Some((aggregated_row, is_new)) =
				operator.apply_aggregations(txn, &window_key, &layout, &window_state.events)?
			{
				result.push(WindowOperator::emit_aggregation_diff(
					&aggregated_row,
					is_new,
					previous_aggregation,
				));
			}
		}
	}

	operator.save_window_state(txn, &window_key, &window_state)?;

	Ok(result)
}

/// Apply changes for rolling windows (no expiration — eviction handles cleanup)
pub fn apply_rolling_window(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let diffs = operator.apply_window_change(txn, &change, false, |op, txn, columns| {
		op.process_insert(txn, columns, process_rolling_group_insert)
	})?;
	Ok(Change::from_flow(operator.node, change.version, diffs))
}
