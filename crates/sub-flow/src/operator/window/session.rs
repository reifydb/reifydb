// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	common::WindowKind,
	encoded::key::EncodedKey,
	interface::change::{Change, Diff},
	internal,
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
	util::encoding::keycode::serializer::KeySerializer,
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{Result, error::Error, value::blob::Blob};

use super::{WindowEvent, WindowLayout, WindowOperator};
use crate::{operator::stateful::window::WindowStateful, transaction::FlowTransaction};

impl WindowOperator {
	/// Get the session gap duration in milliseconds (only valid for Session windows)
	fn session_gap_ms(&self) -> u64 {
		match &self.kind {
			WindowKind::Session {
				gap,
			} => gap.as_millis() as u64,
			_ => 0,
		}
	}

	/// Create a session-tracking key that stores the current session_id for a group
	fn create_session_tracker_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"ses:");
		serializer.extend_u128(group_hash);
		EncodedKey::new(serializer.finish())
	}

	/// Load the current session_id for a group. Returns (session_id, last_event_time).
	/// If no session exists yet, returns (0, 0).
	fn load_session_tracker(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<(u64, u64)> {
		let tracker_key = self.create_session_tracker_key(group_hash);
		let state_row = self.load_state(txn, &tracker_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok((0, 0));
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok((0, 0));
		}

		let tracker: (u64, u64) = from_bytes(blob.as_ref()).unwrap_or((0, 0));
		Ok(tracker)
	}

	/// Save the session tracker (session_id, last_event_time) for a group
	fn save_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		session_id: u64,
		last_event_time: u64,
	) -> Result<()> {
		let tracker_key = self.create_session_tracker_key(group_hash);
		let serialized = to_stdvec(&(session_id, last_event_time))
			.map_err(|e| Error(internal!("Failed to serialize session tracker: {}", e)))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &tracker_key, state_row)
	}

	/// Tick-based session expiration.
	/// Scans all operator state, finds "win:" keys with expired sessions.
	pub fn tick_session_expiration(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let gap_ms = self.session_gap_ms();
		if gap_ms == 0 {
			return Ok(result);
		}

		let all_state = txn.state_scan(self.node)?;
		let prefix = FlowNodeStateKey::new(self.node, vec![]).encode();
		let win_marker = b"win:";

		let mut keys_to_clear = Vec::new();

		for item in &all_state.items {
			let full_key = &item.key;
			if full_key.len() <= prefix.len() {
				continue;
			}
			let inner = &full_key[prefix.len()..];
			if !inner.starts_with(win_marker) {
				continue;
			}

			let window_key = EncodedKey::new(inner);
			let state = self.load_window_state(txn, &window_key)?;
			if state.events.is_empty() || state.last_event_time == 0 {
				continue;
			}

			if current_timestamp.saturating_sub(state.last_event_time) > gap_ms {
				if let Some(layout) = &state.window_layout {
					if let Some((row, _)) =
						self.apply_aggregations(txn, &window_key, layout, &state.events)?
					{
						result.push(Diff::Remove {
							pre: Columns::from_row(&row),
						});
					}
				}
				keys_to_clear.push(window_key);
			}
		}

		for key in &keys_to_clear {
			let empty = self.create_state();
			self.save_state(txn, key, empty)?;
		}

		Ok(result)
	}
}

/// Process inserts for a single group in session windows
fn process_session_group_insert(
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

	let gap_ms = operator.session_gap_ms();
	let timestamps = operator.resolve_event_timestamps(columns, row_count)?;

	let (mut session_id, mut last_event_time) = operator.load_session_tracker(txn, group_hash)?;

	for row_idx in 0..row_count {
		let event_timestamp = timestamps[row_idx];

		// Check if the gap has been exceeded → close old session, open new
		let gap_exceeded = last_event_time > 0 && (event_timestamp - last_event_time) > gap_ms;

		if gap_exceeded {
			// Emit Remove for the old session before starting a new one
			let pre_window_key = operator.create_window_key(group_hash, session_id);
			let pre_state = operator.load_window_state(txn, &pre_window_key)?;
			if !pre_state.events.is_empty() {
				if let Some(layout) = &pre_state.window_layout {
					if let Some((pre_row, _)) = operator.apply_aggregations(
						txn,
						&pre_window_key,
						layout,
						&pre_state.events,
					)? {
						result.push(Diff::Remove {
							pre: Columns::from_row(&pre_row),
						});
					}
				}
			}
			session_id += 1;
		}

		let window_key = operator.create_window_key(group_hash, session_id);
		let mut window_state = operator.load_window_state(txn, &window_key)?;

		let single_row_columns = columns.extract_row(row_idx);
		let projected = operator.project_columns(&single_row_columns);
		let row = projected.to_single_row();

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
		operator.store_row_index(txn, group_hash, event_row_number, session_id)?;
		last_event_time = event_timestamp;
	}

	operator.save_session_tracker(txn, group_hash, session_id, last_event_time)?;

	Ok(result)
}

/// Apply changes for session windows (no time-based expiration — sessions close lazily)
pub fn apply_session_window(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let diffs = operator.apply_window_change(txn, &change, false, |op, txn, columns| {
		op.process_insert(txn, columns, process_session_group_insert)
	})?;
	Ok(Change::from_flow(operator.node, change.version, diffs))
}
