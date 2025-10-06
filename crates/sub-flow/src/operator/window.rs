// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::{Duration, SystemTime};

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, Error, Row, WindowSize, WindowSlide, WindowType,
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, expression::Expression},
	util::encoding::keycode::KeySerializer,
	value::encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Blob, Params, RowNumber, Type, internal_error};
use serde::{Deserialize, Serialize};

use crate::{
	flow::{FlowChange, FlowDiff},
	operator::{
		Operator,
		stateful::{RawStatefulOperator, WindowStateful},
		transform::TransformOperator,
	},
};

static EMPTY_PARAMS: Params = Params::None;

/// A single event stored within a window
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowEvent {
	row_number: RowNumber,
	timestamp: u64, // System timestamp in milliseconds
	#[serde(with = "serde_bytes")]
	encoded_bytes: Vec<u8>,
	layout_names: Vec<String>,
	layout_types: Vec<Type>,
}

impl WindowEvent {
	fn from_row(row: &Row, timestamp: u64) -> Self {
		let names = row.layout.names().to_vec();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		Self {
			row_number: row.number,
			timestamp,
			encoded_bytes: row.encoded.as_slice().to_vec(),
			layout_names: names,
			layout_types: types,
		}
	}

	fn to_row(&self) -> Row {
		let fields: Vec<(String, Type)> =
			self.layout_names.iter().cloned().zip(self.layout_types.iter().cloned()).collect();

		let layout = EncodedValuesNamedLayout::new(fields);
		let encoded = EncodedValues(CowVec::new(self.encoded_bytes.clone()));

		Row {
			number: self.row_number,
			encoded,
			layout,
		}
	}
}

/// State for a single window
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowState {
	/// All events in this window (stored in insertion order)
	events: Vec<WindowEvent>,
	/// Window creation timestamp
	window_start: u64,
	/// Count of events in window (for count-based windows)
	event_count: u64,
	/// Whether this window has been triggered/computed
	is_triggered: bool,
}

impl Default for WindowState {
	fn default() -> Self {
		Self {
			events: Vec::new(),
			window_start: 0,
			event_count: 0,
			is_triggered: false,
		}
	}
}

/// The main window operator
pub struct WindowOperator {
	node: FlowNodeId,
	window_type: WindowType,
	size: WindowSize,
	slide: Option<WindowSlide>,
	group_by: Vec<Expression<'static>>,
	aggregations: Vec<Expression<'static>>,
	layout: EncodedValuesLayout,
}

impl WindowOperator {
	pub fn new(
		node: FlowNodeId,
		window_type: WindowType,
		size: WindowSize,
		slide: Option<WindowSlide>,
		group_by: Vec<Expression<'static>>,
		aggregations: Vec<Expression<'static>>,
	) -> Self {
		Self {
			node,
			window_type,
			size,
			slide,
			group_by,
			aggregations,
			layout: EncodedValuesLayout::new(&[Type::Blob]),
		}
	}

	/// Get the current timestamp in milliseconds
	fn current_timestamp(&self) -> u64 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64
	}

	/// Compute the group key for a row (used for partitioning windows by group_by expressions)
	fn compute_group_key(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<Hash128> {
		if self.group_by.is_empty() {
			// Single global window
			return Ok(Hash128::from(0u128));
		}

		let ctx = RowEvaluationContext {
			row: row.clone(),
			target: None,
			params: &EMPTY_PARAMS,
		};

		let mut data = Vec::new();
		for expr in &self.group_by {
			let value = evaluator.evaluate(&ctx, expr)?;
			let value_str = value.to_string();
			data.extend_from_slice(value_str.as_bytes());
		}

		Ok(xxh3_128(&data))
	}

	/// Create a window key for storage
	fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		EncodedKey::new(serializer.finish())
	}

	/// Determine which window(s) an event belongs to
	fn get_window_ids(&self, timestamp: u64) -> Vec<u64> {
		match (&self.window_type, &self.size) {
			(WindowType::Time, WindowSize::Duration(duration)) => {
				let window_size_ms = duration.as_millis() as u64;
				let base_window = timestamp / window_size_ms;

				match &self.slide {
					Some(WindowSlide::Duration(slide_duration)) => {
						let slide_ms = slide_duration.as_millis() as u64;
						if slide_ms >= window_size_ms {
							// Non-overlapping windows
							vec![base_window]
						} else {
							// Overlapping windows - event belongs to multiple windows
							let mut windows = Vec::new();
							let num_windows = window_size_ms / slide_ms;
							for i in 0..num_windows {
								let window_start = base_window.saturating_sub(i);
								if timestamp >= window_start * window_size_ms {
									windows.push(window_start);
								}
							}
							windows
						}
					}
					Some(WindowSlide::Count(_)) => {
						// Time windows with count-based slide not supported yet
						vec![base_window]
					}
					None => {
						// Tumbling window
						vec![base_window]
					}
				}
			}
			(WindowType::Count, WindowSize::Count(count)) => {
				// For count-based windows, we use a simple incrementing window ID
				// This is a simplified implementation - real implementation would need
				// to track event counts per group
				vec![timestamp / *count]
			}
			_ => {
				// Mismatched window type and size
				vec![0]
			}
		}
	}

	/// Check if a window should be triggered (emitted)
	fn should_trigger_window(&self, state: &WindowState, current_timestamp: u64) -> bool {
		if state.is_triggered {
			return false;
		}

		match (&self.window_type, &self.size) {
			(WindowType::Time, WindowSize::Duration(duration)) => {
				let window_size_ms = duration.as_millis() as u64;
				current_timestamp >= state.window_start + window_size_ms
			}
			(WindowType::Count, WindowSize::Count(count)) => state.event_count >= *count,
			_ => false,
		}
	}

	/// Apply aggregations to all events in a window
	fn apply_aggregations(
		&self,
		events: &[WindowEvent],
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<Row>> {
		if events.is_empty() {
			return Ok(None);
		}

		// For now, we'll use the first event's layout and apply aggregations
		// In a complete implementation, this would properly handle schema evolution
		let first_event = &events[0];
		let result_row = first_event.to_row();

		// Apply each aggregation expression
		for aggregation in &self.aggregations {
			let ctx = RowEvaluationContext {
				row: result_row.clone(),
				target: None,
				params: &EMPTY_PARAMS,
			};

			// This is a simplified aggregation - real implementation would handle
			// different aggregation functions (SUM, COUNT, AVG, etc.)
			let _aggregated_value = evaluator.evaluate(&ctx, aggregation)?;
			// TODO: Update result_row with aggregated value
		}

		Ok(Some(result_row))
	}

	/// Process expired windows and clean up state
	fn process_expired_windows(
		&self,
		txn: &mut StandardCommandTransaction,
		current_timestamp: u64,
	) -> crate::Result<Vec<FlowDiff>> {
		let result = Vec::new();

		// For time-based windows, expire windows that are older than the window size + slide
		if let (WindowType::Time, WindowSize::Duration(duration)) = (&self.window_type, &self.size) {
			let window_size_ms = duration.as_millis() as u64;
			let expire_before = current_timestamp.saturating_sub(window_size_ms * 2); // Keep 2 window sizes

			// This is a simplified cleanup - real implementation would iterate through
			// all group keys and clean up expired windows for each group
			let before_key = self.create_window_key(Hash128::from(0u128), expire_before / window_size_ms);
			let range =
				EncodedKeyRange::new(std::ops::Bound::Excluded(before_key), std::ops::Bound::Unbounded);

			let _expired_count = self.expire_range(txn, range)?;
		}

		Ok(result)
	}
}

impl TransformOperator for WindowOperator {}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> EncodedValuesLayout {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let mut result = Vec::new();
		let current_timestamp = self.current_timestamp();

		// First, process any expired windows
		let expired_diffs = self.process_expired_windows(txn, current_timestamp)?;
		result.extend(expired_diffs);

		// Process each incoming change
		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let group_hash = self.compute_group_key(&post, evaluator)?;
					let window_ids = self.get_window_ids(current_timestamp);

					for window_id in window_ids {
						let window_key = self.create_window_key(group_hash, window_id);
						let mut window_state = self.load_window_state(txn, &window_key)?;

						// Add event to window
						let event = WindowEvent::from_row(&post, current_timestamp);
						window_state.events.push(event);
						window_state.event_count += 1;

						if window_state.window_start == 0 {
							window_state.window_start = current_timestamp;
						}

						// Check if window should be triggered
						if self.should_trigger_window(&window_state, current_timestamp) {
							window_state.is_triggered = true;

							// Apply aggregations and emit result
							if let Some(aggregated_row) = self
								.apply_aggregations(&window_state.events, evaluator)?
							{
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}

						self.save_window_state(txn, &window_key, &window_state)?;
					}
				}
				FlowDiff::Update {
					pre: _,
					post,
				} => {
					// For windows, updates are treated as insert of new value
					// Real implementation might need to handle retractions
					let group_hash = self.compute_group_key(&post, evaluator)?;
					let window_ids = self.get_window_ids(current_timestamp);

					for window_id in window_ids {
						let window_key = self.create_window_key(group_hash, window_id);
						let mut window_state = self.load_window_state(txn, &window_key)?;

						let event = WindowEvent::from_row(&post, current_timestamp);
						window_state.events.push(event);
						window_state.event_count += 1;

						if self.should_trigger_window(&window_state, current_timestamp) {
							window_state.is_triggered = true;

							if let Some(aggregated_row) = self
								.apply_aggregations(&window_state.events, evaluator)?
							{
								result.push(FlowDiff::Insert {
									post: aggregated_row,
								});
							}
						}

						self.save_window_state(txn, &window_key, &window_state)?;
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

		Ok(FlowChange::internal(self.node, change.version, result))
	}
}

impl WindowOperator {
	/// Load window state from storage
	fn load_window_state(
		&self,
		txn: &mut StandardCommandTransaction,
		window_key: &EncodedKey,
	) -> crate::Result<WindowState> {
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		let config = standard();
		decode_from_slice(blob.as_ref(), config)
			.map(|(state, _)| state)
			.map_err(|e| Error(internal_error!("Failed to deserialize WindowState: {}", e)))
	}

	/// Save window state to storage
	fn save_window_state(
		&self,
		txn: &mut StandardCommandTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> crate::Result<()> {
		let config = standard();
		let serialized = encode_to_vec(state, config)
			.map_err(|e| Error(internal_error!("Failed to serialize WindowState: {}", e)))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, window_key, state_row)
	}
}
