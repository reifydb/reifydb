// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, Error, Row, WindowSize, WindowSlide, WindowType,
	interface::{
		ColumnEvaluationContext, ColumnEvaluator, FlowNodeId, RowEvaluationContext, RowEvaluator,
		expression::Expression,
	},
	util::{clock, encoding::keycode::KeySerializer},
	value::{
		column::{Column, ColumnData, Columns},
		encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
	},
};
use reifydb_engine::{StandardColumnEvaluator, StandardCommandTransaction, StandardRowEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::expression::column_name_from_expression;
use reifydb_type::{Blob, Fragment, Params, RowNumber, Type, Value, internal_error};
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
	column_evaluator: StandardColumnEvaluator,
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
			column_evaluator: StandardColumnEvaluator::default(),
		}
	}

	/// Get the current timestamp in milliseconds
	fn current_timestamp(&self) -> u64 {
		clock::now_millis()
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

	/// Extract timestamp from row data
	fn extract_timestamp_from_row(&self, row: &Row) -> crate::Result<u64> {
		// Try to find timestamp field in the row
		if let Some(timestamp_index) = row.layout.names().iter().position(|name| name == "timestamp") {
			let timestamp_value = row.layout.layout().get_i64(&row.encoded, timestamp_index);
			return Ok(timestamp_value as u64);
		}

		// Fallback to current time if no timestamp field found
		Ok(self.current_timestamp())
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

	/// Extract group values from window events (all events in a group have the same group values)
	fn extract_group_values(
		&self,
		events: &[WindowEvent],
		row_evaluator: &StandardRowEvaluator,
	) -> crate::Result<(Vec<Value>, Vec<String>)> {
		if events.is_empty() || self.group_by.is_empty() {
			return Ok((Vec::new(), Vec::new()));
		}

		// Take the first event since all events in a group have the same group values
		let first_event = &events[0];
		let first_row = first_event.to_row();

		let ctx = RowEvaluationContext {
			row: first_row,
			target: None,
			params: &EMPTY_PARAMS,
		};

		let mut group_values = Vec::new();
		let mut group_names = Vec::new();

		for group_expr in &self.group_by {
			let value = row_evaluator.evaluate(&ctx, group_expr)?;
			let name = column_name_from_expression(group_expr).text().to_string();
			group_values.push(value);
			group_names.push(name);
		}

		Ok((group_values, group_names))
	}

	/// Convert window events to columnar format for aggregation
	fn events_to_columns(&self, events: &[WindowEvent]) -> crate::Result<Columns<'static>> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		// Use the first event to determine the schema
		let first_event = &events[0];
		let mut columns = Vec::new();

		// Create columns for each field in the schema
		for (field_idx, (field_name, field_type)) in
			first_event.layout_names.iter().zip(first_event.layout_types.iter()).enumerate()
		{
			let mut column_data = ColumnData::with_capacity(*field_type, events.len());

			// Collect values from all events for this column
			for event in events {
				let row = event.to_row();
				let value = row.layout.get_value(&row.encoded, field_idx);
				column_data.push_value(value);
			}

			columns.push(Column {
				name: Fragment::owned_internal(field_name.clone()),
				data: column_data,
			});
		}

		Ok(Columns::new(columns))
	}

	/// Apply aggregations to all events in a window
	fn apply_aggregations(
		&self,
		events: &[WindowEvent],
		row_evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<Row>> {
		if events.is_empty() {
			return Ok(None);
		}

		if self.aggregations.is_empty() {
			// No aggregations configured, return first event as result
			return Ok(Some(events[0].to_row()));
		}

		// Convert window events to columnar format
		let columns = self.events_to_columns(events)?;

		// Create column evaluation context
		let ctx = ColumnEvaluationContext {
			target: None,
			columns,
			row_count: events.len(),
			take: None,
			params: &EMPTY_PARAMS,
		};

		// Extract group values from window events
		let (group_values, group_names) = self.extract_group_values(events, row_evaluator)?;

		// Evaluate each aggregation expression and collect results
		let mut result_values = Vec::new();
		let mut result_names = Vec::new();
		let mut result_types = Vec::new();

		// Add group-by columns first (if any)
		for (value, name) in group_values.into_iter().zip(group_names.into_iter()) {
			result_values.push(value.clone());
			result_names.push(name);
			result_types.push(value.get_type());
		}

		// Apply aggregation expressions
		for aggregation in &self.aggregations {
			let agg_column = self.column_evaluator.evaluate(&ctx, aggregation)?;
			// For aggregations, we take the computed aggregated value (should be single value)
			let value = agg_column.data().get_value(0);
			result_values.push(value.clone());
			result_names.push(column_name_from_expression(aggregation).text().to_string());
			result_types.push(value.get_type());
		}

		// Create result row with aggregated values
		let layout =
			EncodedValuesNamedLayout::new(result_names.iter().cloned().zip(result_types.iter().cloned()));
		let mut encoded = layout.allocate_row();
		layout.set_values(&mut encoded, &result_values);

		let result_row = Row {
			number: events[0].row_number, // Use the first event's row number
			encoded,
			layout,
		};

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
		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let group_hash = self.compute_group_key(&post, evaluator)?;
					let event_timestamp = self.extract_timestamp_from_row(&post)?;
					let window_ids = self.get_window_ids(event_timestamp);

					for window_id in window_ids {
						let window_key = self.create_window_key(group_hash, window_id);
						let mut window_state = self.load_window_state(txn, &window_key)?;

						// Add event to window
						let event = WindowEvent::from_row(&post, event_timestamp);
						window_state.events.push(event);
						window_state.event_count += 1;

						if window_state.window_start == 0 {
							window_state.window_start = event_timestamp;
						}

						// Check if window should be triggered
						let should_trigger =
							self.should_trigger_window(&window_state, current_timestamp);

						if should_trigger {
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
					let event_timestamp = self.extract_timestamp_from_row(&post)?;
					let window_ids = self.get_window_ids(event_timestamp);

					for window_id in window_ids {
						let window_key = self.create_window_key(group_hash, window_id);
						let mut window_state = self.load_window_state(txn, &window_key)?;

						let event = WindowEvent::from_row(&post, event_timestamp);
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
