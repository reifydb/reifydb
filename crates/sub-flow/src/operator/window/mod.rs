// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, Error, Row, WindowSize, WindowSlide, WindowTimeMode, WindowType,
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
		stateful::{RawStatefulOperator, RowNumberProvider, WindowStateful},
		transform::TransformOperator,
	},
};

mod sliding;
mod tumbling;

pub use sliding::apply_sliding_window;
pub use tumbling::apply_tumbling_window;

static EMPTY_PARAMS: Params = Params::None;

/// A single event stored within a window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEvent {
	pub row_number: RowNumber,
	pub timestamp: u64, // System timestamp in milliseconds
	#[serde(with = "serde_bytes")]
	pub encoded_bytes: Vec<u8>,
	pub layout_names: Vec<String>,
	pub layout_types: Vec<Type>,
}

impl WindowEvent {
	pub fn from_row(row: &Row, timestamp: u64) -> Self {
		let names = row.layout.names().to_vec();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		// Debug: Extract and log the actual values being stored
		let mut stored_values = Vec::new();
		for (i, field) in row.layout.fields.iter().enumerate() {
			let value = row.layout.get_value(&row.encoded, i);
			stored_values.push(format!("{:?}", value));
		}

		eprintln!(
			"DEBUG WindowEvent::from_row: Storing event row_number={}, timestamp={}, values=[{}]",
			row.number,
			timestamp,
			stored_values.join(", ")
		);
		eprintln!(
			"DEBUG WindowEvent::from_row: Encoded bytes length={}, names={:?}",
			row.encoded.as_slice().len(),
			names
		);

		Self {
			row_number: row.number,
			timestamp,
			encoded_bytes: row.encoded.as_slice().to_vec(),
			layout_names: names,
			layout_types: types,
		}
	}

	pub fn to_row(&self) -> Row {
		let fields: Vec<(String, Type)> =
			self.layout_names.iter().cloned().zip(self.layout_types.iter().cloned()).collect();

		let layout = EncodedValuesNamedLayout::new(fields);
		let encoded = EncodedValues(CowVec::new(self.encoded_bytes.clone()));

		let row = Row {
			number: self.row_number,
			encoded,
			layout,
		};

		// Debug: Extract and log the actual values being retrieved
		let mut retrieved_values = Vec::new();
		for (i, _field) in row.layout.fields.iter().enumerate() {
			let value = row.layout.get_value(&row.encoded, i);
			retrieved_values.push(format!("{:?}", value));
		}

		eprintln!(
			"DEBUG WindowEvent::to_row: Retrieving event row_number={}, timestamp={}, values=[{}]",
			self.row_number,
			self.timestamp,
			retrieved_values.join(", ")
		);
		eprintln!(
			"DEBUG WindowEvent::to_row: Original encoded length={}, retrieved encoded length={}, names={:?}",
			self.encoded_bytes.len(),
			row.encoded.as_slice().len(),
			self.layout_names
		);

		row
	}
}

/// State for a single window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowState {
	/// All events in this window (stored in insertion order)
	pub events: Vec<WindowEvent>,
	/// Window creation timestamp
	pub window_start: u64,
	/// Count of events in window (for count-based windows)
	pub event_count: u64,
	/// Whether this window has been triggered/computed
	pub is_triggered: bool,
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
	pub node: FlowNodeId,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'static>>,
	pub aggregations: Vec<Expression<'static>>,
	pub layout: EncodedValuesLayout,
	pub column_evaluator: StandardColumnEvaluator,
	pub row_number_provider: RowNumberProvider,
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
			row_number_provider: RowNumberProvider::new(node),
		}
	}

	/// Get the current timestamp in milliseconds
	pub fn current_timestamp(&self) -> u64 {
		clock::now_millis()
	}

	/// Compute the group key for a row (used for partitioning windows by group_by expressions)
	pub fn compute_group_key(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<Hash128> {
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
	pub fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		EncodedKey::new(serializer.finish())
	}

	/// Extract timestamp from row data
	pub fn extract_timestamp_from_row(&self, row: &Row) -> crate::Result<u64> {
		match &self.window_type {
			WindowType::Time(time_mode) => match time_mode {
				WindowTimeMode::Processing => Ok(self.current_timestamp()),
				WindowTimeMode::EventTime(column_name) => {
					if let Some(timestamp_index) =
						row.layout.names().iter().position(|name| name == column_name)
					{
						let timestamp_value =
							row.layout.layout().get_i64(&row.encoded, timestamp_index);
						Ok(timestamp_value as u64)
					} else {
						Err(Error(internal_error!(
							"Event time column '{}' not found in row with columns: {:?}",
							column_name,
							row.layout.names()
						)))
					}
				}
			},
			WindowType::Count => {
				unreachable!(
					"extract_timestamp_from_row should never be called for count-based windows"
				)
			}
		}
	}

	/// Extract group values from window events (all events in a group have the same group values)
	pub fn extract_group_values(
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
	pub fn events_to_columns(&self, events: &[WindowEvent]) -> crate::Result<Columns<'static>> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		eprintln!("DEBUG events_to_columns: Converting {} events to columns", events.len());

		// Debug: Log each event before conversion
		for (i, event) in events.iter().enumerate() {
			eprintln!(
				"DEBUG events_to_columns: Event[{}] row_number={}, timestamp={}",
				i, event.row_number, event.timestamp
			);
		}

		// Use the first event to determine the schema
		let first_event = &events[0];
		let mut columns = Vec::new();

		// Create columns for each field in the schema
		for (field_idx, (field_name, field_type)) in
			first_event.layout_names.iter().zip(first_event.layout_types.iter()).enumerate()
		{
			eprintln!(
				"DEBUG events_to_columns: Processing field[{}] name='{}' type={:?}",
				field_idx, field_name, field_type
			);

			let mut column_data = ColumnData::with_capacity(*field_type, events.len());
			let mut field_values = Vec::new();

			// Collect values from all events for this column
			for (event_idx, event) in events.iter().enumerate() {
				let row = event.to_row();
				let value = row.layout.get_value(&row.encoded, field_idx);
				eprintln!(
					"DEBUG events_to_columns: Event[{}] field[{}] '{}' = {:?}",
					event_idx, field_idx, field_name, value
				);
				field_values.push(format!("{:?}", value));
				column_data.push_value(value);
			}

			eprintln!(
				"DEBUG events_to_columns: Column '{}' final values = [{}]",
				field_name,
				field_values.join(", ")
			);

			columns.push(Column {
				name: Fragment::owned_internal(field_name.clone()),
				data: column_data,
			});
		}

		eprintln!("DEBUG events_to_columns: Created {} columns", columns.len());
		Ok(Columns::new(columns))
	}

	/// Apply aggregations to all events in a window
	pub fn apply_aggregations(
		&self,
		txn: &mut StandardCommandTransaction,
		window_key: &EncodedKey,
		events: &[WindowEvent],
		row_evaluator: &StandardRowEvaluator,
	) -> crate::Result<Option<(Row, bool)>> {
		if events.is_empty() {
			return Ok(None);
		}

		if self.aggregations.is_empty() {
			// No aggregations configured, return first event as result
			let (result_row_number, is_new) =
				self.row_number_provider.get_or_create_row_number(txn, self, window_key)?;
			let mut result_row = events[0].to_row();
			result_row.number = result_row_number;
			return Ok(Some((result_row, is_new)));
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
			is_aggregate_context: true, // Use aggregate functions for window aggregations
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
		eprintln!("DEBUG apply_aggregations: Processing {} aggregation expressions", self.aggregations.len());
		for (i, aggregation) in self.aggregations.iter().enumerate() {
			eprintln!("DEBUG apply_aggregations: Evaluating aggregation {}: {:?}", i, aggregation);
			let agg_column = self.column_evaluator.evaluate(&ctx, aggregation)?;
			eprintln!(
				"DEBUG apply_aggregations: Got aggregation result with {} values",
				agg_column.data().len()
			);
			// For aggregations, we take the computed aggregated value (should be single value)
			let value = agg_column.data().get_value(0);
			eprintln!("DEBUG apply_aggregations: Aggregation {} result: {:?}", i, value);
			result_values.push(value.clone());
			result_names.push(column_name_from_expression(aggregation).text().to_string());
			result_types.push(value.get_type());
		}

		// Create result row with aggregated values
		let layout =
			EncodedValuesNamedLayout::new(result_names.iter().cloned().zip(result_types.iter().cloned()));
		let mut encoded = layout.allocate_row();
		layout.set_values(&mut encoded, &result_values);

		// Use RowNumberProvider to get unique, stable row number for this window
		let (result_row_number, is_new) =
			self.row_number_provider.get_or_create_row_number(txn, self, window_key)?;

		let result_row = Row {
			number: result_row_number,
			encoded,
			layout,
		};

		Ok(Some((result_row, is_new)))
	}

	/// Process expired windows and clean up state
	pub fn process_expired_windows(
		&self,
		txn: &mut StandardCommandTransaction,
		current_timestamp: u64,
	) -> crate::Result<Vec<FlowDiff>> {
		let result = Vec::new();

		// For time-based windows, expire windows that are older than the window size + slide
		if let (WindowType::Time(_), WindowSize::Duration(duration)) = (&self.window_type, &self.size) {
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

	/// Load window state from storage
	pub fn load_window_state(
		&self,
		txn: &mut StandardCommandTransaction,
		window_key: &EncodedKey,
	) -> crate::Result<WindowState> {
		eprintln!("DEBUG load_window_state: Loading window key={:?}", window_key);

		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			eprintln!("DEBUG load_window_state: No state found, returning default WindowState");
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			eprintln!("DEBUG load_window_state: Empty blob, returning default WindowState");
			return Ok(WindowState::default());
		}

		eprintln!("DEBUG load_window_state: Deserializing {} bytes of state data", blob.as_ref().len());

		let config = standard();
		let result: Result<WindowState, _> = decode_from_slice(blob.as_ref(), config)
			.map(|(state, _): (WindowState, usize)| state)
			.map_err(|e| Error(internal_error!("Failed to deserialize WindowState: {}", e)));

		match &result {
			Ok(state) => {
				eprintln!(
					"DEBUG load_window_state: Loaded state with {} events, window_start={}, event_count={}",
					state.events.len(),
					state.window_start,
					state.event_count
				);

				// Debug: Log the actual event values that were loaded
				for (i, event) in state.events.iter().enumerate() {
					let row = event.to_row();
					let mut event_values = Vec::new();
					for (field_idx, _) in row.layout.fields.iter().enumerate() {
						let value = row.layout.get_value(&row.encoded, field_idx);
						event_values.push(format!("{:?}", value));
					}
					eprintln!(
						"DEBUG load_window_state: Loaded event[{}] row_number={}, values=[{}]",
						i,
						event.row_number,
						event_values.join(", ")
					);
				}
			}
			Err(e) => {
				eprintln!("DEBUG load_window_state: Failed to deserialize: {:?}", e);
			}
		}

		result
	}

	/// Save window state to storage
	pub fn save_window_state(
		&self,
		txn: &mut StandardCommandTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> crate::Result<()> {
		eprintln!("DEBUG save_window_state: Saving window key={:?}", window_key);
		eprintln!(
			"DEBUG save_window_state: State has {} events, window_start={}, event_count={}",
			state.events.len(),
			state.window_start,
			state.event_count
		);

		// Debug: Log the actual event values being saved
		for (i, event) in state.events.iter().enumerate() {
			let row = event.to_row();
			let mut event_values = Vec::new();
			for (field_idx, _) in row.layout.fields.iter().enumerate() {
				let value = row.layout.get_value(&row.encoded, field_idx);
				event_values.push(format!("{:?}", value));
			}
			eprintln!(
				"DEBUG save_window_state: Saving event[{}] row_number={}, values=[{}]",
				i,
				event.row_number,
				event_values.join(", ")
			);
		}

		let config = standard();
		let serialized = encode_to_vec(state, config)
			.map_err(|e| Error(internal_error!("Failed to serialize WindowState: {}", e)))?;

		eprintln!("DEBUG save_window_state: Serialized to {} bytes", serialized.len());

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		let result = self.save_state(txn, window_key, state_row);
		match &result {
			Ok(_) => eprintln!("DEBUG save_window_state: Successfully saved window state"),
			Err(e) => eprintln!("DEBUG save_window_state: Failed to save: {:?}", e),
		}

		result
	}

	/// Get and increment global event count for count-based windows
	pub fn get_and_increment_global_count(
		&self,
		txn: &mut StandardCommandTransaction,
		group_hash: Hash128,
	) -> crate::Result<u64> {
		let count_key = self.create_count_key(group_hash);
		let count_row = self.load_state(txn, &count_key)?;

		let current_count = if count_row.is_empty() || !count_row.is_defined(0) {
			0
		} else {
			let blob = self.layout.get_blob(&count_row, 0);
			if blob.is_empty() {
				0
			} else {
				let config = standard();
				decode_from_slice(blob.as_ref(), config).map(|(count, _): (u64, _)| count).unwrap_or(0)
			}
		};

		let new_count = current_count + 1;

		// Save updated count
		let config = standard();
		let serialized = encode_to_vec(&new_count, config)
			.map_err(|e| Error(internal_error!("Failed to serialize count: {}", e)))?;

		let mut count_state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut count_state_row, 0, &blob);

		self.save_state(txn, &count_key, count_state_row)?;

		Ok(current_count)
	}

	/// Create a count key for global event counting
	pub fn create_count_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"cnt:");
		serializer.extend_u128(group_hash);
		EncodedKey::new(serializer.finish())
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
		match &self.slide {
			Some(_) => apply_sliding_window(self, txn, change, evaluator),
			None => apply_tumbling_window(self, txn, change, evaluator),
		}
	}
}

/// Additional helper methods for window triggering
impl WindowOperator {
	/// Check if a window should be triggered (emitted)
	pub fn should_trigger_window(&self, state: &WindowState, current_timestamp: u64) -> bool {
		match (&self.window_type, &self.size, &self.slide) {
			// Tumbling windows (no slide): emit immediately when events arrive (streaming behavior)
			(WindowType::Time(_), WindowSize::Duration(_), None) => {
				if state.event_count > 0 {
					return true;
				}
				false
			}
			// Sliding windows: use time-based triggering
			// For sliding windows, we should trigger when the window is complete
			// but allow multiple triggers as the window slides
			(WindowType::Time(_), WindowSize::Duration(duration), Some(_)) => {
				// For sliding windows, trigger when we have enough content
				// This allows overlapping windows to emit results independently
				if state.event_count > 0 {
					let window_size_ms = duration.as_millis() as u64;
					let trigger_time = state.window_start + window_size_ms;
					eprintln!(
						"DEBUG should_trigger_window: Sliding time window, current_timestamp={}, trigger_time={}, window_start={}, event_count={}",
						current_timestamp, trigger_time, state.window_start, state.event_count
					);
					current_timestamp >= trigger_time
				} else {
					false
				}
			}
			// Count-based tumbling windows: trigger when count threshold is reached
			(WindowType::Count, WindowSize::Count(count), None) => state.event_count >= *count,
			// Count-based sliding windows: trigger immediately but limit window creation
			(WindowType::Count, WindowSize::Count(count), Some(_)) => {
				eprintln!(
					"DEBUG should_trigger_window: Count-based sliding window, event_count={}, count={}",
					state.event_count, count
				);
				// Trigger immediately for partial window visibility
				state.event_count > 0
			}
			_ => false,
		}
	}
}
