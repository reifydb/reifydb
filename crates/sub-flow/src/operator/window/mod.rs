// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, Error, Row, WindowSize, WindowSlide, WindowTimeMode,
	WindowType,
	interface::FlowNodeId,
	util::{clock, encoding::keycode::KeySerializer},
	value::{
		column::{Column, ColumnData, Columns},
		encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
	},
};
use reifydb_engine::{
	ColumnEvaluationContext, RowEvaluationContext, StandardColumnEvaluator, StandardCommandTransaction,
	StandardRowEvaluator,
};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_rql::expression::{Expression, column_name_from_expression};
use reifydb_type::{Blob, Fragment, Params, RowNumber, Type, Value, internal_error};
use serde::{Deserialize, Serialize};

use crate::{
	flow::{FlowChange, FlowDiff},
	operator::{
		Operator, Operators,
		stateful::{RawStatefulOperator, RowNumberProvider, WindowStateful},
		transform::TransformOperator,
	},
};

mod rolling;
mod sliding;
mod tumbling;

pub use rolling::apply_rolling_window;
pub use sliding::apply_sliding_window;
pub use tumbling::apply_tumbling_window;

static EMPTY_PARAMS: Params = Params::None;

use std::sync::LazyLock;

use reifydb_engine::stack::Stack;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

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
	pub parent: Arc<Operators>,
	pub node: FlowNodeId,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'static>>,
	pub aggregations: Vec<Expression<'static>>,
	pub layout: EncodedValuesLayout,
	pub column_evaluator: StandardColumnEvaluator,
	pub row_number_provider: RowNumberProvider,
	pub min_events: usize,               // Minimum events required before window becomes visible
	pub max_window_count: Option<usize>, // Maximum number of windows to keep per group
	pub max_window_age: Option<std::time::Duration>, // Maximum age of windows before expiration
}

impl WindowOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		window_type: WindowType,
		size: WindowSize,
		slide: Option<WindowSlide>,
		group_by: Vec<Expression<'static>>,
		aggregations: Vec<Expression<'static>>,
		min_events: usize,
		max_window_count: Option<usize>,
		max_window_age: Option<std::time::Duration>,
	) -> Self {
		Self {
			parent,
			node,
			window_type,
			size,
			slide,
			group_by,
			aggregations,
			layout: EncodedValuesLayout::new(&[Type::Blob]),
			column_evaluator: StandardColumnEvaluator::default(),
			row_number_provider: RowNumberProvider::new(node),
			min_events: min_events.max(1), // Ensure at least 1 event is required
			max_window_count,
			max_window_age,
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

		// Use the first event to determine the schema
		let first_event = &events[0];
		let mut columns = Vec::new();

		// Create columns for each field in the schema
		for (field_idx, (field_name, field_type)) in
			first_event.layout_names.iter().zip(first_event.layout_types.iter()).enumerate()
		{
			let mut column_data = ColumnData::with_capacity(*field_type, events.len());

			// Collect values from all events for this column
			for (_event_idx, event) in events.iter().enumerate() {
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
			stack: &EMPTY_STACK,
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
		for (_i, aggregation) in self.aggregations.iter().enumerate() {
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
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		let config = standard();
		let result: Result<WindowState, _> = decode_from_slice(blob.as_ref(), config)
			.map(|(state, _): (WindowState, usize)| state)
			.map_err(|e| Error(internal_error!("Failed to deserialize WindowState: {}", e)));

		result
	}

	/// Save window state to storage
	pub fn save_window_state(
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
			Some(WindowSlide::Rolling) => apply_rolling_window(self, txn, change, evaluator),
			Some(_) => apply_sliding_window(self, txn, change, evaluator),
			None => apply_tumbling_window(self, txn, change, evaluator),
		}
	}

	fn get_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		rows: &[RowNumber],
		version: CommitVersion,
	) -> crate::Result<Vec<Option<Row>>> {
		todo!()
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
					current_timestamp >= trigger_time
				} else {
					false
				}
			}
			// Count-based tumbling windows: trigger when count threshold is reached
			(WindowType::Count, WindowSize::Count(count), None) => state.event_count >= *count,
			// Count-based sliding windows: trigger when min_events threshold is met
			(WindowType::Count, WindowSize::Count(_count), Some(_)) => {
				// Only trigger when we have enough events for meaningful aggregation
				state.event_count >= self.min_events as u64
			}
			_ => false,
		}
	}

	fn get_rows(&self, rows: &[reifydb_type::RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		unimplemented!()
	}
}
