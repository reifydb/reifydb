// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::{WindowSize, WindowSlide, WindowTimeMode, WindowType},
	interface::catalog::flow::FlowNodeId,
	internal,
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

pub mod rolling;
pub mod sliding;
pub mod tumbling;

use rolling::apply_rolling_window;
use sliding::apply_sliding_window;
use tumbling::apply_tumbling_window;

static EMPTY_PARAMS: Params = Params::None;

use std::sync::LazyLock;

use reifydb_core::{
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
		schema::Schema,
	},
	interface::change::{Change, Diff},
	row::Row,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_runtime::{
	clock::Clock,
	hash::{Hash128, xxh3_128},
};
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, blob::Blob, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use crate::operator::stateful::{raw::RawStatefulOperator, row::RowNumberProvider, window::WindowStateful};

static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

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
		let names: Vec<String> = row.schema.field_names().map(|s| s.to_string()).collect();
		let types: Vec<Type> = row.schema.fields().iter().map(|f| f.constraint.get_type()).collect();

		let mut stored_values = Vec::new();
		for (i, _field) in row.schema.fields().iter().enumerate() {
			let value = row.schema.get_value(&row.encoded, i);
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
		let fields: Vec<reifydb_core::encoded::schema::SchemaField> = self
			.layout_names
			.iter()
			.zip(self.layout_types.iter())
			.map(|(name, ty)| {
				reifydb_core::encoded::schema::SchemaField::unconstrained(name.clone(), ty.clone())
			})
			.collect();

		let layout = Schema::new(fields);
		let encoded = EncodedValues(CowVec::new(self.encoded_bytes.clone()));

		let row = Row {
			number: self.row_number,
			encoded,
			schema: layout,
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
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub compiled_aggregations: Vec<CompiledExpr>,
	pub layout: Schema,
	pub functions: Functions,
	pub row_number_provider: RowNumberProvider,
	pub min_events: usize,               // Minimum events required before window becomes visible
	pub max_window_count: Option<usize>, // Maximum number of windows to keep per group
	pub max_window_age: Option<std::time::Duration>, // Maximum age of windows before expiration
	pub clock: Clock,
}

impl WindowOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		window_type: WindowType,
		size: WindowSize,
		slide: Option<WindowSlide>,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		min_events: usize,
		max_window_count: Option<usize>,
		max_window_age: Option<std::time::Duration>,
		clock: Clock,
		functions: Functions,
	) -> Self {
		let symbol_table = SymbolTable::new();
		let compile_ctx = CompileContext {
			functions: &functions,
			symbol_table: &symbol_table,
		};

		// Compile group_by expressions
		let compiled_group_by: Vec<CompiledExpr> = group_by
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile group_by expression"))
			.collect();

		// Compile aggregation expressions
		let compiled_aggregations: Vec<CompiledExpr> = aggregations
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile aggregation expression"))
			.collect();

		Self {
			parent,
			node,
			window_type,
			size,
			slide,
			group_by,
			aggregations,
			compiled_group_by,
			compiled_aggregations,
			layout: Schema::testing(&[Type::Blob]),
			functions,
			row_number_provider: RowNumberProvider::new(node),
			min_events: min_events.max(1), // Ensure at least 1 event is required
			max_window_count,
			max_window_age,
			clock,
		}
	}

	/// Get the current timestamp in milliseconds
	pub fn current_timestamp(&self) -> u64 {
		self.clock.now_millis()
	}

	/// Compute group keys for all rows in Columns
	pub fn compute_group_keys(&self, columns: &Columns) -> reifydb_type::Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_group_by.is_empty() {
			return Ok(vec![Hash128::from(0u128); row_count]);
		}

		let exec_ctx = EvalContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &self.functions,
			clock: &self.clock,
			arena: None,
			identity: IdentityId::root(),
		};

		let mut group_columns: Vec<Column> = Vec::new();
		for compiled_expr in &self.compiled_group_by {
			let col = compiled_expr.execute(&exec_ctx)?;
			group_columns.push(col);
		}

		let mut hashes = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let mut data = Vec::new();
			for col in &group_columns {
				let value = col.data().get_value(row_idx);
				let value_str = value.to_string();
				data.extend_from_slice(value_str.as_bytes());
			}
			hashes.push(xxh3_128(&data));
		}

		Ok(hashes)
	}

	/// Extract timestamps for all rows in Columns
	pub fn extract_timestamps(&self, columns: &Columns) -> reifydb_type::Result<Vec<u64>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		match &self.window_type {
			WindowType::Time(time_mode) => match time_mode {
				WindowTimeMode::Processing => {
					let now = self.current_timestamp();
					Ok(vec![now; row_count])
				}
				WindowTimeMode::EventTime(column_name) => {
					if let Some(col) = columns.column(column_name) {
						let mut timestamps = Vec::with_capacity(row_count);
						for row_idx in 0..row_count {
							let value = col.data().get_value(row_idx);

							let ts = match value {
								Value::Int8(v) => v as u64,
								Value::Uint8(v) => v,
								Value::Int4(v) => v as u64,
								Value::Uint4(v) => v as u64,
								Value::DateTime(dt) => dt.timestamp_millis() as u64,
								_ => {
									return Err(Error(internal!(
										"Cannot convert {:?} to timestamp",
										value.get_type()
									)));
								}
							};
							timestamps.push(ts);
						}
						Ok(timestamps)
					} else {
						Err(Error(internal!(
							"Event time column '{}' not found in columns",
							column_name
						)))
					}
				}
			},
			WindowType::Count => {
				let now = self.current_timestamp();
				Ok(vec![now; row_count])
			}
		}
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
	pub fn extract_timestamp_from_row(&self, row: &Row) -> reifydb_type::Result<u64> {
		match &self.window_type {
			WindowType::Time(time_mode) => match time_mode {
				WindowTimeMode::Processing => Ok(self.current_timestamp()),
				WindowTimeMode::EventTime(column_name) => {
					if let Some(timestamp_index) = row.schema.find_field_index(column_name) {
						let timestamp_value = row.schema.get_i64(&row.encoded, timestamp_index);
						Ok(timestamp_value as u64)
					} else {
						let column_names: Vec<&str> = row.schema.field_names().collect();
						Err(Error(internal!(
							"Event time column '{}' not found in row with columns: {:?}",
							column_name,
							column_names
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
	/// TODO: Refactor to use column-based evaluation when window operator is needed
	pub fn extract_group_values(&self, events: &[WindowEvent]) -> reifydb_type::Result<(Vec<Value>, Vec<String>)> {
		if events.is_empty() || self.group_by.is_empty() {
			return Ok((Vec::new(), Vec::new()));
		}

		// DISABLED: Window operator needs refactoring to use column-based evaluation

		unimplemented!("Window operator extract_group_values needs refactoring to use column-based evaluation")
	}

	/// Convert window events to columnar format for aggregation
	pub fn events_to_columns(&self, events: &[WindowEvent]) -> reifydb_type::Result<Columns> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		let first_event = &events[0];
		let mut columns = Vec::new();

		for (field_idx, (field_name, field_type)) in
			first_event.layout_names.iter().zip(first_event.layout_types.iter()).enumerate()
		{
			let mut column_data = ColumnData::with_capacity(field_type.clone(), events.len());

			for (_event_idx, event) in events.iter().enumerate() {
				let row = event.to_row();
				let value = row.schema.get_value(&row.encoded, field_idx);
				column_data.push_value(value);
			}

			columns.push(Column {
				name: Fragment::internal(field_name.clone()),
				data: column_data,
			});
		}

		Ok(Columns::new(columns))
	}

	/// Apply aggregations to all events in a window
	pub fn apply_aggregations(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		events: &[WindowEvent],
	) -> reifydb_type::Result<Option<(Row, bool)>> {
		if events.is_empty() {
			return Ok(None);
		}

		if self.aggregations.is_empty() {
			// No aggregations configured, return first event as result
			let (result_row_number, is_new) =
				self.row_number_provider.get_or_create_row_number(txn, window_key)?;
			let mut result_row = events[0].to_row();
			result_row.number = result_row_number;
			return Ok(Some((result_row, is_new)));
		}

		let columns = self.events_to_columns(events)?;

		let exec_ctx = EvalContext {
			target: None,
			columns,
			row_count: events.len(),
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: true, // Use aggregate functions for window aggregations
			functions: &self.functions,
			clock: &self.clock,
			arena: None,
			identity: IdentityId::root(),
		};

		let (group_values, group_names) = self.extract_group_values(events)?;

		let mut result_values = Vec::new();
		let mut result_names = Vec::new();
		let mut result_types = Vec::new();

		for (value, name) in group_values.into_iter().zip(group_names.into_iter()) {
			result_values.push(value.clone());
			result_names.push(name);
			result_types.push(value.get_type());
		}

		for (i, compiled_aggregation) in self.compiled_aggregations.iter().enumerate() {
			let agg_column = compiled_aggregation.execute(&exec_ctx)?;

			let value = agg_column.data().get_value(0);
			result_values.push(value.clone());
			result_names.push(column_name_from_expression(&self.aggregations[i]).text().to_string());
			result_types.push(value.get_type());
		}

		let fields: Vec<reifydb_core::encoded::schema::SchemaField> = result_names
			.iter()
			.zip(result_types.iter())
			.map(|(name, ty)| {
				reifydb_core::encoded::schema::SchemaField::unconstrained(name.clone(), ty.clone())
			})
			.collect();
		let layout = Schema::new(fields);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &result_values);

		let (result_row_number, is_new) = self.row_number_provider.get_or_create_row_number(txn, window_key)?;

		let result_row = Row {
			number: result_row_number,
			encoded,
			schema: layout,
		};

		Ok(Some((result_row, is_new)))
	}

	/// Process expired windows and clean up state
	pub fn process_expired_windows(
		&self,
		txn: &mut FlowTransaction,
		current_timestamp: u64,
	) -> reifydb_type::Result<Vec<Diff>> {
		let result = Vec::new();

		if let (WindowType::Time(_), WindowSize::Duration(duration)) = (&self.window_type, &self.size) {
			let window_size_ms = duration.as_millis() as u64;
			let expire_before = current_timestamp.saturating_sub(window_size_ms * 2); // Keep 2 window sizes

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
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
	) -> reifydb_type::Result<WindowState> {
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		postcard::from_bytes(blob.as_ref())
			.map_err(|e| Error(internal!("Failed to deserialize WindowState: {}", e)))
	}

	/// Save window state to storage
	pub fn save_window_state(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> reifydb_type::Result<()> {
		let serialized = postcard::to_stdvec(state)
			.map_err(|e| Error(internal!("Failed to serialize WindowState: {}", e)))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, window_key, state_row)
	}

	/// Get and increment global event count for count-based windows
	pub fn get_and_increment_global_count(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
	) -> reifydb_type::Result<u64> {
		let count_key = self.create_count_key(group_hash);
		let count_row = self.load_state(txn, &count_key)?;

		let current_count = if count_row.is_empty() || !count_row.is_defined(0) {
			0
		} else {
			let blob = self.layout.get_blob(&count_row, 0);
			if blob.is_empty() {
				0
			} else {
				postcard::from_bytes(blob.as_ref()).unwrap_or(0)
			}
		};

		let new_count = current_count + 1;

		let serialized = postcard::to_stdvec(&new_count)
			.map_err(|e| Error(internal!("Failed to serialize count: {}", e)))?;

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

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> Schema {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> reifydb_type::Result<Change> {
		// We'll need to refactor the architecture to support this properly.

		match &self.slide {
			Some(WindowSlide::Rolling) => apply_rolling_window(self, txn, change),
			Some(_) => apply_sliding_window(self, txn, change),
			None => apply_tumbling_window(self, txn, change),
		}
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
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

			// but allow multiple triggers as the window slides
			(WindowType::Time(_), WindowSize::Duration(duration), Some(_)) => {
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
				state.event_count >= self.min_events as u64
			}
			_ => false,
		}
	}
}
