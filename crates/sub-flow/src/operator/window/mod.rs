// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::{CommitVersion, WindowKind, WindowSize},
	error::diagnostic::flow::{flow_window_timestamp_column_not_found, flow_window_timestamp_column_type_mismatch},
	interface::catalog::flow::FlowNodeId,
	internal,
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

pub mod rolling;
pub mod session;
pub mod sliding;
pub mod tumbling;

use rolling::apply_rolling_window;
use session::apply_session_window;
use sliding::apply_sliding_window;
use tumbling::apply_tumbling_window;

static EMPTY_PARAMS: Params = Params::None;

use std::{ops, sync::LazyLock, time::Duration};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		schema::{RowSchema, RowSchemaField},
	},
	interface::change::{Change, Diff},
	row::Row,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalSession},
	},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::{
	Expression,
	name::{collect_all_column_names, column_name_from_expression},
};
use reifydb_runtime::{
	context::RuntimeContext,
	hash::{Hash128, xxh3_128},
};
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, blob::Blob, datetime::DateTime, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use crate::operator::stateful::{raw::RawStatefulOperator, row::RowNumberProvider, window::WindowStateful};

static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

/// RowSchema layout shared across all events in a window (stored once, not per event)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowLayout {
	pub names: Vec<String>,
	pub types: Vec<Type>,
}

impl WindowLayout {
	pub fn from_row(row: &Row) -> Self {
		Self {
			names: row.schema.field_names().map(|s| s.to_string()).collect(),
			types: row.schema.fields().iter().map(|f| f.constraint.get_type()).collect(),
		}
	}

	pub fn to_schema(&self) -> RowSchema {
		let fields: Vec<RowSchemaField> = self
			.names
			.iter()
			.zip(self.types.iter())
			.map(|(name, ty)| RowSchemaField::unconstrained(name.clone(), ty.clone()))
			.collect();
		RowSchema::new(fields)
	}
}

/// A single event stored within a window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEvent {
	pub row_number: RowNumber,
	pub timestamp: u64,
	#[serde(with = "serde_bytes")]
	pub encoded_bytes: Vec<u8>,
}

impl WindowEvent {
	pub fn from_row(row: &Row, timestamp: u64) -> Self {
		Self {
			row_number: row.number,
			timestamp,
			encoded_bytes: row.encoded.as_slice().to_vec(),
		}
	}

	pub fn to_row(&self, layout: &WindowLayout) -> Row {
		let schema = layout.to_schema();
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));
		Row {
			number: self.row_number,
			encoded,
			schema,
		}
	}
}

/// State for a single window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowState {
	/// All events in this window (stored in insertion order)
	pub events: Vec<WindowEvent>,
	/// RowSchema layout shared by all events (set on first event)
	pub window_layout: Option<WindowLayout>,
	/// Window creation timestamp
	pub window_start: u64,
	/// Count of events in window (for count-based windows)
	pub event_count: u64,
	/// Timestamp of last event (for session windows)
	pub last_event_time: u64,
}

impl WindowState {
	/// Get the layout, panics if not set (should always be set after first event)
	pub fn layout(&self) -> &WindowLayout {
		self.window_layout.as_ref().expect("WindowState layout must be set before accessing")
	}
}

impl Default for WindowState {
	fn default() -> Self {
		Self {
			events: Vec::new(),
			window_layout: None,
			window_start: 0,
			event_count: 0,
			last_event_time: 0,
		}
	}
}

/// The main window operator
pub struct WindowOperator {
	pub parent: Arc<Operators>,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub compiled_aggregations: Vec<CompiledExpr>,
	pub layout: RowSchema,
	pub functions: Functions,
	pub row_number_provider: RowNumberProvider,
	pub runtime_context: RuntimeContext,
	/// Column names needed by group_by + aggregations expressions.
	/// When empty, no projection is applied (all columns stored).
	pub projected_columns: Vec<String>,
}

impl WindowOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		kind: WindowKind,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		ts: Option<String>,
		runtime_context: RuntimeContext,
		functions: Functions,
	) -> Self {
		let symbols = SymbolTable::new();
		let compile_ctx = CompileContext {
			functions: &functions,
			symbols: &symbols,
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

		let mut needed = collect_all_column_names(&group_by);
		needed.extend(collect_all_column_names(&aggregations));
		let mut projected_columns: Vec<String> = needed.into_iter().collect();
		projected_columns.sort();

		Self {
			parent,
			node,
			kind,
			group_by,
			aggregations,
			ts,
			compiled_group_by,
			compiled_aggregations,
			layout: RowSchema::testing(&[Type::Blob]),
			functions,
			row_number_provider: RowNumberProvider::new(node),
			runtime_context,
			projected_columns,
		}
	}

	/// Get the current timestamp in milliseconds
	pub fn current_timestamp(&self) -> u64 {
		self.runtime_context.clock.now_millis()
	}

	/// Project a single-row Columns down to only the columns needed by window expressions.
	pub fn project_columns(&self, columns: &Columns) -> Columns {
		if self.projected_columns.is_empty() {
			return columns.clone();
		}
		columns.project_by_names(&self.projected_columns)
	}

	/// Whether this is a count-based window
	pub fn is_count_based(&self) -> bool {
		self.kind.size().map_or(false, |m| m.is_count())
	}

	/// Get the window size as duration (if time-based)
	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	/// Get the window size as count (if count-based)
	pub fn size_count(&self) -> Option<u64> {
		self.kind.size().and_then(|m| m.as_count())
	}

	fn eval_session(&self, is_aggregate: bool) -> EvalSession<'_> {
		EvalSession {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			functions: &self.functions,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: is_aggregate,
		}
	}

	/// Compute group keys for all rows in Columns
	pub fn compute_group_keys(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_group_by.is_empty() {
			return Ok(vec![Hash128::from(0u128); row_count]);
		}

		let session = self.eval_session(false);
		let exec_ctx = session.eval(columns.clone(), row_count);

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

	/// Resolve event timestamps for all rows.
	/// When `ts` is configured, reads from the named DateTime column.
	/// Otherwise falls back to processing time (current clock).
	pub fn resolve_event_timestamps(&self, columns: &Columns, row_count: usize) -> Result<Vec<u64>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		match &self.ts {
			Some(ts_col) => {
				let col = columns
					.column(ts_col)
					.ok_or_else(|| Error(flow_window_timestamp_column_not_found(ts_col)))?;
				let mut timestamps = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match col.data().get_value(i) {
						Value::DateTime(dt) => timestamps.push(dt.timestamp_millis() as u64),
						other => {
							return Err(Error(flow_window_timestamp_column_type_mismatch(
								ts_col,
								other.get_type(),
							)));
						}
					}
				}
				Ok(timestamps)
			}
			None => {
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

	/// Create a row index key for mapping row_number → window_id
	fn create_row_index_key(&self, group_hash: Hash128, row_number: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"idx:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(row_number.0);
		EncodedKey::new(serializer.finish())
	}

	/// Store a row_number → window_ids mapping.
	/// Appends window_id to the existing list (supports sliding windows with multiple windows per event).
	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let index_key = self.create_row_index_key(group_hash, row_number);
		let mut window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if !window_ids.contains(&window_id) {
			window_ids.push(window_id);
		}
		let serialized =
			to_stdvec(&window_ids).map_err(|e| Error(internal!("Failed to serialize row index: {}", e)))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &index_key, state_row)
	}

	/// Look up all window_ids for a given row_number
	fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let index_key = self.create_row_index_key(group_hash, row_number);
		let state_row = self.load_state(txn, &index_key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(Vec::new());
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(Vec::new());
		}
		let window_ids: Vec<u64> = from_bytes(blob.as_ref())
			.map_err(|e| Error(internal!("Failed to deserialize row index: {}", e)))?;
		Ok(window_ids)
	}

	/// Replace an event across all its windows in-place (for UPDATE handling).
	/// For sliding windows, an event may exist in multiple windows — all are updated.
	fn replace_event_in_windows(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		new_row: &Row,
		new_timestamp: u64,
	) -> Result<Vec<Diff>> {
		let window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if window_ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		for window_id in &window_ids {
			let window_key = self.create_window_key(group_hash, *window_id);
			let mut window_state = self.load_window_state(txn, &window_key)?;

			let event_idx = window_state.events.iter().position(|e| e.row_number == row_number);
			if let Some(idx) = event_idx {
				let layout = match &window_state.window_layout {
					Some(l) => l.clone(),
					None => continue,
				};

				let old_aggregation =
					self.apply_aggregations(txn, &window_key, &layout, &window_state.events)?;

				window_state.events[idx] = WindowEvent::from_row(new_row, new_timestamp);

				let new_aggregation =
					self.apply_aggregations(txn, &window_key, &layout, &window_state.events)?;

				self.save_window_state(txn, &window_key, &window_state)?;

				if let (Some((old_row, _)), Some((new_row, _))) = (old_aggregation, new_aggregation) {
					result.push(Diff::Update {
						pre: Columns::from_row(&old_row),
						post: Columns::from_row(&new_row),
					});
				}
			}
		}

		Ok(result)
	}

	/// Process Update diffs by replacing events in-place within their windows.
	fn process_event_updates(&self, txn: &mut FlowTransaction, pre: &Columns, post: &Columns) -> Result<Vec<Diff>> {
		let row_count = pre.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let group_hashes = self.compute_group_keys(pre)?;
		let post_timestamps = self.resolve_event_timestamps(post, row_count)?;
		let mut result = Vec::new();

		for row_idx in 0..row_count {
			let row_number = pre.row_numbers[row_idx];
			let group_hash = group_hashes[row_idx];
			let new_timestamp = post_timestamps[row_idx];

			let single_row = post.extract_row(row_idx);
			let projected = self.project_columns(&single_row);
			let new_row = projected.to_single_row();

			let diffs =
				self.replace_event_in_windows(txn, group_hash, row_number, &new_row, new_timestamp)?;
			result.extend(diffs);
		}

		Ok(result)
	}

	/// Remove an event from all its windows (for DELETE handling).
	fn remove_event_from_windows(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<Diff>> {
		let window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if window_ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		for window_id in &window_ids {
			let window_key = self.create_window_key(group_hash, *window_id);
			let mut window_state = self.load_window_state(txn, &window_key)?;

			let event_idx = window_state.events.iter().position(|e| e.row_number == row_number);
			if let Some(idx) = event_idx {
				let layout = match &window_state.window_layout {
					Some(l) => l.clone(),
					None => continue,
				};

				let old_aggregation =
					self.apply_aggregations(txn, &window_key, &layout, &window_state.events)?;

				window_state.events.remove(idx);
				window_state.event_count = window_state.event_count.saturating_sub(1);

				if window_state.events.is_empty() {
					self.save_window_state(txn, &window_key, &window_state)?;
					if let Some((old_row, _)) = old_aggregation {
						result.push(Diff::Remove {
							pre: Columns::from_row(&old_row),
						});
					}
				} else {
					let new_aggregation = self.apply_aggregations(
						txn,
						&window_key,
						&layout,
						&window_state.events,
					)?;
					self.save_window_state(txn, &window_key, &window_state)?;

					if let (Some((old_row, _)), Some((new_row, _))) =
						(old_aggregation, new_aggregation)
					{
						result.push(Diff::Update {
							pre: Columns::from_row(&old_row),
							post: Columns::from_row(&new_row),
						});
					}
				}
			}
		}

		// Clean up the index entry
		let index_key = self.create_row_index_key(group_hash, row_number);
		let empty = self.layout.allocate();
		self.save_state(txn, &index_key, empty)?;

		Ok(result)
	}

	/// Process Remove diffs by removing events from their windows.
	fn process_event_removals(&self, txn: &mut FlowTransaction, pre: &Columns) -> Result<Vec<Diff>> {
		let row_count = pre.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let group_hashes = self.compute_group_keys(pre)?;
		let mut result = Vec::new();

		for row_idx in 0..row_count {
			let row_number = pre.row_numbers[row_idx];
			let group_hash = group_hashes[row_idx];

			let diffs = self.remove_event_from_windows(txn, group_hash, row_number)?;
			result.extend(diffs);
		}

		Ok(result)
	}

	/// Extract group values from window events (all events in a group have the same group values).
	/// Evaluates compiled_group_by expressions on the first row of the events.
	pub fn extract_group_values(
		&self,
		window_layout: &WindowLayout,
		events: &[WindowEvent],
	) -> Result<(Vec<Value>, Vec<String>)> {
		if events.is_empty() || self.group_by.is_empty() {
			return Ok((Vec::new(), Vec::new()));
		}

		let columns = self.events_to_columns(window_layout, events)?;
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok((Vec::new(), Vec::new()));
		}

		let session = self.eval_session(false);
		let exec_ctx = session.eval(columns, row_count);

		let mut values = Vec::new();
		let mut names = Vec::new();
		for (i, compiled_expr) in self.compiled_group_by.iter().enumerate() {
			let col = compiled_expr.execute(&exec_ctx)?;
			values.push(col.data().get_value(0).clone());
			names.push(column_name_from_expression(&self.group_by[i]).text().to_string());
		}

		Ok((values, names))
	}

	/// Convert window events to columnar format for aggregation
	pub fn events_to_columns(&self, window_layout: &WindowLayout, events: &[WindowEvent]) -> Result<Columns> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		let mut builders: Vec<ColumnData> = window_layout
			.types
			.iter()
			.map(|ty| ColumnData::with_capacity(ty.clone(), events.len()))
			.collect();

		for event in events.iter() {
			let row = event.to_row(window_layout);
			for (idx, builder) in builders.iter_mut().enumerate() {
				let value = row.schema.get_value(&row.encoded, idx);
				builder.push_value(value);
			}
		}

		let columns = window_layout
			.names
			.iter()
			.zip(builders.into_iter())
			.map(|(name, data)| Column {
				name: Fragment::internal(name.clone()),
				data,
			})
			.collect();

		Ok(Columns::new(columns))
	}

	/// Apply aggregations to all events in a window
	pub fn apply_aggregations(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		window_layout: &WindowLayout,
		events: &[WindowEvent],
	) -> Result<Option<(Row, bool)>> {
		if events.is_empty() {
			return Ok(None);
		}

		if self.aggregations.is_empty() {
			// No aggregations configured, return first event as result
			let (result_row_number, is_new) =
				self.row_number_provider.get_or_create_row_number(txn, window_key)?;
			let mut result_row = events[0].to_row(window_layout);
			result_row.number = result_row_number;
			return Ok(Some((result_row, is_new)));
		}

		let columns = self.events_to_columns(window_layout, events)?;

		let agg_session = self.eval_session(true);
		let exec_ctx = agg_session.eval(columns, events.len());

		let (group_values, group_names) = self.extract_group_values(window_layout, events)?;

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

		let fields: Vec<RowSchemaField> = result_names
			.iter()
			.zip(result_types.iter())
			.map(|(name, ty)| RowSchemaField::unconstrained(name.clone(), ty.clone()))
			.collect();
		let layout = RowSchema::new(fields);
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

	/// Process expired windows: emit Remove diffs for each, then delete state.
	/// Uses the group registry for per-group targeted expiration.
	pub fn process_expired_windows(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();

		if let Some(duration) = self.size_duration() {
			let window_size_ms = duration.as_millis() as u64;
			if window_size_ms > 0 {
				let expire_before = current_timestamp.saturating_sub(window_size_ms * 2);
				let cutoff_id = expire_before / window_size_ms;
				if cutoff_id == 0 {
					return Ok(result);
				}

				let groups = self.load_group_registry(txn)?;
				for group_hash in &groups {
					// Keycode uses inverted ordering (NOT of big-endian)
					let low_key = self.create_window_key(*group_hash, cutoff_id);
					let high_key = self.create_window_key(*group_hash, 0);
					let range = EncodedKeyRange::new(
						ops::Bound::Excluded(low_key),
						ops::Bound::Included(high_key),
					);

					let expired_keys = self.scan_keys_in_range(txn, &range)?;
					for key in &expired_keys {
						let window_state = self.load_window_state(txn, key)?;
						if !window_state.events.is_empty() {
							if let Some(layout) = &window_state.window_layout {
								if let Some((row, _)) = self.apply_aggregations(
									txn,
									key,
									layout,
									&window_state.events,
								)? {
									result.push(Diff::Remove {
										pre: Columns::from_row(&row),
									});
								}
							}
						}
					}

					if !expired_keys.is_empty() {
						let low_key = self.create_window_key(*group_hash, cutoff_id);
						let high_key = self.create_window_key(*group_hash, 0);
						let range = EncodedKeyRange::new(
							ops::Bound::Excluded(low_key),
							ops::Bound::Included(high_key),
						);
						let _ = self.expire_range(txn, range)?;
					}
				}
			}
		}

		Ok(result)
	}

	/// Load window state from storage
	pub fn load_window_state(&self, txn: &mut FlowTransaction, window_key: &EncodedKey) -> Result<WindowState> {
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		from_bytes(blob.as_ref()).map_err(|e| Error(internal!("Failed to deserialize WindowState: {}", e)))
	}

	/// Save window state to storage
	pub fn save_window_state(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> Result<()> {
		let serialized =
			to_stdvec(state).map_err(|e| Error(internal!("Failed to serialize WindowState: {}", e)))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, window_key, state_row)
	}

	/// Get and increment global event count for count-based windows
	pub fn get_and_increment_global_count(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<u64> {
		let count_key = self.create_count_key(group_hash);
		let count_row = self.load_state(txn, &count_key)?;

		let current_count = if count_row.is_empty() || !count_row.is_defined(0) {
			0
		} else {
			let blob = self.layout.get_blob(&count_row, 0);
			if blob.is_empty() {
				0
			} else {
				from_bytes(blob.as_ref()).unwrap_or(0)
			}
		};

		let new_count = current_count + 1;

		let serialized =
			to_stdvec(&new_count).map_err(|e| Error(internal!("Failed to serialize count: {}", e)))?;

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

	/// Create the group registry key
	fn create_group_registry_key(&self) -> EncodedKey {
		EncodedKey::new(b"grp:")
	}

	/// Load the set of active group hashes from the registry.
	pub fn load_group_registry(&self, txn: &mut FlowTransaction) -> Result<Vec<Hash128>> {
		let key = self.create_group_registry_key();
		let state_row = self.load_state(txn, &key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(Vec::new());
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(Vec::new());
		}
		let groups: Vec<u128> = from_bytes(blob.as_ref()).unwrap_or_default();
		Ok(groups.into_iter().map(Hash128::from).collect())
	}

	/// Save the group registry.
	fn save_group_registry(&self, txn: &mut FlowTransaction, groups: &[Hash128]) -> Result<()> {
		let key = self.create_group_registry_key();
		let raw: Vec<u128> = groups.iter().map(|h| (*h).into()).collect();
		let serialized =
			to_stdvec(&raw).map_err(|e| Error(internal!("Failed to serialize group registry: {}", e)))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &key, state_row)
	}

	/// Register a group hash in the registry if not already present.
	pub fn register_group(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<()> {
		let mut groups = self.load_group_registry(txn)?;
		if !groups.contains(&group_hash) {
			groups.push(group_hash);
			self.save_group_registry(txn, &groups)?;
		}
		Ok(())
	}

	/// Tick-based window expiration for tumbling/sliding windows.
	/// Scans all operator state, finds expired "win:" windows, emits Remove and cleans up.
	pub fn tick_expire_windows(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let window_size_ms = match self.size_duration() {
			Some(d) => d.as_millis() as u64,
			None => return Ok(result),
		};
		if window_size_ms == 0 {
			return Ok(result);
		}

		// Scan all state for this operator
		let all_state = txn.state_scan(self.node)?;
		let prefix = FlowNodeStateKey::new(self.node, vec![]).encode();
		let win_marker = b"win:";

		let mut keys_to_remove = Vec::new();

		for item in &all_state.items {
			// Strip operator prefix to get the inner key
			let full_key = &item.key;
			if full_key.len() <= prefix.len() {
				continue;
			}
			let inner = &full_key[prefix.len()..];

			// Only process "win:" keys
			if !inner.starts_with(win_marker) {
				continue;
			}

			let window_key = EncodedKey::new(inner);
			let window_state = self.load_window_state(txn, &window_key)?;
			if window_state.events.is_empty() {
				continue;
			}

			// Check if window is expired: newest event older than window size
			let newest_event_time = window_state.events.iter().map(|e| e.timestamp).max().unwrap_or(0);
			if current_timestamp.saturating_sub(newest_event_time) > window_size_ms {
				if let Some(layout) = &window_state.window_layout {
					if let Some((row, _)) =
						self.apply_aggregations(txn, &window_key, layout, &window_state.events)?
					{
						result.push(Diff::Remove {
							pre: Columns::from_row(&row),
						});
					}
				}
				keys_to_remove.push(window_key);
			}
		}

		// Clean up expired windows
		for key in &keys_to_remove {
			let empty = self.create_state();
			self.save_state(txn, key, empty)?;
		}

		Ok(result)
	}

	/// Shared: partition columns by group keys and call `group_fn` for each group.
	pub fn process_insert(
		&self,
		txn: &mut FlowTransaction,
		columns: &Columns,
		group_fn: impl Fn(&WindowOperator, &mut FlowTransaction, &Columns, Hash128) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		let group_hashes = self.compute_group_keys(columns)?;
		let groups = columns.partition_by_keys(&group_hashes);
		let mut result = Vec::new();
		for (group_hash, group_columns) in groups {
			self.register_group(txn, group_hash)?;
			let group_result = group_fn(self, txn, &group_columns, group_hash)?;
			result.extend(group_result);
		}
		Ok(result)
	}

	/// Shared: iterate change diffs and process inserts/updates via `process_fn`.
	/// Optionally runs expiration first (all kinds except rolling).
	pub fn apply_window_change(
		&self,
		txn: &mut FlowTransaction,
		change: &Change,
		expire: bool,
		process_fn: impl Fn(&WindowOperator, &mut FlowTransaction, &Columns) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		if expire {
			let current_timestamp = self.current_timestamp();
			let expired_diffs = self.process_expired_windows(txn, current_timestamp)?;
			result.extend(expired_diffs);
		}
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					result.extend(process_fn(self, txn, post)?);
				}
				Diff::Update {
					pre,
					post,
				} => {
					result.extend(self.process_event_updates(txn, pre, post)?);
				}
				Diff::Remove {
					pre,
				} => {
					result.extend(self.process_event_removals(txn, pre)?);
				}
			}
		}
		Ok(result)
	}

	/// Shared: emit an Insert or Update diff for an aggregation result.
	/// `previous_aggregation` is the pre-update state (if the window already existed).
	pub fn emit_aggregation_diff(
		aggregated_row: &Row,
		is_new: bool,
		previous_aggregation: Option<(Row, bool)>,
	) -> Diff {
		if is_new {
			Diff::Insert {
				post: Columns::from_row(aggregated_row),
			}
		} else if let Some((previous_row, _)) = previous_aggregation {
			Diff::Update {
				pre: Columns::from_row(&previous_row),
				post: Columns::from_row(aggregated_row),
			}
		} else {
			Diff::Insert {
				post: Columns::from_row(aggregated_row),
			}
		}
	}
}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> RowSchema {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_window(self, txn, change),
			WindowKind::Sliding {
				..
			} => apply_sliding_window(self, txn, change),
			WindowKind::Rolling {
				..
			} => apply_rolling_window(self, txn, change),
			WindowKind::Session {
				..
			} => apply_session_window(self, txn, change),
		}
	}

	fn tick(&self, txn: &mut FlowTransaction, timestamp: DateTime) -> Result<Option<Change>> {
		let current_timestamp = (timestamp.to_nanos() / 1_000_000) as u64;
		let diffs = match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			} => self.tick_expire_windows(txn, current_timestamp)?,
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
			} => self.tick_rolling_eviction(txn, current_timestamp)?,
			WindowKind::Session {
				..
			} => self.tick_session_expiration(txn, current_timestamp)?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Change::from_flow(self.node, CommitVersion(0), diffs)))
		}
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
