// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::{CommitVersion, WindowKind, WindowSize},
	error::diagnostic::flow::{flow_window_timestamp_column_not_found, flow_window_timestamp_column_type_mismatch},
	interface::catalog::flow::FlowNodeId,
	internal,
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_sdk::operator::Tick;
use serde::{Deserialize, Serialize};

use crate::{
	operator::{Operator, OperatorCell},
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
		shape::{RowShape, RowShapeField},
	},
	interface::change::{Change, Diff},
	row::Row,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupByView},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::{
	Accumulator, AggregateFunctionCapability, context::FunctionContext, registry::Routines,
};
use reifydb_rql::expression::{
	Expression,
	name::{collect_all_column_names, display_label},
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

#[inline]
fn build_aggregation_shape(names: &[String], types: &[Type]) -> RowShape {
	let fields: Vec<RowShapeField> = names
		.iter()
		.zip(types.iter())
		.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
		.collect();
	RowShape::new(fields)
}

static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowLayout {
	pub names: Vec<String>,
	pub types: Vec<Type>,
}

impl WindowLayout {
	pub fn from_row(row: &Row) -> Self {
		Self {
			names: row.shape.field_names().map(|s| s.to_string()).collect(),
			types: row.shape.fields().iter().map(|f| f.constraint.get_type()).collect(),
		}
	}

	pub fn to_shape(&self) -> RowShape {
		let fields: Vec<RowShapeField> = self
			.names
			.iter()
			.zip(self.types.iter())
			.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
			.collect();
		RowShape::new(fields)
	}
}

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
		let shape = layout.to_shape();
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));
		Row {
			number: self.row_number,
			encoded,
			shape,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindowState {
	pub events: Vec<WindowEvent>,

	pub window_layout: Option<WindowLayout>,

	pub window_start: u64,

	pub event_count: u64,

	pub last_event_time: u64,

	pub running_totals: Vec<Value>,
}

impl WindowState {
	pub fn layout(&self) -> &WindowLayout {
		self.window_layout.as_ref().expect("WindowState layout must be set before accessing")
	}
}

pub struct WindowConfig {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
}

#[derive(Clone, Debug)]
pub struct FastAgg {
	pub function_name: String,
	pub input_column: Option<String>,
}

fn detect_fast_agg(routines: &Routines, expr: &Expression) -> Option<FastAgg> {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let call = match inner {
		Expression::Call(c) => c,
		_ => return None,
	};
	let function_name = call.func.0.text().to_string();
	let func = routines.get_aggregate_function(&function_name)?;
	if !func.aggregate_capabilities().contains(&AggregateFunctionCapability::Retractable) {
		return None;
	}
	let input_column = match call.args.as_slice() {
		[] => None,
		[Expression::Column(col)] => Some(col.0.name.text().to_string()),
		_ => return None,
	};
	Some(FastAgg {
		function_name,
		input_column,
	})
}

fn make_single_row_columns_for_agg(layout: &WindowLayout, row: &Row, kind: &FastAgg) -> Option<Columns> {
	match &kind.input_column {
		Some(col_name) => {
			let col_idx = layout.names.iter().position(|n| n == col_name)?;
			let ty = layout.types[col_idx].clone();
			let value = row.shape.get_value(&row.encoded, col_idx);
			let mut buf = ColumnBuffer::with_capacity(ty, 1);
			buf.push_value(value);
			Some(Columns::new(vec![ColumnWithName {
				name: Fragment::internal(col_name.clone()),
				data: buf,
			}]))
		}
		None => {
			let mut buf = ColumnBuffer::with_capacity(Type::Int4, 1);
			buf.push_value(Value::Int4(0));
			Some(Columns::new(vec![ColumnWithName {
				name: Fragment::internal("dummy"),
				data: buf,
			}]))
		}
	}
}

fn single_group_view() -> GroupByView {
	let mut view = GroupByView::new();
	view.insert(Vec::new(), vec![0]);
	view
}

fn events_to_agg_columns(layout: &WindowLayout, events: &[WindowEvent], kind: &FastAgg) -> Option<Columns> {
	let shape = layout.to_shape();
	match &kind.input_column {
		Some(col_name) => {
			let col_idx = layout.names.iter().position(|n| n == col_name)?;
			let ty = layout.types[col_idx].clone();
			let mut buf = ColumnBuffer::with_capacity(ty, events.len());
			for ev in events {
				let encoded = EncodedRow(CowVec::new(ev.encoded_bytes.clone()));
				let value = shape.get_value(&encoded, col_idx);
				buf.push_value(value);
			}
			Some(Columns::new(vec![ColumnWithName {
				name: Fragment::internal(col_name.clone()),
				data: buf,
			}]))
		}
		None => {
			let mut buf = ColumnBuffer::with_capacity(Type::Int4, events.len());
			for _ in events {
				buf.push_value(Value::Int4(0));
			}
			Some(Columns::new(vec![ColumnWithName {
				name: Fragment::internal("dummy"),
				data: buf,
			}]))
		}
	}
}

fn events_group_view(event_count: usize) -> GroupByView {
	let mut view = GroupByView::new();
	view.insert(Vec::new(), (0..event_count).collect());
	view
}

pub struct WindowOperator {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub compiled_aggregations: Vec<CompiledExpr>,
	pub layout: RowShape,
	pub routines: Routines,
	pub row_number_provider: RowNumberProvider,
	pub runtime_context: RuntimeContext,

	pub projected_columns: Vec<String>,

	pub fast_aggregations: Option<Vec<FastAgg>>,
	pub agg_output_names: Vec<String>,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Self {
		let symbols = SymbolTable::new();
		let compile_ctx = CompileContext {
			symbols: &symbols,
		};

		let compiled_group_by: Vec<CompiledExpr> = config
			.group_by
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile group_by expression"))
			.collect();

		let compiled_aggregations: Vec<CompiledExpr> = config
			.aggregations
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile aggregation expression"))
			.collect();

		let mut needed = collect_all_column_names(&config.group_by);
		needed.extend(collect_all_column_names(&config.aggregations));
		let mut projected_columns: Vec<String> = needed.into_iter().collect();
		projected_columns.sort();

		let detected: Vec<Option<FastAgg>> =
			config.aggregations.iter().map(|e| detect_fast_agg(&config.routines, e)).collect();
		let fast_aggregations = if !detected.is_empty() && detected.iter().all(Option::is_some) {
			Some(detected.into_iter().map(Option::unwrap).collect())
		} else {
			None
		};
		let agg_output_names: Vec<String> =
			config.aggregations.iter().map(|e| display_label(e).text().to_string()).collect();

		Self {
			parent: config.parent,
			node: config.node,
			kind: config.kind,
			group_by: config.group_by,
			aggregations: config.aggregations,
			ts: config.ts,
			compiled_group_by,
			compiled_aggregations,
			layout: RowShape::operator_state(),
			routines: config.routines,
			row_number_provider: RowNumberProvider::new(config.node),
			runtime_context: config.runtime_context,
			projected_columns,
			fast_aggregations,
			agg_output_names,
		}
	}

	pub fn current_timestamp(&self) -> u64 {
		self.runtime_context.clock.now_millis()
	}

	pub fn project_columns(&self, columns: &Columns) -> Columns {
		if self.projected_columns.is_empty() {
			return columns.clone();
		}
		columns.project_by_names(&self.projected_columns)
	}

	pub fn is_count_based(&self) -> bool {
		self.kind.size().is_some_and(|m| m.is_count())
	}

	pub fn is_rolling(&self) -> bool {
		matches!(self.kind, WindowKind::Rolling { .. })
	}

	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	pub fn size_count(&self) -> Option<u64> {
		self.kind.size().and_then(|m| m.as_count())
	}

	fn eval_session(&self, is_aggregate: bool) -> EvalContext<'_> {
		EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: is_aggregate,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		}
	}

	pub fn compute_group_keys(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_group_by.is_empty() {
			return Ok(vec![Hash128::from(0u128); row_count]);
		}

		let session = self.eval_session(false);
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut group_columns: Vec<ColumnWithName> = Vec::new();
		for compiled_expr in &self.compiled_group_by {
			let col = compiled_expr.execute(&exec_ctx)?;
			group_columns.push(col);
		}

		let mut hashes = Vec::with_capacity(row_count);
		let mut buf = Vec::with_capacity(128);
		for row_idx in 0..row_count {
			buf.clear();
			for col in &group_columns {
				let value = col.data().get_value(row_idx);
				let bytes = to_stdvec(&value).map_err(|e| {
					Error(Box::new(internal!("Failed to encode group-by value: {}", e)))
				})?;
				buf.extend_from_slice(&bytes);
			}
			hashes.push(xxh3_128(&buf));
		}

		Ok(hashes)
	}

	pub fn resolve_event_timestamps(&self, columns: &Columns, row_count: usize) -> Result<Vec<u64>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		match &self.ts {
			Some(ts_col) => {
				let col = columns.column(ts_col).ok_or_else(|| {
					Error(Box::new(flow_window_timestamp_column_not_found(ts_col)))
				})?;
				let mut timestamps = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match col.data().get_value(i) {
						Value::DateTime(dt) => timestamps.push(dt.timestamp_millis() as u64),
						other => {
							return Err(Error(Box::new(
								flow_window_timestamp_column_type_mismatch(
									ts_col,
									other.get_type(),
								),
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

	pub fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		serializer.finish()
	}

	fn create_row_index_key(&self, group_hash: Hash128, row_number: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"idx:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(row_number.0);
		serializer.finish()
	}

	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		if self.is_rolling() {
			return Ok(());
		}
		let index_key = self.create_row_index_key(group_hash, row_number);
		let mut window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if !window_ids.contains(&window_id) {
			window_ids.push(window_id);
		}
		let serialized = to_stdvec(&window_ids)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize row index: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &index_key, state_row)
	}

	fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		if self.is_rolling() {
			let window_key = self.create_window_key(group_hash, 0);
			let window_state = self.load_window_state(txn, &window_key)?;
			return Ok(if window_state.events.iter().any(|e| e.row_number == row_number) {
				vec![0]
			} else {
				Vec::new()
			});
		}
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
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize row index: {}", e))))?;
		Ok(window_ids)
	}

	fn replace_event_in_windows(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		post_row: &Row,
		post_timestamp: u64,
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

				let changed_at = DateTime::from_nanos(post_timestamp);
				let pre_aggregation = self.apply_aggregations(
					txn,
					&window_key,
					&layout,
					&window_state.events,
					changed_at,
					&window_state,
				)?;

				let pre_event_row = window_state.events[idx].to_row(&layout);
				window_state.events[idx] = WindowEvent::from_row(post_row, post_timestamp);
				self.update_running_totals_on_evict(&mut window_state, &pre_event_row);
				self.update_running_totals_on_push(&mut window_state, post_row);

				let post_aggregation = self.apply_aggregations(
					txn,
					&window_key,
					&layout,
					&window_state.events,
					changed_at,
					&window_state,
				)?;

				self.save_window_state(txn, &window_key, &window_state)?;

				if let (Some((pre_row, _)), Some((post_row, _))) = (pre_aggregation, post_aggregation) {
					result.push(Diff::update(
						Columns::from_row(&pre_row),
						Columns::from_row(&post_row),
					));
				}
			}
		}

		Ok(result)
	}

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
			let post_timestamp = post_timestamps[row_idx];

			let single_row = post.extract_row(row_idx);
			let projected = self.project_columns(&single_row);
			let post_row = projected.to_single_row();

			let diffs =
				self.replace_event_in_windows(txn, group_hash, row_number, &post_row, post_timestamp)?;
			result.extend(diffs);
		}

		Ok(result)
	}

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
			if let Some(diff) = self.remove_event_from_one_window(txn, &window_key, row_number)? {
				result.push(diff);
			}
		}

		if !self.is_rolling() {
			let index_key = self.create_row_index_key(group_hash, row_number);
			let empty = self.layout.allocate();
			self.save_state(txn, &index_key, empty)?;
		}

		Ok(result)
	}

	#[inline]
	fn remove_event_from_one_window(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		row_number: RowNumber,
	) -> Result<Option<Diff>> {
		let mut window_state = self.load_window_state(txn, window_key)?;
		let Some(idx) = window_state.events.iter().position(|e| e.row_number == row_number) else {
			return Ok(None);
		};
		let Some(layout) = window_state.window_layout.clone() else {
			return Ok(None);
		};

		let changed_at = DateTime::from_nanos(txn.clock().now_nanos());
		let pre_aggregation = self.apply_aggregations(
			txn,
			window_key,
			&layout,
			&window_state.events,
			changed_at,
			&window_state,
		)?;

		let evicted_row = window_state.events[idx].to_row(&layout);
		window_state.events.remove(idx);
		window_state.event_count = window_state.event_count.saturating_sub(1);
		self.update_running_totals_on_evict(&mut window_state, &evicted_row);

		if window_state.events.is_empty() {
			self.save_window_state(txn, window_key, &window_state)?;
			return Ok(pre_aggregation.map(|(pre_row, _)| Diff::remove(Columns::from_row(&pre_row))));
		}

		let post_aggregation = self.apply_aggregations(
			txn,
			window_key,
			&layout,
			&window_state.events,
			changed_at,
			&window_state,
		)?;
		self.save_window_state(txn, window_key, &window_state)?;

		Ok(match (pre_aggregation, post_aggregation) {
			(Some((pre_row, _)), Some((post_row, _))) => {
				Some(Diff::update(Columns::from_row(&pre_row), Columns::from_row(&post_row)))
			}
			_ => None,
		})
	}

	fn process_event_removals(&self, txn: &mut FlowTransaction, pre: &Columns) -> Result<Vec<Diff>> {
		let row_count = pre.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let group_hashes = self.compute_group_keys(pre)?;
		let mut result = Vec::new();

		for (&row_number, &group_hash) in pre.row_numbers.iter().zip(group_hashes.iter()) {
			let diffs = self.remove_event_from_windows(txn, group_hash, row_number)?;
			result.extend(diffs);
		}

		Ok(result)
	}

	pub fn extract_group_values(&self, columns: &Columns) -> Result<(Vec<Value>, Vec<String>)> {
		if columns.row_count() == 0 || self.group_by.is_empty() {
			return Ok((Vec::new(), Vec::new()));
		}

		let session = self.eval_session(false);
		let exec_ctx = session.with_eval(columns.clone(), columns.row_count());

		let mut values = Vec::new();
		let mut names = Vec::new();
		for (i, compiled_expr) in self.compiled_group_by.iter().enumerate() {
			let col = compiled_expr.execute(&exec_ctx)?;
			values.push(col.data().get_value(0).clone());
			names.push(display_label(&self.group_by[i]).text().to_string());
		}

		Ok((values, names))
	}

	pub fn events_to_columns(&self, window_layout: &WindowLayout, events: &[WindowEvent]) -> Result<Columns> {
		if events.is_empty() {
			return Ok(Columns::new(Vec::new()));
		}

		let mut builders: Vec<ColumnBuffer> = window_layout
			.types
			.iter()
			.map(|ty| ColumnBuffer::with_capacity(ty.clone(), events.len()))
			.collect();

		for event in events.iter() {
			let row = event.to_row(window_layout);
			for (idx, builder) in builders.iter_mut().enumerate() {
				let value = row.shape.get_value(&row.encoded, idx);
				builder.push_value(value);
			}
		}

		let columns = window_layout
			.names
			.iter()
			.zip(builders)
			.map(|(name, data)| ColumnWithName {
				name: Fragment::internal(name.clone()),
				data,
			})
			.collect();

		Ok(Columns::new(columns))
	}

	pub fn apply_aggregations(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		window_layout: &WindowLayout,
		events: &[WindowEvent],
		changed_at: DateTime,
		state: &WindowState,
	) -> Result<Option<(Row, bool)>> {
		if events.is_empty() {
			return Ok(None);
		}

		if self.aggregations.is_empty() {
			let (result_row_number, is_new) =
				self.row_number_provider.get_or_create_row_number(txn, window_key)?;
			let mut result_row = events[0].to_row(window_layout);
			result_row.number = result_row_number;
			return Ok(Some((result_row, is_new)));
		}

		if let Some(fast) = &self.fast_aggregations
			&& state.running_totals.len() == fast.len()
			&& let Some(row) =
				self.build_fast_path_result(txn, window_key, events, changed_at, state, fast)?
		{
			return Ok(Some(row));
		}

		let (result_values, result_names, result_types) =
			self.compute_aggregation_outputs(window_layout, events)?;

		let layout = build_aggregation_shape(&result_names, &result_types);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &result_values);

		let ts_nanos = changed_at.to_nanos();
		encoded.set_timestamps(ts_nanos, ts_nanos);

		let (result_row_number, is_new) = self.row_number_provider.get_or_create_row_number(txn, window_key)?;
		Ok(Some((
			Row {
				number: result_row_number,
				encoded,
				shape: layout,
			},
			is_new,
		)))
	}

	fn build_fast_path_result(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		events: &[WindowEvent],
		changed_at: DateTime,
		state: &WindowState,
		fast: &[FastAgg],
	) -> Result<Option<(Row, bool)>> {
		let window_layout = state.layout();
		let (group_values, group_names) = if self.group_by.is_empty() {
			(Vec::new(), Vec::new())
		} else {
			let first_columns = self.events_to_columns(window_layout, &events[..1])?;
			self.extract_group_values(&first_columns)?
		};

		let mut values = Vec::with_capacity(group_values.len() + fast.len());
		let mut names = Vec::with_capacity(group_values.len() + fast.len());
		let mut types = Vec::with_capacity(group_values.len() + fast.len());

		for (value, name) in group_values.into_iter().zip(group_names.into_iter()) {
			types.push(value.get_type());
			values.push(value);
			names.push(name);
		}

		for (slot, _kind) in fast.iter().enumerate() {
			let value = state.running_totals[slot].clone();
			types.push(value.get_type());
			values.push(value);
			names.push(self.agg_output_names[slot].clone());
		}

		let layout = build_aggregation_shape(&names, &types);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &values);

		let ts_nanos = changed_at.to_nanos();
		encoded.set_timestamps(ts_nanos, ts_nanos);

		let (result_row_number, is_new) = self.row_number_provider.get_or_create_row_number(txn, window_key)?;

		#[cfg(debug_assertions)]
		{
			let (slow_values, slow_names, slow_types) =
				self.compute_aggregation_outputs(window_layout, events)?;
			debug_assert_eq!(names, slow_names, "fast-path output names diverge from slow path");
			debug_assert_eq!(types, slow_types, "fast-path output types diverge from slow path");
			debug_assert_eq!(values, slow_values, "fast-path output values diverge from slow path");
		}

		Ok(Some((
			Row {
				number: result_row_number,
				encoded,
				shape: layout,
			},
			is_new,
		)))
	}

	fn build_accumulator(&self, kind: &FastAgg) -> Option<Box<dyn Accumulator>> {
		let func = self.routines.get_aggregate_function(&kind.function_name)?;
		let mut ctx = FunctionContext {
			fragment: Fragment::internal(&kind.function_name),
			identity: IdentityId::root(),
			row_count: 1,
			runtime_context: &self.runtime_context,
		};
		func.accumulator(&mut ctx)
	}

	pub fn ensure_running_totals(&self, state: &mut WindowState, layout: &WindowLayout) {
		let Some(fast) = &self.fast_aggregations else {
			return;
		};
		if state.running_totals.len() == fast.len() {
			return;
		}
		let mut totals: Vec<Value> = Vec::with_capacity(fast.len());
		for kind in fast {
			let Some(mut acc) = self.build_accumulator(kind) else {
				state.running_totals = Vec::new();
				return;
			};
			if !state.events.is_empty() {
				let Some(cols) = events_to_agg_columns(layout, &state.events, kind) else {
					state.running_totals = Vec::new();
					return;
				};
				let view = events_group_view(state.events.len());
				if acc.update(&cols, &view).is_err() {
					state.running_totals = Vec::new();
					return;
				}
			}
			totals.push(acc.peek(&Vec::new()).unwrap_or_else(Value::none));
		}
		state.running_totals = totals;
	}

	pub fn update_running_totals_on_push(&self, state: &mut WindowState, row: &Row) {
		let Some(fast) = &self.fast_aggregations else {
			return;
		};
		let layout = match &state.window_layout {
			Some(l) => l.clone(),
			None => return,
		};
		let bootstrap_was_needed = state.running_totals.len() != fast.len();
		self.ensure_running_totals(state, &layout);
		if state.running_totals.len() != fast.len() {
			return;
		}
		if bootstrap_was_needed {
			return;
		}
		for (slot, kind) in fast.iter().enumerate() {
			let Some(mut acc) = self.build_accumulator(kind) else {
				state.running_totals = Vec::new();
				return;
			};
			if acc.seed(Vec::new(), state.running_totals[slot].clone()).is_err() {
				state.running_totals = Vec::new();
				return;
			}
			let Some(cols) = make_single_row_columns_for_agg(&layout, row, kind) else {
				state.running_totals = Vec::new();
				return;
			};
			if acc.update(&cols, &single_group_view()).is_err() {
				state.running_totals = Vec::new();
				return;
			}
			state.running_totals[slot] = acc.peek(&Vec::new()).unwrap_or_else(Value::none);
		}
	}

	pub fn update_running_totals_on_evict(&self, state: &mut WindowState, row: &Row) {
		let Some(fast) = &self.fast_aggregations else {
			return;
		};
		let layout = match &state.window_layout {
			Some(l) => l.clone(),
			None => return,
		};
		if state.running_totals.len() != fast.len() {
			return;
		}
		for (slot, kind) in fast.iter().enumerate() {
			let Some(mut acc) = self.build_accumulator(kind) else {
				state.running_totals = Vec::new();
				return;
			};
			if acc.seed(Vec::new(), state.running_totals[slot].clone()).is_err() {
				state.running_totals = Vec::new();
				return;
			}
			let Some(cols) = make_single_row_columns_for_agg(&layout, row, kind) else {
				state.running_totals = Vec::new();
				return;
			};
			if acc.retract(&cols, &single_group_view()).is_err() {
				state.running_totals = Vec::new();
				return;
			}
			state.running_totals[slot] = acc.peek(&Vec::new()).unwrap_or_else(Value::none);
		}
	}

	#[inline]
	fn compute_aggregation_outputs(
		&self,
		window_layout: &WindowLayout,
		events: &[WindowEvent],
	) -> Result<(Vec<Value>, Vec<String>, Vec<Type>)> {
		let columns = self.events_to_columns(window_layout, events)?;
		let (group_values, group_names) = self.extract_group_values(&columns)?;
		let agg_session = self.eval_session(true);
		let exec_ctx = agg_session.with_eval(columns, events.len());

		let mut values = Vec::new();
		let mut names = Vec::new();
		let mut types = Vec::new();

		for (value, name) in group_values.into_iter().zip(group_names.into_iter()) {
			types.push(value.get_type());
			values.push(value);
			names.push(name);
		}

		for (i, compiled_aggregation) in self.compiled_aggregations.iter().enumerate() {
			let agg_column = compiled_aggregation.execute(&exec_ctx)?;
			let value = agg_column.data().get_value(0);
			types.push(value.get_type());
			values.push(value);
			names.push(display_label(&self.aggregations[i]).text().to_string());
		}

		Ok((values, names, types))
	}

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
					let low_key = self.create_window_key(*group_hash, cutoff_id);
					let high_key = self.create_window_key(*group_hash, 0);
					let range = EncodedKeyRange::new(
						ops::Bound::Excluded(low_key),
						ops::Bound::Included(high_key),
					);

					let expired_keys = self.scan_keys_in_range(txn, &range)?;
					let changed_at = DateTime::from_nanos(current_timestamp);
					for key in &expired_keys {
						let window_state = self.load_window_state(txn, key)?;
						if !window_state.events.is_empty()
							&& let Some(layout) = &window_state.window_layout && let Some((
							row,
							_,
						)) = self
							.apply_aggregations(
								txn,
								key,
								layout,
								&window_state.events,
								changed_at,
								&window_state,
							)? {
							result.push(Diff::remove(Columns::from_row(&row)));
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

	pub fn load_window_state(&self, txn: &mut FlowTransaction, window_key: &EncodedKey) -> Result<WindowState> {
		let state_row = self.load_state(txn, window_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(WindowState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(WindowState::default());
		}

		from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize WindowState: {}", e))))
	}

	pub fn save_window_state(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		state: &WindowState,
	) -> Result<()> {
		let serialized = to_stdvec(state)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize WindowState: {}", e))))?;

		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, window_key, state_row)
	}

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

		let serialized = to_stdvec(&new_count)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize count: {}", e))))?;

		let mut count_state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut count_state_row, 0, &blob);

		self.save_state(txn, &count_key, count_state_row)?;

		Ok(current_count)
	}

	pub fn create_count_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"cnt:");
		serializer.extend_u128(group_hash);
		serializer.finish()
	}

	fn create_group_registry_key(&self) -> EncodedKey {
		EncodedKey::new(b"grp:")
	}

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

	fn save_group_registry(&self, txn: &mut FlowTransaction, groups: &[Hash128]) -> Result<()> {
		let key = self.create_group_registry_key();
		let raw: Vec<u128> = groups.iter().map(|h| (*h).into()).collect();
		let serialized = to_stdvec(&raw)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize group registry: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &key, state_row)
	}

	pub fn register_group(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<()> {
		let mut groups = self.load_group_registry(txn)?;
		if !groups.contains(&group_hash) {
			groups.push(group_hash);
			self.save_group_registry(txn, &groups)?;
		}
		Ok(())
	}

	pub fn tick_expire_windows(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let window_size_ms = match self.size_duration() {
			Some(d) => d.as_millis() as u64,
			None => return Ok(result),
		};
		if window_size_ms == 0 {
			return Ok(result);
		}

		let mut keys_to_remove = Vec::new();
		for window_key in self.scan_window_keys(txn)? {
			if let Some(diff) =
				self.expire_window_if_due(txn, &window_key, current_timestamp, window_size_ms)?
			{
				result.push(diff);
				keys_to_remove.push(window_key);
			}
		}

		for key in &keys_to_remove {
			let empty = self.create_state();
			self.save_state(txn, key, empty)?;
		}

		Ok(result)
	}

	#[inline]
	fn scan_window_keys(&self, txn: &mut FlowTransaction) -> Result<Vec<EncodedKey>> {
		let all_state = txn.state_scan_all(self.node)?;
		let prefix = FlowNodeStateKey::new(self.node, vec![]).encode();
		let win_marker = b"win:";

		let mut keys = Vec::new();
		for item in &all_state.items {
			let full_key = &item.key;
			if full_key.len() <= prefix.len() {
				continue;
			}
			let inner = &full_key[prefix.len()..];
			if !inner.starts_with(win_marker) {
				continue;
			}
			keys.push(EncodedKey::new(inner));
		}
		Ok(keys)
	}

	#[inline]
	fn expire_window_if_due(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		current_timestamp: u64,
		window_size_ms: u64,
	) -> Result<Option<Diff>> {
		let window_state = self.load_window_state(txn, window_key)?;
		if window_state.events.is_empty() {
			return Ok(None);
		}

		let newest_event_time = window_state.events.iter().map(|e| e.timestamp).max().unwrap_or(0);
		if current_timestamp.saturating_sub(newest_event_time) <= window_size_ms {
			return Ok(None);
		}

		let changed_at = DateTime::from_nanos(current_timestamp);
		if let Some(layout) = &window_state.window_layout
			&& let Some((row, _)) = self.apply_aggregations(
				txn,
				window_key,
				layout,
				&window_state.events,
				changed_at,
				&window_state,
			)? {
			return Ok(Some(Diff::remove(Columns::from_row(&row))));
		}
		Ok(None)
	}

	pub fn process_insert(
		&self,
		txn: &mut FlowTransaction,
		columns: &Columns,
		changed_at: DateTime,
		group_fn: impl Fn(&WindowOperator, &mut FlowTransaction, &Columns, Hash128, DateTime) -> Result<Vec<Diff>>,
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
			let group_result = group_fn(self, txn, &group_columns, group_hash, changed_at)?;
			result.extend(group_result);
		}
		Ok(result)
	}

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
					..
				} => result.extend(process_fn(self, txn, post)?),
				Diff::Update {
					pre,
					post,
					..
				} => result.extend(self.apply_window_update_diff(txn, pre, post, &process_fn)?),
				Diff::Remove {
					pre,
					..
				} => result.extend(self.process_event_removals(txn, pre)?),
			}
		}
		Ok(result)
	}

	#[inline]
	fn apply_window_update_diff(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		process_fn: &impl Fn(&WindowOperator, &mut FlowTransaction, &Columns) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let group_hashes = self.compute_group_keys(pre)?;
		let mut update_indices: Vec<usize> = Vec::new();
		let mut insert_indices: Vec<usize> = Vec::new();
		for (row_idx, &group_hash) in group_hashes.iter().enumerate() {
			let row_number = pre.row_numbers[row_idx];
			if self.lookup_row_index(txn, group_hash, row_number)?.is_empty() {
				insert_indices.push(row_idx);
			} else {
				update_indices.push(row_idx);
			}
		}

		let mut result = Vec::new();
		if !update_indices.is_empty() {
			let pre_subset = pre.extract_by_indices(&update_indices);
			let post_subset = post.extract_by_indices(&update_indices);
			result.extend(self.process_event_updates(txn, &pre_subset, &post_subset)?);
		}
		if !insert_indices.is_empty() {
			let post_subset = post.extract_by_indices(&insert_indices);
			result.extend(process_fn(self, txn, &post_subset)?);
		}
		Ok(result)
	}

	pub fn emit_aggregation_diff(
		aggregated_row: &Row,
		is_new: bool,
		previous_aggregation: Option<(Row, bool)>,
	) -> Diff {
		if is_new {
			Diff::insert(Columns::from_row(aggregated_row))
		} else if let Some((previous_row, _)) = previous_aggregation {
			Diff::update(Columns::from_row(&previous_row), Columns::from_row(aggregated_row))
		} else {
			Diff::insert(Columns::from_row(aggregated_row))
		}
	}
}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> RowShape {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			}
			| WindowKind::Session {
				..
			}
			| WindowKind::Rolling {
				size: WindowSize::Duration(_),
			} => Some(Duration::from_secs(1)),
			WindowKind::Rolling {
				size: WindowSize::Count(_),
			} => None,
		}
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

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let current_timestamp = tick.now.to_nanos() / 1_000_000;
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
			Ok(Some(Change::from_flow(
				self.node,
				CommitVersion(0),
				diffs,
				DateTime::from_nanos(self.runtime_context.clock.now_nanos()),
			)))
		}
	}
}
