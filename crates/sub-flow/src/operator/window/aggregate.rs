// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{
		key::EncodedKey,
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	row::Row,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupByView},
};
use reifydb_routine::routine::{
	Accumulator, AggregateFunctionCapability, context::FunctionContext, registry::Routines,
};
use reifydb_rql::expression::{Expression, name::display_label};
#[cfg(reifydb_assertions)]
use reifydb_value::value::assert_equal_with_tolerance;
use reifydb_value::{
	Result,
	fragment::Fragment,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, identity::IdentityId, value_type::ValueType},
};

use super::{
	operator::WindowOperator,
	state::{WindowEvent, WindowLayout, WindowState},
};
use crate::transaction::FlowTransaction;

#[inline]
pub(super) fn build_aggregation_shape(names: &[String], types: &[ValueType]) -> RowShape {
	let fields: Vec<RowShapeField> = names
		.iter()
		.zip(types.iter())
		.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
		.collect();
	RowShape::new(fields)
}

#[derive(Clone, Debug)]
pub struct FastAgg {
	pub function_name: String,
	pub input_column: Option<String>,
}

pub(super) fn detect_fast_agg(routines: &Routines, expr: &Expression) -> Option<FastAgg> {
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
			let mut buf = ColumnBuffer::with_capacity(ValueType::Int4, 1);
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
			let mut buf = ColumnBuffer::with_capacity(ValueType::Int4, events.len());
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

impl WindowOperator {
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

		let lag_ms = self.rolling_lag_ms();
		let lagged_events: Vec<WindowEvent>;
		let events: &[WindowEvent] = if lag_ms > 0 {
			let max_ts = events.iter().map(|e| e.timestamp).max().unwrap_or(0);
			let boundary = max_ts.saturating_sub(lag_ms);
			lagged_events = events.iter().filter(|e| e.timestamp <= boundary).cloned().collect();
			if lagged_events.is_empty() {
				return Ok(None);
			}
			&lagged_events
		} else {
			events
		};

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

	pub(super) fn build_fast_path_result(
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

		reifydb_assertions! {
			let (slow_values, slow_names, slow_types) =
				self.compute_aggregation_outputs(window_layout, events)?;
			assert_eq!(names, slow_names, "fast-path output names diverge from slow path");
			assert_eq!(types, slow_types, "fast-path output types diverge from slow path");
			assert_equal_with_tolerance(&values, &slow_values);
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

	pub(super) fn build_accumulator(&self, kind: &FastAgg) -> Option<Box<dyn Accumulator>> {
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
	pub(super) fn compute_aggregation_outputs(
		&self,
		window_layout: &WindowLayout,
		events: &[WindowEvent],
	) -> Result<(Vec<Value>, Vec<String>, Vec<ValueType>)> {
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
}
