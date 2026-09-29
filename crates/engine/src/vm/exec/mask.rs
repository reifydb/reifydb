// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{batch, single_row},
	column::{
		cast::cast_column_data,
		factory::rename,
		scatter::{merge_rows, scatter_merge},
	},
};
use reifydb_evaluate::{expression::branch::BranchLayout, stack::Variable};
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	reifydb_assertions,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::TypeConstraint,
		system_columns::user_columns,
		value_type::ValueType,
	},
};

use crate::{
	Result,
	vm::{exec::call::cast_to_declared_return_type, stack::ControlFlow, vm::Vm},
};

pub(crate) fn value_is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::None {
			..
		} => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

#[derive(Debug)]
pub(crate) struct MaskFrame {
	pub parent_mask: BooleanBuffer,

	pub then_mask: BooleanBuffer,

	pub else_mask: BooleanBuffer,

	pub else_addr: usize,

	pub end_addr: usize,

	pub phase: MaskPhase,

	pub stack_depth: usize,

	pub then_stack_delta: Vec<Variable>,

	pub then_var_snapshots: HashMap<String, Variable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaskPhase {
	Then,

	Else,
}

#[derive(Debug)]
pub(crate) struct LoopMaskState {
	pub parent_mask: BooleanBuffer,

	pub active_mask: BooleanBuffer,

	pub broken_mask: BooleanBuffer,

	pub loop_end_addr: usize,
}

pub(crate) fn merge_by_mask(
	existing: &RecordBatch,
	new_value: &RecordBatch,
	mask: &BooleanBuffer,
) -> Result<RecordBatch> {
	let len = existing.num_rows();
	reifydb_assertions! {
		assert_eq!(new_value.num_rows(), len);
		assert_eq!(mask.len(), len);
	}

	let merged_columns: Vec<(FieldRef, ArrayRef)> = user_columns(existing)
		.zip(user_columns(new_value))
		.map(|((old_field, old_array), (new_field, new_array))| {
			merge_rows(
				&ColumnView::try_from((old_array, old_field.as_ref()))?,
				&ColumnView::try_from((new_array, new_field.as_ref()))?,
				mask,
				len,
				old_field.name(),
			)
		})
		.collect::<Result<_>>()?;

	batch(merged_columns)
}

fn scatter_merge_batches(
	then_cols: &RecordBatch,
	else_cols: &RecordBatch,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> Result<RecordBatch> {
	let merged: Vec<(FieldRef, ArrayRef)> = user_columns(then_cols)
		.zip(user_columns(else_cols))
		.map(|((then_field, then_array), (else_field, else_array))| {
			scatter_merge(
				&ColumnView::try_from((then_array, then_field.as_ref()))?,
				&ColumnView::try_from((else_array, else_field.as_ref()))?,
				then_mask,
				else_mask,
				total_len,
				then_field.name(),
			)
		})
		.collect::<Result<_>>()?;

	batch(merged)
}

pub(crate) fn scatter_merge_variables(
	then_var: &Variable,
	else_var: &Variable,
	then_mask: &BooleanBuffer,
	else_mask: &BooleanBuffer,
	total_len: usize,
) -> Result<Variable> {
	let then_cols = variable_to_columns(then_var)?;
	let else_cols = variable_to_columns(else_var)?;

	Ok(Variable::columns(scatter_merge_batches(&then_cols, &else_cols, then_mask, else_mask, total_len)?))
}

fn named_types<'c>(columns: &'c RecordBatch, name: &'c str) -> Result<Vec<(&'c str, ValueType, bool)>> {
	user_columns(columns)
		.map(|(field, array)| {
			let ty = ColumnView::try_from((array, field.as_ref()))?.get_type().inner_type().clone();
			let fits_any = ty == ValueType::Any;
			Ok((name, ty, fits_any))
		})
		.collect()
}

fn variable_columns(var: &Variable) -> Option<&RecordBatch> {
	match var {
		Variable::Columns {
			batch: c,
			..
		}
		| Variable::ForIterator {
			batch: c,
			..
		} => Some(c),
		Variable::Closure(_) => None,
	}
}

fn variable_to_columns(var: &Variable) -> Result<RecordBatch> {
	match var {
		Variable::Columns {
			batch: c,
			..
		}
		| Variable::ForIterator {
			batch: c,
			..
		} => Ok(c.clone()),
		Variable::Closure(_) => single_row([("value", Value::none())]),
	}
}

pub(crate) fn extract_bool_bitvec(var: &Variable) -> Result<BooleanBuffer> {
	let cols = match var {
		Variable::Columns {
			batch: c,
			..
		} => c,
		_ => {
			return Err(TypeError::Runtime {
				kind: RuntimeErrorKind::ExpectedSingleColumn {
					actual: 0,
				},
				message: "Expected a boolean value for conditional branch".to_string(),
			}
			.into());
		}
	};
	let Some((field, array)) = user_columns(cols).next() else {
		return Ok(BooleanBuffer::new_unset(0));
	};
	let col = ColumnView::try_from((array, field.as_ref()))?;
	match col.data {
		ViewData::Bool(container) => {
			let bv = container.values().clone();
			match col.logical_nulls() {
				Some(nulls) => Ok(&bv & nulls.inner()),
				None => Ok(bv),
			}
		}
		_ => {
			let len = col.len();
			Ok(BooleanBuffer::collect_bool(len, |i| value_is_truthy(&col.get_value(i))))
		}
	}
}

impl<'a> Vm<'a> {
	pub(crate) fn effective_mask(&self) -> BooleanBuffer {
		let active = self.active_mask.clone().unwrap_or_else(|| BooleanBuffer::new_set(self.batch_size));
		match &self.returned_mask {
			Some(returned) => &active & &!returned,
			None => active,
		}
	}

	pub(crate) fn has_masked_return(&self) -> bool {
		self.is_masked() || self.pending_return.is_some()
	}

	fn align_types(&self, left: &RecordBatch, right: &RecordBatch) -> Result<(RecordBatch, RecordBatch)> {
		let ctx = self.eval_ctx();
		let mut aligned_left = Vec::with_capacity(left.num_columns());
		let mut aligned_right = Vec::with_capacity(right.num_columns());

		for ((left_field, left_array), (right_field, right_array)) in
			user_columns(left).zip(user_columns(right))
		{
			let left_data = ColumnView::try_from((left_array, left_field.as_ref()))?;
			let right_data = ColumnView::try_from((right_array, right_field.as_ref()))?;
			let target = ValueType::super_type_of([left_data.get_type(), right_data.get_type()]);
			let name = Fragment::internal(left_field.name());

			let left_cast = if left_data.get_type() == target {
				(left_field.clone(), left_array.clone())
			} else {
				cast_column_data(&ctx, &left_data, target.clone(), name.clone())?
			};
			let right_cast = if right_data.get_type() == target {
				(right_field.clone(), right_array.clone())
			} else {
				cast_column_data(&ctx, &right_data, target.clone(), name.clone())?
			};

			aligned_left.push(left_cast);
			aligned_right.push(right_cast);
		}

		Ok((batch(aligned_left)?, batch(aligned_right)?))
	}

	fn cast_to_declared(
		&self,
		left: &RecordBatch,
		right: &RecordBatch,
		declared: &TypeConstraint,
	) -> Result<(RecordBatch, RecordBatch)> {
		let ctx = self.eval_ctx();
		let fragment = self.udf_call.fragment.clone();
		let target = declared.get_type();
		let cast = |columns: &RecordBatch| -> Result<RecordBatch> {
			let mut out = Vec::with_capacity(columns.num_columns());
			for (field, array) in user_columns(columns) {
				let data = ColumnView::try_from((array, field.as_ref()))?;
				let casted = if data.get_type().inner_type() == &target {
					(field.clone(), array.clone())
				} else {
					rename(
						cast_to_declared_return_type(
							&ctx,
							&data,
							declared,
							fragment.text(),
							&fragment,
						)?,
						field.name(),
					)
				};
				out.push(casted);
			}
			batch(out)
		};
		Ok((cast(left)?, cast(right)?))
	}

	fn fill_none_columns(
		&self,
		returning: &RecordBatch,
		pending: &RecordBatch,
	) -> Result<(RecordBatch, RecordBatch)> {
		let ctx = self.eval_ctx();
		let mut filled_returning = Vec::with_capacity(returning.num_columns());
		let mut filled_pending = Vec::with_capacity(pending.num_columns());

		for ((returning_field, returning_array), (pending_field, pending_array)) in
			user_columns(returning).zip(user_columns(pending))
		{
			let returning_data = ColumnView::try_from((returning_array, returning_field.as_ref()))?;
			let pending_data = ColumnView::try_from((pending_array, pending_field.as_ref()))?;
			let returning_type = returning_data.get_type().inner_type().clone();
			let pending_type = pending_data.get_type().inner_type().clone();
			let name = Fragment::internal(returning_field.name());
			let returning_pair = (returning_field.clone(), returning_array.clone());
			let pending_pair = (pending_field.clone(), pending_array.clone());

			let (returning_filled, pending_filled) = match (returning_type, pending_type) {
				(ValueType::Any, pending_type) if pending_type != ValueType::Any => (
					cast_column_data(&ctx, &returning_data, pending_type, name.clone())?,
					pending_pair,
				),
				(returning_type, ValueType::Any) if returning_type != ValueType::Any => (
					returning_pair,
					cast_column_data(&ctx, &pending_data, returning_type, name.clone())?,
				),
				_ => (returning_pair, pending_pair),
			};

			filled_returning.push(returning_filled);
			filled_pending.push(pending_filled);
		}

		Ok((batch(filled_returning)?, batch(filled_pending)?))
	}

	pub(crate) fn exec_return_value_masked(&mut self, columns: RecordBatch) -> Result<()> {
		let write_mask = self.effective_mask();

		let already_returned =
			self.returned_mask.clone().unwrap_or_else(|| BooleanBuffer::new_unset(self.batch_size));

		let merged = match self.pending_return.take() {
			Some(pending) => {
				let pending = variable_to_columns(&pending)?;
				let (returning, pending) = if let Some(declared) = self.udf_call.return_type.clone() {
					self.cast_to_declared(&columns, &pending, &declared)?
				} else {
					let name = self.udf_call.fragment.text();
					BranchLayout::new(named_types(&pending, name)?)
						.admit(named_types(&columns, name)?, &self.udf_call.fragment)?;
					self.fill_none_columns(&columns, &pending)?
				};
				scatter_merge_variables(
					&Variable::columns(returning),
					&Variable::columns(pending),
					&write_mask,
					&already_returned,
					self.batch_size,
				)?
			}
			None => {
				let returning = Variable::columns(columns);
				scatter_merge_variables(
					&returning,
					&returning,
					&write_mask,
					&already_returned,
					self.batch_size,
				)?
			}
		};

		for loop_state in self.loop_mask_stack.iter_mut() {
			loop_state.active_mask = &loop_state.active_mask & &!&write_mask;
		}

		let returned = &already_returned | &write_mask;
		let all_returned = !returned.has_false();

		self.returned_mask = Some(returned);
		self.pending_return = Some(merged);

		if all_returned {
			self.finalize_masked_return()?;
		}

		Ok(())
	}

	pub(crate) fn finalize_masked_return(&mut self) -> Result<()> {
		let Some(pending) = self.pending_return.take() else {
			return Ok(());
		};

		self.returned_mask = None;
		self.control_flow = ControlFlow::Return(Some(variable_to_columns(&pending)?));
		Ok(())
	}

	pub(crate) fn is_masked(&self) -> bool {
		self.active_mask.is_some()
	}

	pub(crate) fn intersect_condition(&self, bool_bv: &BooleanBuffer) -> BooleanBuffer {
		let parent = self.effective_mask();
		if bool_bv.len() == parent.len() {
			&parent & bool_bv
		} else if bool_bv.len() == 1 {
			if bool_bv.value(0) {
				parent
			} else {
				BooleanBuffer::new_unset(parent.len())
			}
		} else {
			&parent & bool_bv
		}
	}

	pub(crate) fn exec_jump_if_false_pop_columnar(&mut self, target_addr: usize) -> Result<bool> {
		let var = self.stack.pop()?;
		let bool_bv = extract_bool_bitvec(&var)?;

		if let Some(loop_state) = self.loop_mask_stack.last_mut()
			&& loop_state.loop_end_addr == target_addr
		{
			let candidate = &loop_state.active_mask & &bool_bv;

			if !candidate.has_true() {
				let state = self.loop_mask_stack.pop().unwrap();
				self.active_mask = if self.loop_mask_stack.is_empty() && self.mask_stack.is_empty() {
					None
				} else {
					Some(state.parent_mask)
				};
				self.ip = target_addr;
				return Ok(true);
			}

			loop_state.active_mask = candidate.clone();
			self.active_mask = Some(candidate);
			return Ok(false);
		}

		let parent = self.effective_mask();
		let candidate = self.intersect_condition(&bool_bv);

		if candidate == parent {
			return Ok(false);
		}

		if !candidate.has_true() {
			self.ip = target_addr;
			return Ok(true);
		}

		let else_mask = &parent & &!&candidate;

		self.mask_stack.push(MaskFrame {
			parent_mask: parent,
			then_mask: candidate.clone(),
			else_mask,
			else_addr: target_addr,
			end_addr: 0,
			phase: MaskPhase::Then,
			stack_depth: self.stack.len(),
			then_stack_delta: Vec::new(),
			then_var_snapshots: HashMap::new(),
		});

		self.active_mask = Some(candidate);
		Ok(false)
	}

	pub(crate) fn exec_jump_if_true_pop_columnar(&mut self, target_addr: usize) -> Result<bool> {
		let var = self.stack.pop()?;
		let bool_bv = extract_bool_bitvec(&var)?;

		let parent = self.effective_mask();

		let jumping = self.intersect_condition(&bool_bv);

		if !jumping.has_true() {
			return Ok(false);
		}

		if jumping == parent {
			self.ip = target_addr;
			return Ok(true);
		}

		let continuing = &parent & &!&jumping;

		self.mask_stack.push(MaskFrame {
			parent_mask: parent,
			then_mask: continuing.clone(),
			else_mask: jumping,
			else_addr: target_addr,
			end_addr: 0,
			phase: MaskPhase::Then,
			stack_depth: self.stack.len(),
			then_stack_delta: Vec::new(),
			then_var_snapshots: HashMap::new(),
		});

		self.active_mask = Some(continuing);
		Ok(false)
	}

	pub(crate) fn enter_loop_mask(&mut self, loop_end_addr: usize, active_rows: BooleanBuffer) {
		let parent = self.effective_mask();
		self.loop_mask_stack.push(LoopMaskState {
			parent_mask: parent,
			active_mask: active_rows.clone(),
			broken_mask: BooleanBuffer::new_unset(self.batch_size),
			loop_end_addr,
		});
		self.active_mask = Some(active_rows);
	}

	pub(crate) fn exec_break_masked(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		let breaking_rows = self.effective_mask();
		if let Some(loop_state) = self.loop_mask_stack.last_mut() {
			loop_state.broken_mask = &loop_state.broken_mask | &breaking_rows;

			let remaining = &loop_state.active_mask & &!&breaking_rows;
			loop_state.active_mask = remaining.clone();

			if !remaining.has_true() {
				for _ in 0..exit_scopes {
					self.symbols.exit_scope()?;
				}
				let state = self.loop_mask_stack.pop().unwrap();
				self.active_mask = if self.loop_mask_stack.is_empty() && self.mask_stack.is_empty() {
					None
				} else {
					Some(state.parent_mask)
				};
				self.ip = addr;
			} else {
				self.active_mask = Some(remaining);
			}
		} else {
			for _ in 0..exit_scopes {
				self.symbols.exit_scope()?;
			}
			self.ip = addr;
		}
		Ok(())
	}

	pub(crate) fn exec_continue_masked(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		let continuing_rows = self.effective_mask();
		if let Some(loop_state) = self.loop_mask_stack.last_mut() {
			let remaining = &loop_state.active_mask & &!&continuing_rows;

			if !remaining.has_true() {
				for _ in 0..exit_scopes {
					self.symbols.exit_scope()?;
				}

				loop_state.active_mask = &loop_state.parent_mask & &!&loop_state.broken_mask;
				self.active_mask = Some(loop_state.active_mask.clone());
				self.ip = addr;
			} else {
				loop_state.active_mask = remaining.clone();
				self.active_mask = Some(remaining);
			}
		} else {
			for _ in 0..exit_scopes {
				self.symbols.exit_scope()?;
			}
			self.ip = addr;
		}
		Ok(())
	}

	pub(crate) fn exec_jump_masked(&mut self, addr: usize) -> Result<bool> {
		if let Some(frame) = self.mask_stack.last_mut()
			&& frame.phase == MaskPhase::Then
		{
			let stack_delta: Vec<Variable> = {
				let mut delta = Vec::new();
				while self.stack.len() > frame.stack_depth {
					delta.push(self.stack.pop()?);
				}
				delta.reverse();
				delta
			};
			frame.then_stack_delta = stack_delta;

			frame.end_addr = addr;
			frame.phase = MaskPhase::Else;
			self.active_mask = Some(frame.else_mask.clone());
			self.ip = frame.else_addr;
			return Ok(true);
		}

		self.iteration_count += 1;
		if self.iteration_count > 10_000 {
			return Err(TypeError::Runtime {
				kind: RuntimeErrorKind::MaxIterationsExceeded {
					limit: 10_000,
				},
				message: format!("Loop exceeded maximum iteration limit of {}", 10_000),
			}
			.into());
		}
		self.ip = addr;
		Ok(true)
	}

	pub(crate) fn check_mask_merge_point(&mut self) -> Result<bool> {
		let should_merge =
			self.mask_stack.last().is_some_and(|f| f.phase == MaskPhase::Else && self.ip == f.end_addr);

		if !should_merge {
			return Ok(false);
		}

		let frame = self.mask_stack.pop().unwrap();

		let mut else_stack_delta = Vec::new();
		while self.stack.len() > frame.stack_depth {
			else_stack_delta.push(self.stack.pop()?);
		}
		else_stack_delta.reverse();

		let total_len = self.batch_size;
		for (then_var, else_var) in frame.then_stack_delta.iter().zip(else_stack_delta.iter()) {
			if let (Some(then_columns), Some(else_columns)) =
				(variable_columns(then_var), variable_columns(else_var))
			{
				let name = self.udf_call.fragment.text();
				BranchLayout::new(named_types(then_columns, name)?)
					.admit(named_types(else_columns, name)?, &self.udf_call.fragment)?;
			}
			let merged = scatter_merge_variables(
				then_var,
				else_var,
				&frame.then_mask,
				&frame.else_mask,
				total_len,
			)?;
			self.stack.push(merged);
		}

		for (name, then_snapshot) in &frame.then_var_snapshots {
			if let Some(current) = self.symbols.get(name) {
				let then_cols = variable_to_columns(then_snapshot)?;
				let else_cols = variable_to_columns(current)?;
				let merged_cols = scatter_merge_batches(
					&then_cols,
					&else_cols,
					&frame.then_mask,
					&frame.else_mask,
					total_len,
				)?;
				self.symbols.reassign(name.clone(), Variable::columns(merged_cols))?;
			}
		}

		if self.mask_stack.is_empty() {
			self.active_mask = None;
		} else {
			self.active_mask = Some(frame.parent_mask);
		}

		Ok(true)
	}

	pub(crate) fn exec_store_var_masked(&mut self, name: &str, new_value: Variable) -> Result<()> {
		let mask = self.effective_mask();

		match self.symbols.get(name) {
			Some(existing) => {
				let existing_cols = variable_to_columns(existing)?;
				let new_cols = variable_to_columns(&new_value)?;
				let (existing_cols, new_cols) = if self.udf_call.return_type.is_some() {
					let (existing_cols, new_cols) =
						self.fill_none_columns(&existing_cols, &new_cols)?;
					self.align_types(&existing_cols, &new_cols)?
				} else {
					let name = self.udf_call.fragment.text();
					BranchLayout::new(named_types(&existing_cols, name)?)
						.admit(named_types(&new_cols, name)?, &self.udf_call.fragment)?;
					let (new_cols, existing_cols) =
						self.fill_none_columns(&new_cols, &existing_cols)?;
					(existing_cols, new_cols)
				};
				let merged = merge_by_mask(&existing_cols, &new_cols, &mask)?;
				self.symbols.reassign(name.to_string(), Variable::columns(merged))?;
			}
			None => {
				self.symbols.reassign(name.to_string(), new_value)?;
			}
		}

		if let Some(frame) = self.mask_stack.last_mut()
			&& frame.phase == MaskPhase::Then
			&& let Some(current) = self.symbols.get(name)
		{
			frame.then_var_snapshots.insert(name.to_string(), current.clone());
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;

	fn int4_column(name: &str, values: &[i32]) -> (FieldRef, ArrayRef) {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Int4, values.len());
		for &v in values {
			builder.push(v);
		}
		builder.finish(name)
	}

	#[test]
	fn scatter_merge_all_then() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BooleanBuffer::from(vec![true, true, true]);
		let else_mask = BooleanBuffer::from(vec![false, false, false]);

		let merged = scatter_merge(
			&ColumnView::try_from(&then_col).unwrap(),
			&ColumnView::try_from(&else_col).unwrap(),
			&then_mask,
			&else_mask,
			3,
			"x",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(20));
		assert_eq!(merged.get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_all_else() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BooleanBuffer::from(vec![false, false, false]);
		let else_mask = BooleanBuffer::from(vec![true, true, true]);

		let merged = scatter_merge(
			&ColumnView::try_from(&then_col).unwrap(),
			&ColumnView::try_from(&else_col).unwrap(),
			&then_mask,
			&else_mask,
			3,
			"x",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert_eq!(merged.get_value(0), Value::Int4(40));
		assert_eq!(merged.get_value(1), Value::Int4(50));
		assert_eq!(merged.get_value(2), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_alternating() {
		let then_col = int4_column("x", &[10, 20, 30, 40]);
		let else_col = int4_column("x", &[90, 80, 70, 60]);
		let then_mask = BooleanBuffer::from(vec![true, false, true, false]);
		let else_mask = BooleanBuffer::from(vec![false, true, false, true]);

		let merged = scatter_merge(
			&ColumnView::try_from(&then_col).unwrap(),
			&ColumnView::try_from(&else_col).unwrap(),
			&then_mask,
			&else_mask,
			4,
			"x",
		)
		.unwrap();
		let merged = ColumnView::try_from(&merged).unwrap();
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(30));
		assert_eq!(merged.get_value(3), Value::Int4(60));
	}

	#[test]
	fn merge_by_mask_selective_update() {
		let existing = batch(vec![int4_column("x", &[1, 2, 3])]).unwrap();
		let new_value = batch(vec![int4_column("x", &[10, 20, 30])]).unwrap();
		let mask = BooleanBuffer::from(vec![true, false, true]);

		let merged = merge_by_mask(&existing, &new_value, &mask).unwrap();
		let col = ColumnView::try_from((merged.column(0), merged.schema_ref().field(0))).unwrap();
		assert_eq!(col.get_value(0), Value::Int4(10));
		assert_eq!(col.get_value(1), Value::Int4(2));
		assert_eq!(col.get_value(2), Value::Int4(30));
	}
}
