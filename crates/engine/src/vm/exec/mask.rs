// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	util::bitvec::BitVec,
	value::Value,
};

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
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
	pub parent_mask: BitVec,

	pub then_mask: BitVec,

	pub else_mask: BitVec,

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
	pub parent_mask: BitVec,

	pub active_mask: BitVec,

	pub broken_mask: BitVec,

	pub loop_end_addr: usize,
}

pub(crate) fn merge_by_mask(existing: &Columns, new_value: &Columns, mask: &BitVec) -> Result<Columns> {
	let len = existing.row_count();
	debug_assert_eq!(new_value.row_count(), len);
	debug_assert_eq!(mask.len(), len);

	let merged_columns: Vec<ColumnWithName> = existing
		.columns
		.iter()
		.zip(new_value.columns.iter())
		.enumerate()
		.map(|(idx, (old_col, new_col))| {
			let result_type = old_col.get_type();
			let mut data = ColumnBuffer::with_capacity(result_type, len);
			for i in 0..len {
				if mask.get(i) {
					data.push_value(new_col.get_value(i));
				} else {
					data.push_value(old_col.get_value(i));
				}
			}
			ColumnWithName::new(existing.name_at(idx).clone(), data)
		})
		.collect();

	Ok(Columns::new(merged_columns))
}

pub(crate) fn scatter_merge_variables(
	then_var: &Variable,
	else_var: &Variable,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> Variable {
	let then_cols = variable_to_columns(then_var);
	let else_cols = variable_to_columns(else_var);

	let merged: Vec<ColumnWithName> = then_cols
		.columns
		.iter()
		.zip(else_cols.columns.iter())
		.enumerate()
		.map(|(idx, (tc, ec))| {
			let merged_data = tc.scatter_merge(ec, then_mask, else_mask, total_len);
			ColumnWithName::new(then_cols.name_at(idx).clone(), merged_data)
		})
		.collect();

	Variable::columns(Columns::new(merged))
}

fn variable_to_columns(var: &Variable) -> Columns {
	match var {
		Variable::Columns {
			columns: c,
			..
		}
		| Variable::ForIterator {
			columns: c,
			..
		} => c.clone(),
		Variable::Closure(_) => Columns::single_row([("value", Value::none())]),
	}
}

pub(crate) fn extract_bool_bitvec(var: &Variable) -> Result<BitVec> {
	let cols = match var {
		Variable::Columns {
			columns: c,
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
	if cols.is_empty() {
		return Ok(BitVec::repeat(0, false));
	}
	let col = &cols.columns[0];
	let (inner_data, opt_bv) = col.unwrap_option();
	match inner_data {
		ColumnBuffer::Bool(container) => {
			let bv = container.data().clone();
			match opt_bv {
				Some(defined_bv) => Ok(bv.and(defined_bv)),
				None => Ok(bv),
			}
		}
		_ => {
			let len = col.len();
			Ok(BitVec::from_fn(len, |i| value_is_truthy(&col.get_value(i))))
		}
	}
}

impl<'a> Vm<'a> {
	pub(crate) fn effective_mask(&self) -> BitVec {
		self.active_mask.clone().unwrap_or_else(|| BitVec::repeat(self.batch_size, true))
	}

	pub(crate) fn is_masked(&self) -> bool {
		self.active_mask.is_some()
	}

	pub(crate) fn intersect_condition(&self, bool_bv: &BitVec) -> BitVec {
		let parent = self.effective_mask();
		if bool_bv.len() == parent.len() {
			parent.and(bool_bv)
		} else if bool_bv.len() == 1 {
			if bool_bv.get(0) {
				parent
			} else {
				BitVec::repeat(parent.len(), false)
			}
		} else {
			parent.and(bool_bv)
		}
	}

	pub(crate) fn exec_jump_if_false_pop_columnar(&mut self, target_addr: usize) -> Result<bool> {
		let var = self.stack.pop()?;
		let bool_bv = extract_bool_bitvec(&var)?;

		if let Some(loop_state) = self.loop_mask_stack.last_mut()
			&& loop_state.loop_end_addr == target_addr
		{
			let candidate = loop_state.active_mask.and(&bool_bv);

			if candidate.none() {
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

		if candidate.none() {
			self.ip = target_addr;
			return Ok(true);
		}

		let else_mask = parent.and(&candidate.not());

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

		if jumping.none() {
			return Ok(false);
		}

		if jumping == parent {
			self.ip = target_addr;
			return Ok(true);
		}

		let continuing = parent.and(&jumping.not());

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

	pub(crate) fn enter_loop_mask(&mut self, loop_end_addr: usize, active_rows: BitVec) {
		let parent = self.effective_mask();
		self.loop_mask_stack.push(LoopMaskState {
			parent_mask: parent,
			active_mask: active_rows.clone(),
			broken_mask: BitVec::repeat(self.batch_size, false),
			loop_end_addr,
		});
		self.active_mask = Some(active_rows);
	}

	pub(crate) fn exec_break_masked(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		let breaking_rows = self.effective_mask();
		if let Some(loop_state) = self.loop_mask_stack.last_mut() {
			loop_state.broken_mask = loop_state.broken_mask.or(&breaking_rows);

			let remaining = loop_state.active_mask.and(&breaking_rows.not());
			loop_state.active_mask = remaining.clone();

			if remaining.none() {
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
			let remaining = loop_state.active_mask.and(&continuing_rows.not());

			if remaining.none() {
				for _ in 0..exit_scopes {
					self.symbols.exit_scope()?;
				}

				loop_state.active_mask = loop_state.parent_mask.and(&loop_state.broken_mask.not());
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
			let merged = scatter_merge_variables(
				then_var,
				else_var,
				&frame.then_mask,
				&frame.else_mask,
				total_len,
			);
			self.stack.push(merged);
		}

		for (name, then_snapshot) in &frame.then_var_snapshots {
			if let Some(current) = self.symbols.get(name) {
				let then_cols = variable_to_columns(then_snapshot);
				let else_cols = variable_to_columns(current);
				let merged_cols: Vec<ColumnWithName> = then_cols
					.columns
					.iter()
					.zip(else_cols.columns.iter())
					.enumerate()
					.map(|(idx, (tc, ec))| {
						let merged_data = tc.scatter_merge(
							ec,
							&frame.then_mask,
							&frame.else_mask,
							total_len,
						);
						ColumnWithName::new(then_cols.name_at(idx).clone(), merged_data)
					})
					.collect();
				self.symbols.reassign(name.clone(), Variable::columns(Columns::new(merged_cols)))?;
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
				let existing_cols = variable_to_columns(existing);
				let new_cols = variable_to_columns(&new_value);
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
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
	use reifydb_type::{
		fragment::Fragment,
		util::bitvec::BitVec,
		value::{Value, r#type::Type},
	};

	use super::*;

	fn int4_column(name: &str, values: &[i32]) -> ColumnWithName {
		let mut data = ColumnBuffer::with_capacity(Type::Int4, values.len());
		for &v in values {
			data.push(v);
		}
		ColumnWithName::new(Fragment::internal(name), data)
	}

	#[test]
	fn scatter_merge_all_then() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BitVec::from_slice(&[true, true, true]);
		let else_mask = BitVec::from_slice(&[false, false, false]);

		let merged = then_col.data().scatter_merge(else_col.data(), &then_mask, &else_mask, 3);
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(20));
		assert_eq!(merged.get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_all_else() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BitVec::from_slice(&[false, false, false]);
		let else_mask = BitVec::from_slice(&[true, true, true]);

		let merged = then_col.data().scatter_merge(else_col.data(), &then_mask, &else_mask, 3);
		assert_eq!(merged.get_value(0), Value::Int4(40));
		assert_eq!(merged.get_value(1), Value::Int4(50));
		assert_eq!(merged.get_value(2), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_alternating() {
		let then_col = int4_column("x", &[10, 20, 30, 40]);
		let else_col = int4_column("x", &[90, 80, 70, 60]);
		let then_mask = BitVec::from_slice(&[true, false, true, false]);
		let else_mask = BitVec::from_slice(&[false, true, false, true]);

		let merged = then_col.data().scatter_merge(else_col.data(), &then_mask, &else_mask, 4);
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(30));
		assert_eq!(merged.get_value(3), Value::Int4(60));
	}

	#[test]
	fn merge_by_mask_selective_update() {
		let existing = Columns::new(vec![int4_column("x", &[1, 2, 3])]);
		let new_value = Columns::new(vec![int4_column("x", &[10, 20, 30])]);
		let mask = BitVec::from_slice(&[true, false, true]);

		let merged = merge_by_mask(&existing, &new_value, &mask).unwrap();
		let col = &merged.columns[0];
		assert_eq!(col.get_value(0), Value::Int4(10));
		assert_eq!(col.get_value(1), Value::Int4(2));
		assert_eq!(col.get_value(2), Value::Int4(30));
	}
}
