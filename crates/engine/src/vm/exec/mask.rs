// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Execution mask infrastructure for columnar VM control flow.
//!
//! When the VM operates in columnar mode (batch_size > 1), conditional branches
//! produce boolean Columns where different rows have different truth values.
//! Rather than jumping, the VM uses execution masks to run both branches and
//! merge results.

use std::collections::HashMap;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	util::bitvec::BitVec,
	value::Value,
};

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

/// Returns true when `value` is considered truthy for conditional branches.
/// None and zero numerics are falsy; non-empty strings and any other non-zero
/// value are truthy.
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

/// Tracks mask state for an IF/ELSE conditional in columnar mode.
///
/// Created when `JumpIfFalsePop` encounters a mixed boolean Column.
/// The VM executes the then-branch with `then_mask` active, then switches
/// to the else-branch with `else_mask` active, and finally merges results.
#[derive(Debug)]
pub(crate) struct MaskFrame {
	/// Mask active before this conditional (restored on merge).
	pub parent_mask: BitVec,
	/// Rows where condition was true - active during then-branch.
	pub then_mask: BitVec,
	/// Rows where condition was false - active during else-branch.
	pub else_mask: BitVec,
	/// IP of the else branch start (the JumpIfFalsePop target).
	pub else_addr: usize,
	/// IP past the entire if/else construct (captured from Jump(end) at then-boundary).
	pub end_addr: usize,
	/// Current execution phase.
	pub phase: MaskPhase,
	/// Stack depth at mask entry - for knowing how many values the branch produced.
	pub stack_depth: usize,
	/// Stack value(s) produced by then-branch, saved at phase transition.
	pub then_stack_delta: Vec<Variable>,
	/// Variables modified during then-branch (name -> snapshot at transition).
	pub then_var_snapshots: HashMap<String, Variable>,
}

/// Which phase of a masked conditional is currently executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaskPhase {
	/// Executing the then-branch with then_mask active.
	Then,
	/// Executing the else-branch with else_mask active.
	Else,
}

/// Tracks mask state for a WHILE loop in columnar mode.
///
/// Rows progressively exit the loop as their conditions become false.
#[derive(Debug)]
pub(crate) struct LoopMaskState {
	/// Mask inherited from enclosing context.
	pub parent_mask: BitVec,
	/// Rows still iterating (narrows each iteration).
	pub active_mask: BitVec,
	/// Rows that exited via BREAK (accumulated across iterations).
	pub broken_mask: BitVec,
	/// The loop_end address (JumpIfFalsePop target). Used to identify
	/// re-entry into this loop's condition check.
	pub loop_end_addr: usize,
}

/// Merge two columns by mask: row i gets `then_col[i]` if `then_mask[i]`,
/// `else_col[i]` if `else_mask[i]`, `None` otherwise.
pub(crate) fn scatter_merge_columns(
	then_col: &Column,
	else_col: &Column,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> Column {
	let merged = then_col.data().scatter_merge(else_col.data(), then_mask, else_mask, total_len);
	Column::new(then_col.name().clone(), merged)
}

/// Selective update: row i gets `new_value[i]` if `mask[i]`, keeps `existing[i]` otherwise.
pub(crate) fn merge_by_mask(existing: &Columns, new_value: &Columns, mask: &BitVec) -> Result<Columns> {
	let len = existing.row_count();
	debug_assert_eq!(new_value.row_count(), len);
	debug_assert_eq!(mask.len(), len);

	let merged_columns: Vec<Column> = existing
		.columns
		.iter()
		.zip(new_value.columns.iter())
		.map(|(old_col, new_col)| {
			let result_type = old_col.data().get_type();
			let mut data = ColumnData::with_capacity(result_type, len);
			for i in 0..len {
				if mask.get(i) {
					data.push_value(new_col.data().get_value(i));
				} else {
					data.push_value(old_col.data().get_value(i));
				}
			}
			Column::new(old_col.name().clone(), data)
		})
		.collect();

	Ok(Columns::new(merged_columns))
}

/// Merge two Variables by scattering their columns according to then/else masks.
pub(crate) fn scatter_merge_variables(
	then_var: &Variable,
	else_var: &Variable,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> Variable {
	let then_cols = variable_to_columns(then_var);
	let else_cols = variable_to_columns(else_var);

	let merged: Vec<Column> = then_cols
		.columns
		.iter()
		.zip(else_cols.columns.iter())
		.map(|(tc, ec)| scatter_merge_columns(tc, ec, then_mask, else_mask, total_len))
		.collect();

	Variable::columns(Columns::new(merged))
}

/// Convert any Variable to Columns for merging purposes.
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
		Variable::Closure(_) => Columns::scalar(Value::none()),
	}
}

/// Extract a boolean BitVec from a Variable.
///
/// For a boolean Column, returns a BitVec where each bit is the truth value of that row.
/// For Option<Bool>, None is treated as false.
/// For a scalar boolean, returns a single-element BitVec.
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
	let (inner_data, opt_bv) = col.data.unwrap_option();
	match inner_data {
		ColumnData::Bool(container) => {
			let bv = container.data().clone();
			match opt_bv {
				Some(defined_bv) => Ok(bv.and(defined_bv)),
				None => Ok(bv),
			}
		}
		_ => {
			// Non-boolean: evaluate truthiness per row
			let len = col.data.len();
			Ok(BitVec::from_fn(len, |i| value_is_truthy(&col.data.get_value(i))))
		}
	}
}

impl<'a> Vm<'a> {
	/// Returns the current effective mask as a BitVec.
	/// If no mask is active, returns an all-true BitVec of batch_size.
	pub(crate) fn effective_mask(&self) -> BitVec {
		self.active_mask.clone().unwrap_or_else(|| BitVec::repeat(self.batch_size, true))
	}

	/// Returns true if the VM is currently inside a masked execution context.
	pub(crate) fn is_masked(&self) -> bool {
		self.active_mask.is_some()
	}

	/// Intersect a boolean BitVec with the current effective mask,
	/// handling scalar broadcast.
	pub(crate) fn intersect_condition(&self, bool_bv: &BitVec) -> BitVec {
		let parent = self.effective_mask();
		if bool_bv.len() == parent.len() {
			parent.and(bool_bv)
		} else if bool_bv.len() == 1 {
			// Scalar condition in columnar context: broadcast
			if bool_bv.get(0) {
				parent
			} else {
				BitVec::repeat(parent.len(), false)
			}
		} else {
			parent.and(bool_bv)
		}
	}

	/// Columnar JumpIfFalsePop: handles boolean Columns with mixed true/false values.
	///
	/// Returns `Ok(true)` if the jump was taken (all-false fast path or loop exit),
	/// `Ok(false)` if execution should continue to the next instruction.
	///
	/// Handles two cases:
	/// - **WHILE loop re-entry**: if a LoopMaskState with matching loop_end_addr exists, narrows the loop mask
	///   instead of creating a MaskFrame.
	/// - **IF/ELSE or first WHILE entry**: creates a MaskFrame or LoopMaskState for mixed conditions.
	pub(crate) fn exec_jump_if_false_pop_columnar(&mut self, target_addr: usize) -> Result<bool> {
		let var = self.stack.pop()?;
		let bool_bv = extract_bool_bitvec(&var)?;

		// Check if this is a WHILE loop re-entry (LoopMaskState already active for this loop)
		if let Some(loop_state) = self.loop_mask_stack.last_mut()
			&& loop_state.loop_end_addr == target_addr
		{
			// Re-entering the loop condition: narrow the active mask
			let candidate = loop_state.active_mask.and(&bool_bv);

			if candidate.none() {
				// All remaining rows are done - exit the loop
				let state = self.loop_mask_stack.pop().unwrap();
				self.active_mask = if self.loop_mask_stack.is_empty() && self.mask_stack.is_empty() {
					None
				} else {
					Some(state.parent_mask)
				};
				self.ip = target_addr;
				return Ok(true);
			}

			// Some rows still iterating
			loop_state.active_mask = candidate.clone();
			self.active_mask = Some(candidate);
			return Ok(false); // continue into loop body
		}

		// Not a loop re-entry - standard IF/ELSE or first WHILE entry
		let parent = self.effective_mask();
		let candidate = self.intersect_condition(&bool_bv);

		// Fast path: all true (within the active mask)
		if candidate == parent {
			return Ok(false); // don't jump, no mask frame needed
		}

		// Fast path: all false
		if candidate.none() {
			self.ip = target_addr;
			return Ok(true); // jump taken
		}

		// Mixed: push mask frame for IF/ELSE, execute then-branch
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
		Ok(false) // don't jump, execute then-branch
	}

	/// Columnar `JumpIfTruePop`: the dual of `exec_jump_if_false_pop_columnar`.
	/// Rows whose condition is true jump to `target_addr`; rows whose condition is
	/// false continue executing the intermediate block.
	///
	/// Returns `Ok(true)` if the jump was taken for all active rows (caller should
	/// `continue` after setting ip). Returns `Ok(false)` otherwise.
	pub(crate) fn exec_jump_if_true_pop_columnar(&mut self, target_addr: usize) -> Result<bool> {
		let var = self.stack.pop()?;
		let bool_bv = extract_bool_bitvec(&var)?;

		let parent = self.effective_mask();
		// "Jumping rows" = rows where condition is true (within parent mask).
		let jumping = self.intersect_condition(&bool_bv);

		// Fast path: no rows jump (all false) - continue normally.
		if jumping.none() {
			return Ok(false);
		}

		// Fast path: all active rows jump (all true within parent).
		if jumping == parent {
			self.ip = target_addr;
			return Ok(true);
		}

		// Mixed: then-branch (continuing here) runs on false rows; else-branch (at
		// target_addr) runs on true rows.
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

	/// Enter a WHILE loop in columnar mode. Called when the first
	/// JumpIfFalsePop of a WHILE loop produces a mixed boolean Column.
	///
	/// This should be called instead of the standard MaskFrame push when
	/// we know we're at a WHILE loop (detected by the EnterScope(Loop) following).
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

	/// Masked Break: rows hitting BREAK exit the loop.
	pub(crate) fn exec_break_masked(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		let breaking_rows = self.effective_mask();
		if let Some(loop_state) = self.loop_mask_stack.last_mut() {
			loop_state.broken_mask = loop_state.broken_mask.or(&breaking_rows);

			// Remove breaking rows from active mask
			let remaining = loop_state.active_mask.and(&breaking_rows.not());
			loop_state.active_mask = remaining.clone();

			if remaining.none() {
				// All rows have broken - actually exit the loop
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
			// Not in a loop mask - use normal break
			for _ in 0..exit_scopes {
				self.symbols.exit_scope()?;
			}
			self.ip = addr;
		}
		Ok(())
	}

	/// Masked Continue: rows hitting CONTINUE skip the rest of the body.
	pub(crate) fn exec_continue_masked(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		let continuing_rows = self.effective_mask();
		if let Some(loop_state) = self.loop_mask_stack.last_mut() {
			// Remove continuing rows from active mask for rest of body
			let remaining = loop_state.active_mask.and(&continuing_rows.not());

			if remaining.none() {
				// All remaining rows have continued - jump to condition
				for _ in 0..exit_scopes {
					self.symbols.exit_scope()?;
				}
				// Restore loop's active mask for next iteration (all non-broken rows)
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

	/// Masked Jump: at the then/else boundary, switches to the else-branch.
	/// At all other times, behaves like a normal jump.
	///
	/// Returns `true` if this was a mask phase transition (caller should `continue`
	/// to skip normal IP increment), `false` for normal jump behavior.
	pub(crate) fn exec_jump_masked(&mut self, addr: usize) -> Result<bool> {
		if let Some(frame) = self.mask_stack.last_mut()
			&& frame.phase == MaskPhase::Then
		{
			// Finishing the then-branch: capture results
			let stack_delta: Vec<Variable> = {
				let mut delta = Vec::new();
				while self.stack.len() > frame.stack_depth {
					delta.push(self.stack.pop()?);
				}
				delta.reverse();
				delta
			};
			frame.then_stack_delta = stack_delta;

			// Snapshot modified variables is handled incrementally by
			// exec_store_var_masked, which records into then_var_snapshots.

			frame.end_addr = addr;
			frame.phase = MaskPhase::Else;
			self.active_mask = Some(frame.else_mask.clone());
			self.ip = frame.else_addr;
			return Ok(true); // redirect to else-branch
		}

		// Normal jump (not at a then/else boundary)
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
		Ok(true) // normal jump, caller should continue
	}

	/// Check if the current IP is a mask merge point. If so, merge then/else results.
	/// Must be called at the top of the dispatch loop, before instruction execution.
	pub(crate) fn check_mask_merge_point(&mut self) -> Result<bool> {
		let should_merge =
			self.mask_stack.last().is_some_and(|f| f.phase == MaskPhase::Else && self.ip == f.end_addr);

		if !should_merge {
			return Ok(false);
		}

		let frame = self.mask_stack.pop().unwrap();

		// Capture else-branch stack delta
		let mut else_stack_delta = Vec::new();
		while self.stack.len() > frame.stack_depth {
			else_stack_delta.push(self.stack.pop()?);
		}
		else_stack_delta.reverse();

		// Merge stack results (for IF expressions)
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

		// Merge modified variables
		for (name, then_snapshot) in &frame.then_var_snapshots {
			if let Some(current) = self.symbols.get(name) {
				let then_cols = variable_to_columns(then_snapshot);
				let else_cols = variable_to_columns(current);
				let merged_cols: Vec<Column> = then_cols
					.columns
					.iter()
					.zip(else_cols.columns.iter())
					.map(|(tc, ec)| {
						scatter_merge_columns(
							tc,
							ec,
							&frame.then_mask,
							&frame.else_mask,
							total_len,
						)
					})
					.collect();
				self.symbols.reassign(name.clone(), Variable::columns(Columns::new(merged_cols)))?;
			}
		}

		// Restore parent mask
		if self.mask_stack.is_empty() {
			self.active_mask = None;
		} else {
			self.active_mask = Some(frame.parent_mask);
		}

		Ok(true)
	}

	/// Masked StoreVar: only updates rows where the active mask is true.
	/// Also tracks the variable in the current MaskFrame for merge.
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
				// Variable doesn't exist yet - store directly
				self.symbols.reassign(name.to_string(), new_value)?;
			}
		}

		// Track in then_var_snapshots if we're in the Then phase
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
	use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
	use reifydb_type::{
		fragment::Fragment,
		util::bitvec::BitVec,
		value::{Value, r#type::Type},
	};

	use super::*;

	fn int4_column(name: &str, values: &[i32]) -> Column {
		let mut data = ColumnData::with_capacity(Type::Int4, values.len());
		for &v in values {
			data.push(v);
		}
		Column::new(Fragment::internal(name), data)
	}

	#[test]
	fn scatter_merge_all_then() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BitVec::from_slice(&[true, true, true]);
		let else_mask = BitVec::from_slice(&[false, false, false]);

		let merged = scatter_merge_columns(&then_col, &else_col, &then_mask, &else_mask, 3);
		assert_eq!(merged.data().get_value(0), Value::Int4(10));
		assert_eq!(merged.data().get_value(1), Value::Int4(20));
		assert_eq!(merged.data().get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_all_else() {
		let then_col = int4_column("x", &[10, 20, 30]);
		let else_col = int4_column("x", &[40, 50, 60]);
		let then_mask = BitVec::from_slice(&[false, false, false]);
		let else_mask = BitVec::from_slice(&[true, true, true]);

		let merged = scatter_merge_columns(&then_col, &else_col, &then_mask, &else_mask, 3);
		assert_eq!(merged.data().get_value(0), Value::Int4(40));
		assert_eq!(merged.data().get_value(1), Value::Int4(50));
		assert_eq!(merged.data().get_value(2), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_alternating() {
		let then_col = int4_column("x", &[10, 20, 30, 40]);
		let else_col = int4_column("x", &[90, 80, 70, 60]);
		let then_mask = BitVec::from_slice(&[true, false, true, false]);
		let else_mask = BitVec::from_slice(&[false, true, false, true]);

		let merged = scatter_merge_columns(&then_col, &else_col, &then_mask, &else_mask, 4);
		assert_eq!(merged.data().get_value(0), Value::Int4(10));
		assert_eq!(merged.data().get_value(1), Value::Int4(80));
		assert_eq!(merged.data().get_value(2), Value::Int4(30));
		assert_eq!(merged.data().get_value(3), Value::Int4(60));
	}

	#[test]
	fn merge_by_mask_selective_update() {
		let existing = Columns::new(vec![int4_column("x", &[1, 2, 3])]);
		let new_value = Columns::new(vec![int4_column("x", &[10, 20, 30])]);
		let mask = BitVec::from_slice(&[true, false, true]);

		let merged = merge_by_mask(&existing, &new_value, &mask).unwrap();
		let col = &merged.columns[0];
		assert_eq!(col.data().get_value(0), Value::Int4(10));
		assert_eq!(col.data().get_value(1), Value::Int4(2));
		assert_eq!(col.data().get_value(2), Value::Int4(30));
	}
}
