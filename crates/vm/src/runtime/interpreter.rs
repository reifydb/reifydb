// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{columns::Columns, data::ColumnData};
use reifydb_rqlv2::{
	bytecode::{
		instruction::BytecodeReader,
		opcode::OperatorKind,
		program::{CompiledProgramBuilder, SubqueryDef},
	},
	expression::eval::{context::EvalContext, value::EvalValue},
};
use reifydb_transaction::standard::{IntoStandardTransaction, StandardTransaction};

use super::{
	dispatch::{self, DispatchResult},
	operand::OperandValue,
	script::BytecodeScriptCaller,
	state::VmState,
};
use crate::{
	error::{Result, VmError},
	handler::HandlerContext,
	operator::{filter::FilterOp, project::ProjectOp, select::SelectOp, sort::SortOp, take::TakeOp},
	pipeline::{self, Pipeline},
};

impl VmState {
	/// Execute the program until halt or yield.
	pub fn execute<T: IntoStandardTransaction + ?Sized>(&mut self, tx: &mut T) -> Result<Option<Pipeline>> {
		let mut std_tx = tx.into_standard_transaction();

		loop {
			let result = self.step(Some(&mut std_tx))?;
			match result {
				DispatchResult::Continue => continue,
				DispatchResult::Halt => break,
				DispatchResult::Yield(pipeline) => return Ok(Some(pipeline)),
			}
		}

		// Return top of pipeline stack if present
		Ok(self.pipeline_stack.pop())
	}

	/// Execute a single instruction using the dispatch table.
	///
	/// The transaction is optional - if None, only in-memory sources can be used.
	pub fn step<'a>(&mut self, rx: Option<&mut StandardTransaction<'a>>) -> Result<DispatchResult> {
		// Clone bytecode to avoid borrow conflict with HandlerContext
		let bytecode = self.program.bytecode.clone();
		let mut reader = BytecodeReader::new(&bytecode);
		reader.set_position(self.ip);

		// Read the opcode byte
		let opcode_byte = reader.read_u8().ok_or(VmError::InvalidBytecode {
			position: self.ip,
		})?;

		// Create handler context and dispatch
		let mut ctx = HandlerContext::new(self, &mut reader, rx);
		dispatch::dispatch_step(&mut ctx, opcode_byte)
	}

	pub fn apply_operator(&mut self, op_kind: OperatorKind) -> Result<()> {
		let pipeline = self.pop_pipeline()?;

		let new_pipeline = match op_kind {
			OperatorKind::Filter => {
				let expr_ref = self.pop_operand()?;
				let compiled = self.resolve_compiled_filter(&expr_ref)?;
				let eval_ctx = self.capture_scope_context();
				FilterOp::with_context(compiled, eval_ctx).apply(pipeline)
			}

			OperatorKind::Select => {
				let col_list = self.pop_operand()?;
				let columns = self.resolve_col_list(&col_list)?;
				SelectOp::new(columns).apply(pipeline)
			}

			OperatorKind::Extend => {
				let spec = self.pop_operand()?;
				let extensions = self.resolve_extension_spec(&spec)?;
				let eval_ctx = self.capture_scope_context();
				ProjectOp::extend_with_context(extensions, eval_ctx).apply(pipeline)
			}

			OperatorKind::Map => {
				let spec = self.pop_operand()?;
				let extensions = self.resolve_extension_spec(&spec)?;
				let eval_ctx = self.capture_scope_context();
				ProjectOp::replace_with_context(extensions, eval_ctx).apply(pipeline)
			}

			OperatorKind::Take => {
				let limit = self.pop_operand()?;
				let n = self.resolve_int(&limit)?;
				TakeOp::new(n as usize).apply(pipeline)
			}

			OperatorKind::Sort => {
				let spec = self.pop_operand()?;
				let sort_spec = self.resolve_sort_spec(&spec)?;
				SortOp::new(sort_spec).apply(pipeline)
			}

			// Not yet implemented
			_ => {
				return Err(VmError::UnsupportedOperation {
					operation: format!("OperatorKind {:?} not yet implemented", op_kind),
				});
			}
		};

		self.push_pipeline(new_pipeline)?;
		Ok(())
	}

	/// Capture all scope variables into an EvalContext for expression evaluation.
	///
	/// This method creates an EvalContext with a BytecodeScriptCaller that can
	/// execute script functions by running their bytecode.
	pub fn capture_scope_context(&self) -> EvalContext {
		// Create a script function caller that can execute bytecode
		let caller = Arc::new(BytecodeScriptCaller::new(self.program.clone()));
		let mut ctx = EvalContext::with_script_functions(caller);

		// Add function registry if available
		if let Some(functions) = &self.context.functions {
			ctx = ctx.with_functions(Arc::clone(functions));
		}

		ctx
	}

	/// Execute a subquery and return the collected result.
	///
	/// This creates a nested VM execution with the subquery's bytecode.
	pub fn execute_subquery<'a>(
		&self,
		subquery_def: &SubqueryDef,
		rx: Option<&mut StandardTransaction<'a>>,
	) -> Result<Columns> {
		let mut builder = CompiledProgramBuilder::new();
		builder.bytecode = subquery_def.bytecode.clone();
		builder.constants = subquery_def.constants.clone();
		builder.sources = subquery_def.sources.clone();
		builder.source_map = subquery_def.source_map.clone();
		// Subqueries don't have their own nested subqueries, column lists, etc.
		// (These are already initialized as empty by CompiledProgramBuilder::new())

		let subquery_program = builder.build();

		// Create a new VM state for the subquery
		let mut subquery_vm = VmState::new(subquery_program, self.context.clone());

		// Execute the subquery - it should end with Collect which pushes a Frame
		if let Some(rx) = rx {
			// For now, we can't share the transaction, so this won't work for real table access
			// This is a limitation we need to address in the future
			// For testing, we'll use a workaround
			let _ = subquery_vm.execute(rx)?;
		} else {
			// No transaction available - subquery can only work with in-memory data
			// This shouldn't happen in practice
			return Err(VmError::NoTransactionAvailable);
		}

		// The subquery should have pushed a Frame onto the operand stack
		if let Some(OperandValue::Frame(columns)) = subquery_vm.operand_stack.pop() {
			Ok(columns)
		} else {
			// Check pipeline stack as fallback
			if let Some(pipeline) = subquery_vm.pipeline_stack.pop() {
				pipeline::collect(pipeline)
			} else {
				Ok(Columns::empty())
			}
		}
	}
}

/// Convert an OperandValue to an EvalValue if possible.
///
/// DEPRECATED: This function is no longer used since the old DSL module is deprecated.
#[allow(dead_code)]
fn operand_to_eval_value(value: &OperandValue) -> Option<EvalValue> {
	match value {
		OperandValue::Scalar(v) => Some(EvalValue::Scalar(v.clone())),
		OperandValue::Record(r) => {
			// Convert Record to HashMap for RQLv2's EvalValue
			let mut map = HashMap::new();
			for (name, val) in &r.fields {
				map.insert(name.clone(), val.clone());
			}
			Some(EvalValue::Record(map))
		}
		_ => None, // Other types cannot be used in expressions
	}
}

/// Broadcast a scalar value to a column with the given row count.
#[allow(dead_code)]
fn broadcast_scalar_to_column(value: &reifydb_type::value::Value, row_count: usize) -> ColumnData {
	match value {
		reifydb_type::value::Value::Boolean(b) => ColumnData::bool(vec![*b; row_count]),
		reifydb_type::value::Value::Int8(n) => ColumnData::int8(vec![*n; row_count]),
		reifydb_type::value::Value::Float8(f) => ColumnData::float8(vec![f.value(); row_count]),
		reifydb_type::value::Value::Utf8(s) => ColumnData::utf8(vec![s.clone(); row_count]),
		reifydb_type::value::Value::Undefined => ColumnData::int8(vec![0; row_count]),
		_ => ColumnData::int8(vec![0; row_count]),
	}
}
