// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pipeline opcodes: Source, Inline, Apply, Collect, PopPipeline, Merge,
//! EvalMapWithoutInput, EvalExpandWithoutInput, FetchBatch, CheckComplete.

use reifydb_core::value::column::{Column, columns::Columns};
use reifydb_rqlv2::bytecode::opcode::OperatorKind;
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

use super::HandlerContext;
use crate::{
	error::{Result, VmError},
	operator::scan_table::ScanTableOp,
	pipeline,
	runtime::{dispatch::DispatchResult, operand::OperandValue},
};

/// Source - start a table scan and push the first batch as a pipeline.
pub fn source(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let source_index = ctx.read_u16()?;

	let source_def = ctx.vm.program.sources.get(source_index as usize).ok_or(VmError::InvalidSourceIndex {
		index: source_index,
	})?;

	if let (Some(catalog), Some(tx)) = (&ctx.vm.context.catalog, ctx.tx.as_mut()) {
		// 1. Initialize scan state
		let op = ScanTableOp::new(source_def.name.clone(), ctx.vm.context.config.batch_size);
		let mut scan_state = op.initialize(catalog, *tx)?;

		// 2. Fetch first batch
		let batch_size = ctx.vm.context.config.batch_size;
		let batch_opt = ScanTableOp::next_batch(&mut scan_state, *tx, batch_size)?;

		// 3. Store scan state
		ctx.vm.active_scans.insert(source_index, scan_state);

		// 4. Push first batch as pipeline
		let pipeline = if let Some(batch) = batch_opt {
			pipeline::from_batch(batch)
		} else {
			pipeline::empty()
		};
		ctx.vm.push_pipeline(pipeline)?;

		Ok(ctx.advance_and_continue())
	} else {
		Err(VmError::TableNotFound {
			name: source_def.name.clone(),
		})
	}
}

/// Inline - push an empty pipeline onto the stack.
pub fn inline(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let pipeline = Box::new(std::iter::empty());
	ctx.vm.push_pipeline(pipeline)?;
	Ok(ctx.advance_and_continue())
}

/// EvalMapWithoutInput / EvalExpandWithoutInput - evaluate expressions without input.
pub fn eval_without_input(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	// Pop extension spec from operand stack
	let spec_value = ctx.vm.pop_operand()?;
	let extensions = ctx.vm.resolve_extension_spec(&spec_value)?;

	// Create evaluation context
	let eval_ctx = ctx.vm.capture_scope_context();

	// Create empty columns with row_count=1
	let empty_with_one_row = Columns {
		row_numbers: CowVec::new(vec![RowNumber(0)]),
		columns: CowVec::new(vec![]),
	};

	// Evaluate each expression to create result columns
	let mut result_columns = Vec::new();
	for (name, compiled_expr) in extensions {
		let column = compiled_expr.eval(&empty_with_one_row, &eval_ctx)?;
		let renamed = Column::new(Fragment::from(name), column.data().clone());
		result_columns.push(renamed);
	}

	// Create a single-row Columns from the evaluated expressions
	let columns = Columns::new(result_columns);

	// Create a pipeline from the columns
	let new_pipeline = pipeline::from_columns(columns);
	ctx.vm.push_pipeline(new_pipeline)?;

	Ok(ctx.advance_and_continue())
}

/// Apply - apply an operator to the top pipeline.
pub fn apply(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let op_kind_byte = ctx.read_u8()?;
	let op_kind = OperatorKind::try_from(op_kind_byte).map_err(|_| VmError::UnknownOperatorKind {
		kind: op_kind_byte,
	})?;
	ctx.vm.apply_operator(op_kind)?;
	Ok(ctx.advance_and_continue())
}

/// Collect - collect a pipeline into a frame and push onto operand stack.
pub fn collect(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let pipeline = ctx.vm.pop_pipeline()?;
	let columns = pipeline::collect(pipeline)?;
	ctx.vm.push_operand(OperandValue::Frame(columns))?;
	Ok(ctx.advance_and_continue())
}

/// PopPipeline - discard the top pipeline.
pub fn pop_pipeline(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let _ = ctx.vm.pop_pipeline()?;
	Ok(ctx.advance_and_continue())
}

/// Merge - not yet implemented.
pub fn merge(_ctx: &mut HandlerContext) -> Result<DispatchResult> {
	Err(VmError::UnsupportedOperation {
		operation: "Merge".to_string(),
	})
}

/// FetchBatch - fetch the next batch from an active scan.
pub fn fetch_batch(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let source_index = ctx.read_u16()?;

	if let Some(tx) = ctx.tx.as_mut() {
		let scan_state =
			ctx.vm.active_scans
				.get_mut(&source_index)
				.ok_or(VmError::Internal("scan not initialized".to_string()))?;

		let batch_size = ctx.vm.context.config.batch_size;
		let batch_opt = ScanTableOp::next_batch(scan_state, *tx, batch_size)?;

		if let Some(batch) = batch_opt {
			// Has more data - push batch and true
			ctx.vm.push_pipeline(pipeline::from_batch(batch))?;
			ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(true)))?;
		} else {
			// Exhausted - push empty pipeline and false
			ctx.vm.push_pipeline(pipeline::empty())?;
			ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(false)))?;
		}
		Ok(ctx.advance_and_continue())
	} else {
		Err(VmError::Internal("FetchBatch requires transaction".to_string()))
	}
}

/// CheckComplete - consume a completion flag from the operand stack.
pub fn check_complete(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let _complete = match ctx.vm.pop_operand()? {
		OperandValue::Scalar(Value::Boolean(b)) => b,
		_ => return Err(VmError::ExpectedBoolean),
	};
	// For now, just consume the flag
	Ok(ctx.advance_and_continue())
}
