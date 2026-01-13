// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Stack push opcodes: PushConst, PushExpr, PushColRef, PushColList, PushSortSpec, PushExtSpec.

use crate::error::{Result, VmError};
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::operand::OperandValue;

use super::HandlerContext;

/// PushConst - push a constant value onto the operand stack.
pub fn push_const(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.read_u16()?;
	let value = ctx.vm.get_constant(index)?;
	ctx.vm.push_operand(OperandValue::Scalar(value))?;
	Ok(ctx.advance_and_continue())
}

/// PushExpr - push an expression reference onto the operand stack.
pub fn push_expr(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.read_u16()?;
	ctx.vm.push_operand(OperandValue::ExprRef(index))?;
	Ok(ctx.advance_and_continue())
}

/// PushColRef - push a column reference by name onto the operand stack.
pub fn push_col_ref(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let name_index = ctx.read_u16()?;
	let name = ctx.vm.get_constant_string(name_index)?;
	ctx.vm.push_operand(OperandValue::ColRef(name))?;
	Ok(ctx.advance_and_continue())
}

/// PushColList - push a column list onto the operand stack.
pub fn push_col_list(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.read_u16()?;
	let columns = ctx.vm.program.column_lists.get(index as usize).cloned().ok_or(
		VmError::InvalidColumnListIndex { index },
	)?;
	ctx.vm.push_operand(OperandValue::ColList(columns))?;
	Ok(ctx.advance_and_continue())
}

/// PushSortSpec - push a sort specification reference onto the operand stack.
pub fn push_sort_spec(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.read_u16()?;
	ctx.vm.push_operand(OperandValue::SortSpecRef(index))?;
	Ok(ctx.advance_and_continue())
}

/// PushExtSpec - push an extension specification reference onto the operand stack.
pub fn push_ext_spec(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.read_u16()?;
	ctx.vm.push_operand(OperandValue::ExtSpecRef(index))?;
	Ok(ctx.advance_and_continue())
}
