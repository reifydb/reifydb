// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integer arithmetic opcodes: IntAdd, IntSub, IntMul, IntDiv, IntLt, IntLe, IntGt, IntGe, IntEq, IntNe.

use reifydb_type::Value;

use crate::error::{Result, VmError};
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::operand::OperandValue;

use super::HandlerContext;

/// Helper to pop two integers from the stack.
fn pop_two_ints(ctx: &mut HandlerContext) -> Result<(i64, i64)> {
	let b = ctx.vm.pop_operand()?;
	let a = ctx.vm.pop_operand()?;

	match (a, b) {
		(OperandValue::Scalar(Value::Int8(a)), OperandValue::Scalar(Value::Int8(b))) => Ok((a, b)),
		_ => Err(VmError::ExpectedInteger),
	}
}

/// Execute IntAdd - adds two integers.
pub fn int_add(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Int8(a + b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntSub - subtracts two integers.
pub fn int_sub(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Int8(a - b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntMul - multiplies two integers.
pub fn int_mul(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Int8(a * b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntDiv - divides two integers.
pub fn int_div(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	if b == 0 {
		return Err(VmError::DivisionByZero);
	}
	ctx.vm.push_operand(OperandValue::Scalar(Value::Int8(a / b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntLt - less than comparison.
pub fn int_lt(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a < b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntLe - less than or equal comparison.
pub fn int_le(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a <= b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntGt - greater than comparison.
pub fn int_gt(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a > b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntGe - greater than or equal comparison.
pub fn int_ge(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a >= b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntEq - equality comparison.
pub fn int_eq(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a == b)))?;
	Ok(ctx.advance_and_continue())
}

/// Execute IntNe - inequality comparison.
pub fn int_ne(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let (a, b) = pop_two_ints(ctx)?;
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(a != b)))?;
	Ok(ctx.advance_and_continue())
}
