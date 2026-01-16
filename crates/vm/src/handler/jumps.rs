// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Jump opcodes: Jump, JumpIf, JumpIfNot.

use super::HandlerContext;
use crate::{error::Result, runtime::dispatch::DispatchResult};

/// Jump - unconditional jump to offset.
pub fn jump(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let offset = ctx.read_i16()?;
	// Offset is relative to the position after reading the offset
	let base = ctx.reader.position();
	ctx.vm.ip = (base as i64 + offset as i64) as usize;
	Ok(DispatchResult::Continue)
}

/// JumpIf - jump if top of stack is truthy.
pub fn jump_if(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let offset = ctx.read_i16()?;
	let next_ip = ctx.reader.position();
	let condition = ctx.vm.pop_operand()?;

	if ctx.vm.is_truthy(&condition)? {
		ctx.vm.ip = (next_ip as i64 + offset as i64) as usize;
	} else {
		ctx.vm.ip = next_ip;
	}
	Ok(DispatchResult::Continue)
}

/// JumpIfNot - jump if top of stack is falsy.
pub fn jump_if_not(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let offset = ctx.read_i16()?;
	let next_ip = ctx.reader.position();
	let condition = ctx.vm.pop_operand()?;

	if !ctx.vm.is_truthy(&condition)? {
		ctx.vm.ip = (next_ip as i64 + offset as i64) as usize;
	} else {
		ctx.vm.ip = next_ip;
	}
	Ok(DispatchResult::Continue)
}
