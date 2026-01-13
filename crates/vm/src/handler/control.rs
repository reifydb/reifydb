// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow opcodes: Nop, Halt.

use crate::error::Result;
use crate::runtime::dispatch::DispatchResult;

use super::HandlerContext;

/// Execute the Nop opcode - does nothing, just advances IP.
pub fn nop(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	Ok(ctx.advance_and_continue())
}

/// Execute the Halt opcode - stops VM execution.
pub fn halt(_ctx: &mut HandlerContext) -> Result<DispatchResult> {
	Ok(DispatchResult::Halt)
}
