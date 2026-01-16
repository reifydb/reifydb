// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Scope management opcodes: EnterScope, ExitScope.

use super::HandlerContext;
use crate::{error::Result, runtime::dispatch::DispatchResult};

/// Execute the EnterScope opcode - creates a new variable scope.
pub fn enter_scope(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	ctx.vm.scopes.push();
	Ok(ctx.advance_and_continue())
}

/// Execute the ExitScope opcode - removes the current variable scope.
pub fn exit_scope(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	ctx.vm.scopes.pop();
	Ok(ctx.advance_and_continue())
}
