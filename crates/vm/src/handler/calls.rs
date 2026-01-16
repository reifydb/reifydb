// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Function call opcodes: Call, Return, CallBuiltin.

use super::HandlerContext;
use crate::{
	error::{Result, VmError},
	runtime::{builtin::BuiltinRegistry, dispatch::DispatchResult, stack::CallFrame},
};

/// Call - call a user-defined function.
pub fn call(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let func_index = ctx.read_u16()?;
	let next_ip = ctx.reader.position();

	let func_def =
		ctx.vm.program.script_functions.get(func_index as usize).ok_or(VmError::InvalidFunctionIndex {
			index: func_index,
		})?;

	// Push call frame
	let frame = CallFrame::new(
		func_index,
		next_ip,
		ctx.vm.operand_stack.len(),
		ctx.vm.pipeline_stack.len(),
		ctx.vm.scopes.depth(),
	);

	if !ctx.vm.call_stack.push(frame) {
		return Err(VmError::StackOverflow {
			stack: "call".into(),
		});
	}

	// Jump to function body
	ctx.vm.ip = func_def.bytecode_offset;
	Ok(DispatchResult::Continue)
}

/// Return - return from a function or yield from top-level.
pub fn return_op(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	// Check if we're at the top level (no call frames)
	if ctx.vm.call_stack.is_empty() {
		// Top-level return: yield the pipeline if present
		if let Some(pipeline) = ctx.vm.pipeline_stack.pop() {
			return Ok(DispatchResult::Yield(pipeline));
		} else {
			// No pipeline to return, just halt
			return Ok(DispatchResult::Halt);
		}
	}

	// Inside a function: pop call frame and return to caller
	let frame = ctx.vm.call_stack.pop().unwrap();

	// Restore scope
	ctx.vm.scopes.pop_to_depth(frame.scope_depth);

	// Clean up operand stack (keep return value if any)
	let return_value = if ctx.vm.operand_stack.len() > frame.operand_base {
		ctx.vm.operand_stack.pop()
	} else {
		None
	};
	ctx.vm.operand_stack.truncate(frame.operand_base);
	if let Some(value) = return_value {
		ctx.vm.push_operand(value)?;
	}

	// Return to caller
	ctx.vm.ip = frame.return_address;
	Ok(DispatchResult::Continue)
}

/// CallBuiltin - call a builtin function by name.
pub fn call_builtin(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let name_index = ctx.read_u16()?;
	let arg_count = ctx.read_u8()? as usize;

	// Get function name from constant pool
	let func_name = ctx.vm.get_constant_string(name_index)?;

	// Pop arguments from stack (in reverse order)
	let mut args = Vec::with_capacity(arg_count);
	for _ in 0..arg_count {
		args.push(ctx.vm.pop_operand()?);
	}
	args.reverse();

	// Look up and execute builtin
	let registry = BuiltinRegistry::new();
	if let Some(result) = registry.call(&func_name, &args)? {
		ctx.vm.push_operand(result)?;
	}

	Ok(ctx.advance_and_continue())
}
