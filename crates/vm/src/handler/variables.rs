// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Variable opcodes: LoadVar, StoreVar, UpdateVar, LoadPipeline, StorePipeline,
//! LoadInternalVar, StoreInternalVar.

use crate::error::{Result, VmError};
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::operand::OperandValue;

use super::HandlerContext;

/// LoadVar - load a variable by ID onto the operand stack.
pub fn load_var(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u32()?;
	let value = ctx.vm.scopes.get_by_id(var_id).cloned().ok_or(VmError::UndefinedVariable {
		name: format!("${}", var_id),
	})?;
	ctx.vm.push_operand(value)?;
	Ok(ctx.advance_and_continue())
}

/// StoreVar - store a value from the operand stack into a variable.
pub fn store_var(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u32()?;
	let value = ctx.vm.pop_operand()?;
	ctx.vm.scopes.set_by_id(var_id, value);
	Ok(ctx.advance_and_continue())
}

/// UpdateVar - update an existing variable (searches all scopes).
pub fn update_var(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u32()?;
	let value = ctx.vm.pop_operand()?;
	if !ctx.vm.scopes.update_by_id(var_id, value) {
		return Err(VmError::UndefinedVariable {
			name: format!("${}", var_id),
		});
	}
	Ok(ctx.advance_and_continue())
}

/// LoadPipeline - load a pipeline variable and push it onto the pipeline stack.
pub fn load_pipeline(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u32()?;
	let value = ctx.vm.scopes.get_by_id(var_id).cloned().ok_or(VmError::UndefinedVariable {
		name: format!("${}", var_id),
	})?;

	match value {
		OperandValue::PipelineRef(handle) => {
			let pipeline = ctx.vm.take_pipeline(&handle).ok_or(VmError::InvalidPipelineHandle)?;
			ctx.vm.push_pipeline(pipeline)?;
		}
		_ => return Err(VmError::ExpectedPipeline),
	}
	Ok(ctx.advance_and_continue())
}

/// StorePipeline - pop a pipeline and store it in a variable.
pub fn store_pipeline(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u32()?;
	let pipeline = ctx.vm.pop_pipeline()?;
	let handle = ctx.vm.register_pipeline(pipeline);
	ctx.vm.scopes.set_by_id(var_id, OperandValue::PipelineRef(handle));
	Ok(ctx.advance_and_continue())
}

/// LoadInternalVar - load a compiler-generated internal variable.
pub fn load_internal_var(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u16()?;
	let value = ctx.vm.internal_vars.get(&var_id).cloned().ok_or(VmError::UndefinedVariable {
		name: format!("__internal_{}", var_id),
	})?;
	ctx.vm.push_operand(value)?;
	Ok(ctx.advance_and_continue())
}

/// StoreInternalVar - store a value into a compiler-generated internal variable.
pub fn store_internal_var(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let var_id = ctx.read_u16()?;
	let value = ctx.vm.pop_operand()?;
	ctx.vm.internal_vars.insert(var_id, value);
	Ok(ctx.advance_and_continue())
}
