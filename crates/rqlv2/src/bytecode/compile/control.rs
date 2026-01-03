// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow compilation.

use std::mem::swap;

use crate::{
	bytecode::{
		BytecodeWriter,
		compile::{CompileError, LoopContext, PlanCompiler, Result},
		opcode::Opcode,
		program::Constant,
	},
	plan::node::control::{
		AssignNode, BreakNode, CallScriptFunctionNode, ConditionalNode, ContinueNode, DeclareNode,
		DeclareValue, DefineScriptFunctionNode, ExprStmtNode, ForIterableValue, ForNode, LoopNode, ReturnNode,
	},
};

impl PlanCompiler {
	pub(crate) fn compile_conditional<'bump>(&mut self, node: &ConditionalNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile condition
		self.compile_expr(node.condition)?;

		// JumpIfNot to else branch (or end)
		self.writer.emit_opcode(Opcode::JumpIfNot);
		let else_jump = self.writer.position();
		self.writer.emit_u16(0); // Placeholder

		// Compile then branch
		self.writer.emit_opcode(Opcode::EnterScope);
		for stmt in node.then_branch.iter() {
			self.compile_plan(stmt)?;
		}
		self.writer.emit_opcode(Opcode::ExitScope);

		// Handle else-if branches and else branch
		if !node.else_ifs.is_empty() || node.else_branch.is_some() {
			// Jump over else/else-if
			self.writer.emit_opcode(Opcode::Jump);
			let end_jump = self.writer.position();
			self.writer.emit_u16(0); // Placeholder

			// Patch else jump to here
			self.writer.patch_jump(else_jump);

			// Compile else-if branches
			let mut pending_jumps = vec![end_jump];
			for else_if in node.else_ifs.iter() {
				self.compile_expr(else_if.condition)?;
				self.writer.emit_opcode(Opcode::JumpIfNot);
				let next_jump = self.writer.position();
				self.writer.emit_u16(0);

				self.writer.emit_opcode(Opcode::EnterScope);
				for stmt in else_if.body.iter() {
					self.compile_plan(stmt)?;
				}
				self.writer.emit_opcode(Opcode::ExitScope);

				self.writer.emit_opcode(Opcode::Jump);
				pending_jumps.push(self.writer.position());
				self.writer.emit_u16(0);

				self.writer.patch_jump(next_jump);
			}

			// Compile else branch
			if let Some(else_stmts) = node.else_branch {
				self.writer.emit_opcode(Opcode::EnterScope);
				for stmt in else_stmts.iter() {
					self.compile_plan(stmt)?;
				}
				self.writer.emit_opcode(Opcode::ExitScope);
			}

			// Patch all pending jumps to end
			for jump in pending_jumps {
				self.writer.patch_jump(jump);
			}
		} else {
			// Patch else jump to end
			self.writer.patch_jump(else_jump);
		}

		Ok(())
	}

	pub(crate) fn compile_loop<'bump>(&mut self, node: &LoopNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Record loop start
		let loop_start = self.writer.position();

		// Push loop context
		self.loop_contexts.push(LoopContext {
			continue_target: loop_start,
			break_patches: Vec::new(),
		});

		// Enter scope
		self.writer.emit_opcode(Opcode::EnterScope);

		// Compile body
		for stmt in node.body.iter() {
			self.compile_plan(stmt)?;
		}

		// Exit scope
		self.writer.emit_opcode(Opcode::ExitScope);

		// Jump back to loop start
		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		let offset = (loop_start as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);

		// Patch all break jumps
		let loop_end = self.writer.position();
		let context = self.loop_contexts.pop().expect("loop context");
		for break_pos in context.break_patches {
			self.writer.patch_jump_at(break_pos, loop_end);
		}

		Ok(())
	}

	pub(crate) fn compile_for<'bump>(&mut self, node: &ForNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile iterable (expression or pipeline)
		match &node.iterable {
			ForIterableValue::Expression(expr) => {
				self.compile_expr(expr)?;
			}
			ForIterableValue::Plan(plans) => {
				for plan in plans.iter() {
					self.compile_plan(plan)?;
				}
			}
		}

		// Collect to frame
		self.writer.emit_opcode(Opcode::Collect);

		// Allocate internal variable IDs for loop state
		let frame_var = self.alloc_internal_var();
		let len_var = self.alloc_internal_var();
		let idx_var = self.alloc_internal_var();

		// Store frame (internal variable)
		self.writer.emit_opcode(Opcode::StoreInternalVar);
		self.writer.emit_u16(frame_var);

		// Get frame length
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(frame_var);
		self.writer.emit_opcode(Opcode::FrameLen);
		self.writer.emit_opcode(Opcode::StoreInternalVar);
		self.writer.emit_u16(len_var);

		// Initialize index = 0
		let zero_const = self.program.add_constant(Constant::Int(0));
		self.writer.emit_opcode(Opcode::PushConst);
		self.writer.emit_u16(zero_const);
		self.writer.emit_opcode(Opcode::StoreInternalVar);
		self.writer.emit_u16(idx_var);

		// Loop start
		let loop_start = self.writer.position();

		self.loop_contexts.push(LoopContext {
			continue_target: loop_start,
			break_patches: Vec::new(),
		});

		// Check idx < len
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(idx_var);
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(len_var);
		self.writer.emit_opcode(Opcode::IntLt);

		// JumpIfNot to end
		self.writer.emit_opcode(Opcode::JumpIfNot);
		let end_jump = self.writer.position();
		self.writer.emit_u16(0);

		// Enter scope
		self.writer.emit_opcode(Opcode::EnterScope);

		// Get current row
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(frame_var);
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(idx_var);
		self.writer.emit_opcode(Opcode::FrameRow);

		// Store in loop variable (user variable ID)
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u32(node.variable.variable_id);

		// Compile body
		for stmt in node.body.iter() {
			self.compile_plan(stmt)?;
		}

		// Exit scope
		self.writer.emit_opcode(Opcode::ExitScope);

		// Increment index
		self.writer.emit_opcode(Opcode::LoadInternalVar);
		self.writer.emit_u16(idx_var);
		let one_const = self.program.add_constant(Constant::Int(1));
		self.writer.emit_opcode(Opcode::PushConst);
		self.writer.emit_u16(one_const);
		self.writer.emit_opcode(Opcode::IntAdd);
		self.writer.emit_opcode(Opcode::StoreInternalVar);
		self.writer.emit_u16(idx_var);

		// Jump back to loop start
		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		let offset = (loop_start as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);

		// Patch end jump
		let loop_end = self.writer.position();
		self.writer.patch_jump_at(end_jump, loop_end);

		// Patch break jumps
		let context = self.loop_contexts.pop().expect("loop context");
		for break_pos in context.break_patches {
			self.writer.patch_jump_at(break_pos, loop_end);
		}

		Ok(())
	}

	pub(crate) fn compile_declare<'bump>(&mut self, node: &DeclareNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		match &node.value {
			DeclareValue::Expression(expr) => {
				self.compile_expr(expr)?;
				// Store using variable ID
				self.writer.emit_opcode(Opcode::StoreVar);
				self.writer.emit_u32(node.variable.variable_id);
			}
			DeclareValue::Plan(plans) => {
				for plan in plans.iter() {
					self.compile_plan(plan)?;
				}
				// Store pipeline using variable ID
				self.writer.emit_opcode(Opcode::StorePipeline);
				self.writer.emit_u32(node.variable.variable_id);
			}
		}
		Ok(())
	}

	pub(crate) fn compile_assign<'bump>(&mut self, node: &AssignNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		match &node.value {
			DeclareValue::Expression(expr) => {
				self.compile_expr(expr)?;
				// Update using variable ID
				self.writer.emit_opcode(Opcode::UpdateVar);
				self.writer.emit_u32(node.variable.variable_id);
			}
			DeclareValue::Plan(plans) => {
				for plan in plans.iter() {
					self.compile_plan(plan)?;
				}
				// Update pipeline using variable ID
				self.writer.emit_opcode(Opcode::StorePipeline);
				self.writer.emit_u32(node.variable.variable_id);
			}
		}
		Ok(())
	}

	pub(crate) fn compile_return<'bump>(&mut self, node: &ReturnNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		if let Some(value) = node.value {
			self.compile_expr(value)?;
		}
		self.writer.emit_opcode(Opcode::Return);
		Ok(())
	}

	pub(crate) fn compile_break<'bump>(&mut self, node: &BreakNode) -> Result<()> {
		self.record_span(node.span);

		let context = self.loop_contexts.last_mut().ok_or_else(|| CompileError::Internal {
			message: "break outside of loop".to_string(),
		})?;

		self.writer.emit_opcode(Opcode::Jump);
		let jump_pos = self.writer.position();
		self.writer.emit_i16(0);
		context.break_patches.push(jump_pos);
		Ok(())
	}

	pub(crate) fn compile_continue<'bump>(&mut self, node: &ContinueNode) -> Result<()> {
		self.record_span(node.span);

		let context = self.loop_contexts.last().ok_or_else(|| CompileError::Internal {
			message: "continue outside of loop".to_string(),
		})?;

		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		let offset = (context.continue_target as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);
		Ok(())
	}

	pub(crate) fn compile_define_script_function<'bump>(
		&mut self,
		node: &DefineScriptFunctionNode<'bump>,
	) -> Result<()> {
		self.record_span(node.span);

		let func_index = self.pending_script_functions.len();
		self.script_function_indices.insert(node.name.to_string(), func_index as u16);

		let mut func_writer = BytecodeWriter::new();
		swap(&mut self.writer, &mut func_writer);
		self.writer.emit_opcode(Opcode::EnterScope);

		// Compile function body
		for plan in node.body.iter() {
			self.compile_plan(plan)?;
		}

		// Implicit return at end of function
		self.writer.emit_opcode(Opcode::ExitScope);
		self.writer.emit_opcode(Opcode::Return);

		swap(&mut self.writer, &mut func_writer);

		self.pending_script_functions.push((node.name.to_string(), func_writer.finish()));

		Ok(())
	}

	pub(crate) fn compile_call_script_function<'bump>(
		&mut self,
		node: &CallScriptFunctionNode<'bump>,
	) -> Result<()> {
		self.record_span(node.span);

		let func_index =
			*self.script_function_indices.get(node.name).ok_or_else(|| CompileError::Internal {
				message: format!("undefined script function: {}", node.name),
			})?;

		self.writer.emit_opcode(Opcode::Call);
		self.writer.emit_u16(func_index);

		Ok(())
	}

	pub(crate) fn compile_expr_stmt<'bump>(&mut self, node: &ExprStmtNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		// Compile the expression for its side effects
		self.compile_expr(node.expr)?;
		// Note: If the expression leaves a value on the stack and we want to discard it,
		// we would need a Pop opcode. For now, builtin functions like console::log
		// that return nothing won't leave a value on the stack.
		Ok(())
	}
}
