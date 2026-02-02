// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_rql::plan::physical::PhysicalPlan;
use reifydb_type::error::diagnostic::runtime;

use super::{Addr, Instruction};
use crate::vm::stack::ScopeType;

struct LoopContext {
	/// Address of the condition check / ForNext (target for Continue)
	continue_addr: Addr,
	/// Placeholder indices in `instructions` where the loop-end address must be backpatched
	break_patches: Vec<usize>,
	/// Scope depth at the point the loop was entered (used to compute exit_scopes)
	scope_depth: usize,
}

struct Compiler {
	instructions: Vec<Instruction>,
	loop_stack: Vec<LoopContext>,
	scope_depth: usize,
}

impl Compiler {
	fn new() -> Self {
		Self {
			instructions: Vec::new(),
			loop_stack: Vec::new(),
			scope_depth: 0,
		}
	}

	fn emit(&mut self, instr: Instruction) -> usize {
		let addr = self.instructions.len();
		self.instructions.push(instr);
		addr
	}

	fn current_addr(&self) -> Addr {
		self.instructions.len()
	}

	/// Emit EvalCondition + JumpIfFalsePop, return the index of JumpIfFalsePop for backpatching
	fn emit_conditional_jump(&mut self, condition: reifydb_rql::expression::Expression) -> usize {
		self.emit(Instruction::EvalCondition(condition));
		self.emit(Instruction::JumpIfFalsePop(0))
	}

	fn compile_plan(&mut self, plan: PhysicalPlan) -> crate::Result<()> {
		match plan {
			// DDL
			PhysicalPlan::CreateNamespace(node) => {
				self.emit(Instruction::CreateNamespace(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateTable(node) => {
				self.emit(Instruction::CreateTable(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateRingBuffer(node) => {
				self.emit(Instruction::CreateRingBuffer(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateFlow(node) => {
				self.emit(Instruction::CreateFlow(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateDeferredView(node) => {
				self.emit(Instruction::CreateDeferredView(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateTransactionalView(node) => {
				self.emit(Instruction::CreateTransactionalView(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateDictionary(node) => {
				self.emit(Instruction::CreateDictionary(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateSubscription(node) => {
				self.emit(Instruction::CreateSubscription(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterSequence(node) => {
				self.emit(Instruction::AlterSequence(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterTable(node) => {
				self.emit(Instruction::AlterTable(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterView(node) => {
				self.emit(Instruction::AlterView(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterFlow(node) => {
				self.emit(Instruction::AlterFlow(node));
				self.emit(Instruction::Emit);
			}

			// DML
			PhysicalPlan::Delete(node) => {
				self.emit(Instruction::Delete(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DeleteRingBuffer(node) => {
				self.emit(Instruction::DeleteRingBuffer(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertTable(node) => {
				self.emit(Instruction::InsertTable(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertRingBuffer(node) => {
				self.emit(Instruction::InsertRingBuffer(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertDictionary(node) => {
				self.emit(Instruction::InsertDictionary(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Update(node) => {
				self.emit(Instruction::Update(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::UpdateRingBuffer(node) => {
				self.emit(Instruction::UpdateRingBuffer(node));
				self.emit(Instruction::Emit);
			}

			// Variables
			PhysicalPlan::Declare(node) => {
				self.emit(Instruction::Declare(node));
			}
			PhysicalPlan::Assign(node) => {
				self.emit(Instruction::Assign(node));
			}

			// Control flow
			PhysicalPlan::Conditional(node) => {
				self.compile_conditional(node)?;
			}
			PhysicalPlan::Loop(node) => {
				self.compile_loop(node)?;
			}
			PhysicalPlan::While(node) => {
				self.compile_while(node)?;
			}
			PhysicalPlan::For(node) => {
				self.compile_for(node)?;
			}
			PhysicalPlan::Break => {
				self.compile_break()?;
			}
			PhysicalPlan::Continue => {
				self.compile_continue()?;
			}

			// Everything else is a query pipeline
			other => {
				self.emit(Instruction::Query(other));
				self.emit(Instruction::Emit);
			}
		}
		Ok(())
	}

	fn compile_conditional(&mut self, node: reifydb_rql::plan::physical::ConditionalNode) -> crate::Result<()> {
		// Collect all jump-to-end patches
		let mut end_patches: Vec<usize> = Vec::new();

		// IF cond THEN body
		let false_jump = self.emit_conditional_jump(node.condition);
		self.emit(Instruction::EnterScope(ScopeType::Conditional));
		self.scope_depth += 1;
		self.compile_plan(*node.then_branch)?;
		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		let end_jump = self.emit(Instruction::Jump(0)); // backpatched
		end_patches.push(end_jump);

		// Patch the false jump to point here (start of else-if chain or else or end)
		let else_if_start = self.current_addr();
		self.patch_jump_if_false_pop(false_jump, else_if_start);

		// ELSE IF branches
		for else_if in node.else_ifs {
			let false_jump = self.emit_conditional_jump(else_if.condition);
			self.emit(Instruction::EnterScope(ScopeType::Conditional));
			self.scope_depth += 1;
			self.compile_plan(*else_if.then_branch)?;
			self.scope_depth -= 1;
			self.emit(Instruction::ExitScope);
			let end_jump = self.emit(Instruction::Jump(0)); // backpatched
			end_patches.push(end_jump);

			let next_start = self.current_addr();
			self.patch_jump_if_false_pop(false_jump, next_start);
		}

		// ELSE branch
		if let Some(else_branch) = node.else_branch {
			self.emit(Instruction::EnterScope(ScopeType::Conditional));
			self.scope_depth += 1;
			self.compile_plan(*else_branch)?;
			self.scope_depth -= 1;
			self.emit(Instruction::ExitScope);
		}

		// Patch all end jumps
		let end_addr = self.current_addr();
		for patch_idx in end_patches {
			self.patch_jump(patch_idx, end_addr);
		}

		self.emit(Instruction::Nop); // end marker
		Ok(())
	}

	fn compile_loop(&mut self, node: reifydb_rql::plan::physical::LoopPhysicalNode) -> crate::Result<()> {
		let loop_start = self.current_addr();

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: loop_start,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(loop_start));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop); // loop_end target

		// Backpatch breaks
		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	fn compile_while(&mut self, node: reifydb_rql::plan::physical::WhilePhysicalNode) -> crate::Result<()> {
		let condition_addr = self.current_addr();
		let false_jump = self.emit_conditional_jump(node.condition);

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: condition_addr,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(condition_addr));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop);

		self.patch_jump_if_false_pop(false_jump, loop_end);

		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	fn compile_for(&mut self, node: reifydb_rql::plan::physical::ForPhysicalNode) -> crate::Result<()> {
		self.emit(Instruction::Query(*node.iterable));
		self.emit(Instruction::ForInit {
			variable_name: node.variable_name.clone(),
		});

		let for_next_addr = self.current_addr();
		let for_next_idx = self.emit(Instruction::ForNext {
			variable_name: node.variable_name,
			addr: 0, // backpatched to loop_end
		});

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: for_next_addr,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(for_next_addr));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop);

		// Patch ForNext to jump to loop_end when exhausted
		self.patch_for_next(for_next_idx, loop_end);

		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	fn compile_break(&mut self) -> crate::Result<()> {
		let loop_ctx = self
			.loop_stack
			.last_mut()
			.ok_or_else(|| reifydb_type::error!(runtime::break_outside_loop()))?;
		let exit_scopes = self.scope_depth - loop_ctx.scope_depth;
		let idx = self.emit(Instruction::Break {
			exit_scopes,
			addr: 0, // backpatched
		});
		// We need to reborrow since emit takes &mut self
		self.loop_stack.last_mut().unwrap().break_patches.push(idx);
		Ok(())
	}

	fn compile_continue(&mut self) -> crate::Result<()> {
		let loop_ctx =
			self.loop_stack.last().ok_or_else(|| reifydb_type::error!(runtime::continue_outside_loop()))?;
		let exit_scopes = self.scope_depth - loop_ctx.scope_depth;
		let continue_addr = loop_ctx.continue_addr;
		self.emit(Instruction::Continue {
			exit_scopes,
			addr: continue_addr,
		});
		Ok(())
	}

	fn patch_jump(&mut self, idx: usize, addr: Addr) {
		if let Instruction::Jump(ref mut target) = self.instructions[idx] {
			*target = addr;
		}
	}

	fn patch_jump_if_false_pop(&mut self, idx: usize, addr: Addr) {
		if let Instruction::JumpIfFalsePop(ref mut target) = self.instructions[idx] {
			*target = addr;
		}
	}

	fn patch_break_or_continue(&mut self, idx: usize, addr: Addr) {
		match &mut self.instructions[idx] {
			Instruction::Break {
				addr: target,
				..
			} => {
				*target = addr;
			}
			Instruction::Continue {
				addr: target,
				..
			} => {
				*target = addr;
			}
			_ => {}
		}
	}

	fn patch_for_next(&mut self, idx: usize, addr: Addr) {
		if let Instruction::ForNext {
			addr: ref mut target,
			..
		} = self.instructions[idx]
		{
			*target = addr;
		}
	}
}

pub fn compile(plans: Vec<PhysicalPlan>) -> crate::Result<Vec<Instruction>> {
	let mut compiler = Compiler::new();
	for plan in plans {
		compiler.compile_plan(plan)?;
	}
	compiler.emit(Instruction::Halt);
	Ok(compiler.instructions)
}
