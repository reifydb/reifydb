// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{fmt::Debug, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::util::lru::LruCache;
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{Result, error::diagnostic::runtime};

use crate::{
	ast::{ast::AstStatement, parse_str},
	instruction::{Addr, CompiledFunctionDef, Instruction, ScopeType},
	nodes::{ConditionalNode, ForPhysicalNode, LoopPhysicalNode, PhysicalPlan, WhilePhysicalNode},
	plan::plan,
	query::QueryPlan,
};

const DEFAULT_CAPACITY: usize = 1024 * 8;

#[derive(Debug, Clone)]
pub struct Compiled {
	pub instructions: Vec<Instruction>,
	pub is_output: bool,
}

/// Result of compiling a query.
pub enum CompilationResult {
	Ready(Arc<Vec<Compiled>>),
	Incremental(IncrementalCompilation),
}

/// Opaque state for incremental compilation.
pub struct IncrementalCompilation {
	statements: Vec<AstStatement>,
	current: usize,
}

#[derive(Debug, Clone)]
pub struct Compiler(Arc<CompilerInner>);

struct CompilerInner {
	catalog: Catalog,
	cache: LruCache<Hash128, Arc<Vec<Compiled>>>,
}

impl Debug for CompilerInner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompilerInner")
			.field("catalog", &self.catalog)
			.field("cache_len", &self.cache.len())
			.field("cache_capacity", &self.cache.capacity())
			.finish()
	}
}

impl Compiler {
	pub fn new(catalog: Catalog) -> Self {
		Self(Arc::new(CompilerInner {
			catalog,
			cache: LruCache::new(DEFAULT_CAPACITY),
		}))
	}

	pub fn compile<T: AsTransaction>(&self, tx: &mut T, query: &str) -> Result<CompilationResult> {
		let hash = xxh3_128(query.as_bytes());

		if let Some(cached) = self.0.cache.get(&hash) {
			return Ok(CompilationResult::Ready(cached));
		}

		let statements = parse_str(query)?;
		let has_ddl = statements.iter().any(|s| s.contains_ddl());
		let needs_incremental = statements.len() > 1 && has_ddl;

		if needs_incremental {
			return Ok(CompilationResult::Incremental(IncrementalCompilation {
				statements,
				current: 0,
			}));
		}

		// Batch compile
		let mut plans = Vec::new();
		for statement in statements {
			let is_output = statement.is_output;
			if let Some(physical) = plan(&self.0.catalog, tx, statement)? {
				plans.push(Compiled {
					instructions: compile_instructions(vec![physical])?,
					is_output,
				});
			}
		}

		let arc_plans = Arc::new(plans);
		if !has_ddl {
			self.0.cache.put(hash, arc_plans.clone());
		}
		Ok(CompilationResult::Ready(arc_plans))
	}

	/// Compile the next statement in an incremental compilation.
	/// Returns `None` when all statements have been compiled.
	pub fn compile_next<T: AsTransaction>(
		&self,
		tx: &mut T,
		state: &mut IncrementalCompilation,
	) -> Result<Option<Compiled>> {
		if state.current >= state.statements.len() {
			return Ok(None);
		}

		let statement = state.statements[state.current].clone();
		state.current += 1;

		let is_output = statement.is_output;
		if let Some(physical) = plan(&self.0.catalog, tx, statement)? {
			Ok(Some(Compiled {
				instructions: compile_instructions(vec![physical])?,
				is_output,
			}))
		} else {
			self.compile_next(tx, state)
		}
	}

	/// Clear all cached plans.
	pub fn clear(&self) {
		self.0.cache.clear();
	}

	/// Return the number of cached plans.
	pub fn len(&self) -> usize {
		self.0.cache.len()
	}

	/// Return true if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.0.cache.is_empty()
	}

	/// Return the cache capacity.
	pub fn capacity(&self) -> usize {
		self.0.cache.capacity()
	}
}

fn compile_instructions(plans: Vec<PhysicalPlan>) -> crate::Result<Vec<Instruction>> {
	let mut compiler = InstructionCompiler::new();
	for plan in plans {
		compiler.compile_plan(plan)?;
	}
	compiler.emit(Instruction::Halt);
	Ok(compiler.instructions)
}

// ============================================================================
// Instruction Compilation
// ============================================================================

/// Context for tracking loop information during compilation
struct LoopContext {
	/// Address of the condition check / ForNext (target for Continue)
	continue_addr: Addr,
	/// Placeholder indices in `instructions` where the loop-end address must be backpatched
	break_patches: Vec<usize>,
	/// Scope depth at the point the loop was entered (used to compute exit_scopes)
	scope_depth: usize,
}

/// Instruction compiler that transforms PhysicalPlan to Instructions
struct InstructionCompiler {
	instructions: Vec<Instruction>,
	loop_stack: Vec<LoopContext>,
	scope_depth: usize,
}

impl InstructionCompiler {
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
	fn emit_conditional_jump(&mut self, condition: crate::expression::Expression) -> usize {
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

			// User-defined functions
			PhysicalPlan::DefineFunction(node) => {
				// Pre-compile the function body to instructions
				let body_instructions = compile_instructions(node.body)?;
				let compiled_func = CompiledFunctionDef {
					name: node.name,
					parameters: node.parameters,
					return_type: node.return_type,
					body: body_instructions,
				};
				self.emit(Instruction::DefineFunction(compiled_func));
			}
			PhysicalPlan::CallFunction(node) => {
				self.emit(Instruction::CallFunction(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Return(node) => {
				self.emit(Instruction::Return(node));
			}

			// Query operations - convert to QueryPlan
			PhysicalPlan::TableScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::TableVirtualScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableVirtualScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::ViewScan(node) => {
				self.emit(Instruction::Query(QueryPlan::ViewScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RingBufferScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RingBufferScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::FlowScan(node) => {
				self.emit(Instruction::Query(QueryPlan::FlowScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DictionaryScan(node) => {
				self.emit(Instruction::Query(QueryPlan::DictionaryScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::IndexScan(node) => {
				self.emit(Instruction::Query(QueryPlan::IndexScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowPointLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowPointLookup(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowListLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowListLookup(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowRangeScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RowRangeScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Aggregate(node) => {
				self.emit(Instruction::Query(QueryPlan::Aggregate(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Distinct(node) => {
				self.emit(Instruction::Query(QueryPlan::Distinct(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Filter(node) => {
				self.emit(Instruction::Query(QueryPlan::Filter(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinInner(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinInner(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinLeft(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinLeft(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinNatural(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinNatural(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Merge(node) => {
				self.emit(Instruction::Query(QueryPlan::Merge(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Take(node) => {
				self.emit(Instruction::Query(QueryPlan::Take(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Sort(node) => {
				self.emit(Instruction::Query(QueryPlan::Sort(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Map(node) => {
				self.emit(Instruction::Query(QueryPlan::Map(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Extend(node) => {
				self.emit(Instruction::Query(QueryPlan::Extend(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Patch(node) => {
				self.emit(Instruction::Query(QueryPlan::Patch(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Apply(node) => {
				self.emit(Instruction::Query(QueryPlan::Apply(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InlineData(node) => {
				self.emit(Instruction::Query(QueryPlan::InlineData(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Generator(node) => {
				self.emit(Instruction::Query(QueryPlan::Generator(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Window(node) => {
				self.emit(Instruction::Query(QueryPlan::Window(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Variable(node) => {
				self.emit(Instruction::Query(QueryPlan::Variable(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Environment(node) => {
				self.emit(Instruction::Query(QueryPlan::Environment(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Scalarize(node) => {
				self.emit(Instruction::Query(QueryPlan::Scalarize(node)));
				self.emit(Instruction::Emit);
			}
		}
		Ok(())
	}

	fn compile_conditional(&mut self, node: ConditionalNode) -> crate::Result<()> {
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

	fn compile_loop(&mut self, node: LoopPhysicalNode) -> crate::Result<()> {
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

	fn compile_while(&mut self, node: WhilePhysicalNode) -> crate::Result<()> {
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

	fn compile_for(&mut self, node: ForPhysicalNode) -> crate::Result<()> {
		// Compile the iterable and emit it as a query
		self.compile_plan_for_iterable(*node.iterable)?;
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

	/// Compile a plan that will be used as an iterable (for FOR loops).
	/// This is similar to compile_plan but doesn't emit Emit at the end.
	fn compile_plan_for_iterable(&mut self, plan: PhysicalPlan) -> crate::Result<()> {
		match plan {
			// Query operations - convert to QueryPlan (no Emit since ForInit will use it)
			PhysicalPlan::TableScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableScan(node)));
			}
			PhysicalPlan::TableVirtualScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableVirtualScan(node)));
			}
			PhysicalPlan::ViewScan(node) => {
				self.emit(Instruction::Query(QueryPlan::ViewScan(node)));
			}
			PhysicalPlan::RingBufferScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RingBufferScan(node)));
			}
			PhysicalPlan::FlowScan(node) => {
				self.emit(Instruction::Query(QueryPlan::FlowScan(node)));
			}
			PhysicalPlan::DictionaryScan(node) => {
				self.emit(Instruction::Query(QueryPlan::DictionaryScan(node)));
			}
			PhysicalPlan::IndexScan(node) => {
				self.emit(Instruction::Query(QueryPlan::IndexScan(node)));
			}
			PhysicalPlan::RowPointLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowPointLookup(node)));
			}
			PhysicalPlan::RowListLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowListLookup(node)));
			}
			PhysicalPlan::RowRangeScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RowRangeScan(node)));
			}
			PhysicalPlan::Aggregate(node) => {
				self.emit(Instruction::Query(QueryPlan::Aggregate(node)));
			}
			PhysicalPlan::Distinct(node) => {
				self.emit(Instruction::Query(QueryPlan::Distinct(node)));
			}
			PhysicalPlan::Filter(node) => {
				self.emit(Instruction::Query(QueryPlan::Filter(node)));
			}
			PhysicalPlan::JoinInner(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinInner(node)));
			}
			PhysicalPlan::JoinLeft(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinLeft(node)));
			}
			PhysicalPlan::JoinNatural(node) => {
				self.emit(Instruction::Query(QueryPlan::JoinNatural(node)));
			}
			PhysicalPlan::Merge(node) => {
				self.emit(Instruction::Query(QueryPlan::Merge(node)));
			}
			PhysicalPlan::Take(node) => {
				self.emit(Instruction::Query(QueryPlan::Take(node)));
			}
			PhysicalPlan::Sort(node) => {
				self.emit(Instruction::Query(QueryPlan::Sort(node)));
			}
			PhysicalPlan::Map(node) => {
				self.emit(Instruction::Query(QueryPlan::Map(node)));
			}
			PhysicalPlan::Extend(node) => {
				self.emit(Instruction::Query(QueryPlan::Extend(node)));
			}
			PhysicalPlan::Patch(node) => {
				self.emit(Instruction::Query(QueryPlan::Patch(node)));
			}
			PhysicalPlan::Apply(node) => {
				self.emit(Instruction::Query(QueryPlan::Apply(node)));
			}
			PhysicalPlan::InlineData(node) => {
				self.emit(Instruction::Query(QueryPlan::InlineData(node)));
			}
			PhysicalPlan::Generator(node) => {
				self.emit(Instruction::Query(QueryPlan::Generator(node)));
			}
			PhysicalPlan::Window(node) => {
				self.emit(Instruction::Query(QueryPlan::Window(node)));
			}
			PhysicalPlan::Variable(node) => {
				self.emit(Instruction::Query(QueryPlan::Variable(node)));
			}
			PhysicalPlan::Environment(node) => {
				self.emit(Instruction::Query(QueryPlan::Environment(node)));
			}
			PhysicalPlan::Scalarize(node) => {
				self.emit(Instruction::Query(QueryPlan::Scalarize(node)));
			}
			// Anything else should go through compile_plan
			other => {
				self.compile_plan(other)?;
			}
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
