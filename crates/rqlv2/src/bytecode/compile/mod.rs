// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Plan compiler - transforms Plan into bytecode.

mod control;
mod ddl;
mod dml;
mod expr;
mod query;

use crate::{
	bytecode::{
		instruction::BytecodeWriter,
		opcode::Opcode,
		program::{CompiledProgram, SourceMap, SourceMapEntry},
	},
	plan::Plan,
	token::Span,
};

/// Error type for compilation.
#[derive(Debug, Clone)]
pub enum CompileError {
	/// Unsupported plan node.
	UnsupportedPlan {
		message: String,
		span: Span,
	},
	/// Unsupported expression.
	UnsupportedExpr {
		message: String,
		span: Span,
	},
	/// Internal compiler error.
	Internal {
		message: String,
	},
}

impl std::fmt::Display for CompileError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			CompileError::UnsupportedPlan {
				message,
				..
			} => {
				write!(f, "unsupported plan: {}", message)
			}
			CompileError::UnsupportedExpr {
				message,
				..
			} => {
				write!(f, "unsupported expression: {}", message)
			}
			CompileError::Internal {
				message,
			} => write!(f, "internal error: {}", message),
		}
	}
}

impl std::error::Error for CompileError {}

/// Result type for compilation.
pub type Result<T> = std::result::Result<T, CompileError>;

/// Context for tracking loop break/continue targets.
pub(crate) struct LoopContext {
	/// Position of loop start (for continue).
	pub continue_target: usize,
	/// Positions of break jumps that need patching.
	pub break_patches: Vec<usize>,
}

/// Plan compiler that transforms a Plan into bytecode.
pub struct PlanCompiler {
	pub(crate) program: CompiledProgram,
	pub(crate) writer: BytecodeWriter,
	/// Source map entries being built.
	pub(crate) source_map_entries: Vec<SourceMapEntry>,
	/// Stack of loop contexts for nested loops.
	pub(crate) loop_contexts: Vec<LoopContext>,
}

impl PlanCompiler {
	/// Create a new plan compiler.
	pub fn new() -> Self {
		Self {
			program: CompiledProgram::new(),
			writer: BytecodeWriter::new(),
			source_map_entries: Vec::new(),
			loop_contexts: Vec::new(),
		}
	}

	/// Compile a plan into a program.
	pub fn compile<'bump>(plan: &Plan<'bump>) -> Result<CompiledProgram> {
		let mut compiler = Self::new();
		compiler.compile_plan(plan)?;
		compiler.writer.emit_opcode(Opcode::Halt);
		compiler.program.bytecode = compiler.writer.finish();
		compiler.program.source_map = SourceMap::from_entries(compiler.source_map_entries);
		Ok(compiler.program)
	}

	/// Compile multiple plans into a single program.
	///
	/// This is used for multi-statement programs where each statement becomes a separate plan
	/// (e.g., let bindings, multiple queries). All plans are compiled sequentially into the
	/// same bytecode program, allowing variable declarations in earlier plans to be referenced
	/// in later plans.
	pub fn compile_all<'bump>(plans: &[Plan<'bump>]) -> Result<CompiledProgram> {
		let mut compiler = Self::new();
		for plan in plans {
			compiler.compile_plan(plan)?;
		}
		compiler.writer.emit_opcode(Opcode::Halt);
		compiler.program.bytecode = compiler.writer.finish();
		compiler.program.source_map = SourceMap::from_entries(compiler.source_map_entries);
		Ok(compiler.program)
	}

	/// Record a span at the current bytecode position.
	pub(crate) fn record_span(&mut self, span: Span) {
		self.source_map_entries.push(SourceMapEntry {
			bytecode_offset: self.writer.position() as u32,
			span,
		});
	}

	/// Compile a plan node.
	pub(crate) fn compile_plan<'bump>(&mut self, plan: &Plan<'bump>) -> Result<()> {
		match plan {
			// Query Operations
			Plan::Scan(node) => self.compile_scan(node),
			Plan::IndexScan(node) => self.compile_index_scan(node),
			Plan::Filter(node) => self.compile_filter(node),
			Plan::Project(node) => self.compile_project(node),
			Plan::Extend(node) => self.compile_extend(node),
			Plan::Aggregate(node) => self.compile_aggregate(node),
			Plan::Sort(node) => self.compile_sort(node),
			Plan::Take(node) => self.compile_take(node),
			Plan::Distinct(node) => self.compile_distinct(node),
			Plan::JoinInner(node) => self.compile_join_inner(node),
			Plan::JoinLeft(node) => self.compile_join_left(node),
			Plan::JoinNatural(node) => self.compile_join_natural(node),
			Plan::Merge(node) => self.compile_merge(node),
			Plan::Window(node) => self.compile_window(node),
			Plan::Apply(node) => self.compile_apply(node),

			// Optimized Row Access
			Plan::RowPointLookup(node) => self.compile_row_point_lookup(node),
			Plan::RowListLookup(node) => self.compile_row_list_lookup(node),
			Plan::RowRangeScan(node) => self.compile_row_range_scan(node),

			// DML Operations
			Plan::Insert(node) => self.compile_insert(node),
			Plan::Update(node) => self.compile_update(node),
			Plan::Delete(node) => self.compile_delete(node),

			// DDL Operations
			Plan::Create(node) => self.compile_create(node),
			Plan::Alter(node) => self.compile_alter(node),
			Plan::Drop(node) => self.compile_drop(node),

			// Control Flow
			Plan::Conditional(node) => self.compile_conditional(node),
			Plan::Loop(node) => self.compile_loop(node),
			Plan::For(node) => self.compile_for(node),
			Plan::Declare(node) => self.compile_declare(node),
			Plan::Assign(node) => self.compile_assign(node),
			Plan::Return(node) => self.compile_return(node),
			Plan::Break(node) => self.compile_break(node),
			Plan::Continue(node) => self.compile_continue(node),

			// Other
			Plan::InlineData(node) => self.compile_inline_data(node),
			Plan::Generator(node) => self.compile_generator(node),
			Plan::VariableSource(node) => self.compile_variable_source(node),
			Plan::Environment(node) => self.compile_environment(node),
			Plan::Scalarize(node) => self.compile_scalarize(node),
		}
	}
}

impl Default for PlanCompiler {
	fn default() -> Self {
		Self::new()
	}
}
