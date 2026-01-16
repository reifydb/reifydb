// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Plan compiler - transforms Plan into bytecode.

pub mod control;
pub mod ddl;
pub mod dml;
pub mod expr;
pub mod query;

use std::collections::HashMap;

use crate::{
	bytecode::{
		instruction::BytecodeWriter,
		opcode::Opcode,
		program::{
			CompiledProgram, CompiledProgramBuilder, ScriptFunctionDef, SourceMap, SourceMapEntry,
			SubqueryDef,
		},
	},
	error::RqlError,
	plan::Plan,
	token::span::Span,
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
	pub(crate) builder: CompiledProgramBuilder,
	pub(crate) writer: BytecodeWriter,
	/// Source map entries being built.
	pub(crate) source_map_entries: Vec<SourceMapEntry>,
	/// Stack of loop contexts for nested loops.
	pub(crate) loop_contexts: Vec<LoopContext>,
	/// Script function name to index mapping.
	pub(crate) script_function_indices: HashMap<String, u16>,
	/// Pending script function bytecode (name, bytecode).
	pub(crate) pending_script_functions: Vec<(String, Vec<u8>)>,
	/// Counter for internal variable IDs.
	pub(crate) next_internal_var_id: u16,
}

impl PlanCompiler {
	/// Create a new plan compiler.
	pub fn new() -> Self {
		Self {
			builder: CompiledProgramBuilder::new(),
			writer: BytecodeWriter::new(),
			source_map_entries: Vec::new(),
			loop_contexts: Vec::new(),
			script_function_indices: HashMap::new(),
			pending_script_functions: Vec::new(),
			next_internal_var_id: 0,
		}
	}

	/// Allocate a new internal variable ID.
	pub(crate) fn alloc_internal_var(&mut self) -> u16 {
		let id = self.next_internal_var_id;
		self.next_internal_var_id += 1;
		id
	}

	/// Compile multiple plans into a single program.
	///
	/// This is used for multi-statement programs where each statement becomes a separate plan
	/// (e.g., let bindings, multiple queries). All plans are compiled sequentially into the
	/// same bytecode program, allowing variable declarations in earlier plans to be referenced
	/// in later plans.
	pub fn compile<'bump>(plans: &[Plan<'bump>]) -> std::result::Result<CompiledProgram, RqlError> {
		let mut compiler = Self::new();
		for plan in plans {
			compiler.compile_plan(plan).map_err(RqlError::Compile)?;
		}
		compiler.writer.emit_opcode(Opcode::Halt);
		compiler.finalize_script_functions();
		*compiler.builder.bytecode_mut() = compiler.writer.finish();
		*compiler.builder.source_map_mut() = SourceMap::from_entries(compiler.source_map_entries);
		Ok(compiler.builder.build())
	}

	/// Record a span at the current bytecode position.
	pub(crate) fn record_span(&mut self, span: Span) {
		self.source_map_entries.push(SourceMapEntry {
			bytecode_offset: self.writer.position() as u32,
			span,
		});
	}

	/// Compile a subquery plan into a SubqueryDef.
	///
	/// This creates a separate bytecode stream for the subquery that can be
	/// executed independently. Used for EXISTS, IN subqueries, and scalar subqueries.
	pub(crate) fn compile_subquery<'bump>(&mut self, plan: &Plan<'bump>) -> Result<u16> {
		// Save current state
		let saved_writer = std::mem::replace(&mut self.writer, BytecodeWriter::new());
		let saved_source_map = std::mem::take(&mut self.source_map_entries);
		let saved_builder = std::mem::replace(&mut self.builder, CompiledProgramBuilder::new());

		// Compile the subquery plan into the new writer
		self.compile_plan(plan)?;
		self.writer.emit_opcode(Opcode::Collect);
		self.writer.emit_opcode(Opcode::Halt);

		// Extract the compiled subquery
		let subquery_bytecode = self.writer.to_vec();
		let subquery_constants = std::mem::take(&mut self.builder.constants);
		let subquery_sources = std::mem::take(&mut self.builder.sources);
		let subquery_source_map = SourceMap::from_entries(std::mem::take(&mut self.source_map_entries));

		// Restore state
		self.writer = saved_writer;
		self.source_map_entries = saved_source_map;
		self.builder = saved_builder;

		// Create the subquery definition
		let subquery_def = SubqueryDef {
			bytecode: subquery_bytecode,
			constants: subquery_constants,
			sources: subquery_sources,
			outer_refs: Vec::new(), // TODO: Track outer references for correlated subqueries
			source_map: subquery_source_map,
		};

		// Add to builder and return index
		Ok(self.builder.add_subquery(subquery_def))
	}

	/// Finalize script functions by appending their bytecode to the main program.
	fn finalize_script_functions(&mut self) {
		for (name, bytecode) in self.pending_script_functions.drain(..) {
			let offset = self.writer.position();
			let len = bytecode.len();
			self.writer.append(&bytecode);
			self.builder.add_script_function(ScriptFunctionDef {
				name,
				bytecode_offset: offset,
				bytecode_len: len,
				parameter_count: 0,
			});
		}
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
			Plan::DefineScriptFunction(node) => self.compile_define_script_function(node),
			Plan::CallScriptFunction(node) => self.compile_call_script_function(node),
			Plan::Expr(node) => self.compile_expr_node(node),

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
