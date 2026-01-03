// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
//! Bytecode compiler - transforms AST into a Program.
use std::collections::HashMap;

use reifydb_type::{OrderedF64, Value};

use crate::{
	bytecode::{
		self,
		instruction::BytecodeWriter,
		opcode::{Opcode, OperatorKind},
		program::{FunctionDef, ParameterDef, Program, SourceDef},
	},
	dsl::{self, ast::*},
	error::{Result, VmError},
	expr::{self, BinaryOp, ColumnRef, Expr, Literal, UnaryOp, compile_expr, compile_filter},
	operator::sort::{SortOrder, SortSpec},
};
/// Context for tracking loop break/continue targets.
struct LoopContext {
	/// Position of loop start (for continue)
	continue_target: usize,
	/// Positions of break jumps that need patching
	break_patches: Vec<usize>,
}

/// Collect all column references from an expression AST.
fn collect_column_refs(expr: &ExprAst, refs: &mut Vec<String>) {
	match expr {
		ExprAst::Column {
			name,
			..
		} => {
			if !refs.contains(name) {
				refs.push(name.clone());
			}
		}
		ExprAst::BinaryOp {
			left,
			right,
			..
		} => {
			collect_column_refs(left, refs);
			collect_column_refs(right, refs);
		}
		ExprAst::UnaryOp {
			operand,
			..
		} => {
			collect_column_refs(operand, refs);
		}
		ExprAst::Paren {
			inner,
			..
		} => {
			collect_column_refs(inner, refs);
		}
		ExprAst::Subquery {
			pipeline,
			..
		} => {
			// Don't look inside subqueries - they have their own scope
			let _ = pipeline;
		}
		ExprAst::InSubquery {
			expr,
			..
		} => {
			collect_column_refs(expr, refs);
		}
		ExprAst::InList {
			expr,
			values,
			..
		} => {
			collect_column_refs(expr, refs);
			for v in values {
				collect_column_refs(v, refs);
			}
		}
		ExprAst::FieldAccess {
			object,
			..
		} => {
			collect_column_refs(object, refs);
		}
		// Literals and variables don't reference columns
		_ => {}
	}
}
/// Bytecode compiler that transforms a DSL AST into a Program.
pub struct BytecodeCompiler {
	program: Program,
	writer: BytecodeWriter,
	/// Map from function name to function index (for forward references)
	functions: HashMap<String, u16>,
	/// Stack of loop contexts for nested loops
	loop_contexts: Vec<LoopContext>,
}
impl BytecodeCompiler {
	/// Create a new bytecode compiler.
	pub fn new() -> Self {
		Self {
			program: Program::new(),
			writer: BytecodeWriter::new(),
			functions: HashMap::new(),
			loop_contexts: Vec::new(),
		}
	}
	/// Compile a DSL AST into a Program.
	pub fn compile(mut self, ast: DslAst) -> Result<Program> {
		// First pass: register all function definitions
		for stmt in &ast.statements {
			if let StatementAst::Def(def) = stmt {
				self.register_function(def)?;
			}
		}
		// Second pass: compile all statements
		for stmt in ast.statements {
			self.compile_statement(stmt)?;
		}
		// Emit halt at end
		self.writer.emit_opcode(Opcode::Halt);
		self.program.bytecode = self.writer.finish();
		Ok(self.program)
	}
	/// Register a function for forward references.
	fn register_function(&mut self, def: &DefAst) -> Result<()> {
		if self.functions.contains_key(&def.name) {
			return Err(VmError::DuplicateFunction {
				name: def.name.clone(),
			});
		}
		let func_def = FunctionDef {
			name: def.name.clone(),
			parameters: def
				.parameters
				.iter()
				.map(|p| ParameterDef {
					name: p.name.clone(),
					param_type: p.param_type.clone(),
				})
				.collect(),
			bytecode_offset: 0, // Will be patched
			bytecode_len: 0,
		};
		let index = self.program.add_function(func_def);
		self.functions.insert(def.name.clone(), index);
		Ok(())
	}
	/// Compile a single statement.
	fn compile_statement(&mut self, stmt: StatementAst) -> Result<()> {
		match stmt {
			StatementAst::Pipeline(pipeline) => {
				self.compile_pipeline(pipeline)?;
			}
			StatementAst::Let(let_stmt) => {
				self.compile_let(let_stmt)?;
			}
			StatementAst::Def(def) => {
				self.compile_def(def)?;
			}
			StatementAst::If(if_stmt) => {
				self.compile_if(if_stmt)?;
			}
			StatementAst::Call(call) => {
				self.compile_call(call)?;
			}
			StatementAst::Loop(loop_stmt) => {
				self.compile_loop(loop_stmt)?;
			}
			StatementAst::Break(_) => {
				self.compile_break()?;
			}
			StatementAst::Continue(_) => {
				self.compile_continue()?;
			}
			StatementAst::For(for_stmt) => {
				self.compile_for(for_stmt)?;
			}
			StatementAst::ModuleCall(module_call) => {
				self.compile_module_call(module_call)?;
			}
			StatementAst::Assign(assign) => {
				self.compile_assign(assign)?;
			}
			StatementAst::Expression(expr_stmt) => {
				// Compile expression and leave value on operand stack for implicit return
				self.compile_expr_to_operand(expr_stmt.expr)?;
			}
		}
		Ok(())
	}
	/// Compile a let statement.
	fn compile_let(&mut self, let_stmt: LetAst) -> Result<()> {
		let name_index = self.program.add_constant(Value::Utf8(let_stmt.name));
		match let_stmt.value {
			LetValue::Expr(expr) => {
				// Compile expression and push result to operand stack
				self.compile_expr_to_operand(expr)?;
				// Store as scalar variable
				self.writer.emit_opcode(Opcode::StoreVar);
				self.writer.emit_u16(name_index);
			}
			LetValue::Pipeline(pipeline) => {
				// Compile the pipeline
				self.compile_pipeline(*pipeline)?;
				// Store as pipeline variable (using StorePipelineById)
				let var_id = self.program.next_var_id();
				self.writer.emit_opcode(Opcode::StorePipelineById);
				self.writer.emit_u32(var_id);
			}
		}
		Ok(())
	}
	/// Compile an assignment statement (updates existing variable).
	fn compile_assign(&mut self, assign: AssignAst) -> Result<()> {
		let name_index = self.program.add_constant(Value::Utf8(assign.name.clone()));
		// Compile expression and push result to operand stack
		self.compile_expr_to_operand(assign.value)?;
		// Update existing variable (use UpdateVarById)
		let var_id = self.program.next_var_id();
		self.writer.emit_opcode(Opcode::UpdateVarById);
		self.writer.emit_u32(var_id);
		Ok(())
	}
	/// Compile a function definition.
	fn compile_def(&mut self, def: DefAst) -> Result<()> {
		// Jump over function body (functions are called, not fallen into)
		self.writer.emit_opcode(Opcode::Jump);
		let jump_pos = self.writer.position();
		self.writer.emit_u16(0); // Placeholder
		// Record function offset
		let func_index = *self.functions.get(&def.name).unwrap();
		self.program.functions[func_index as usize].bytecode_offset = self.writer.position();
		// Enter function scope
		self.writer.emit_opcode(Opcode::EnterScope);
		// Bind parameters from operand stack
		// Arguments are pushed in order, so we pop them in reverse
		for param in def.parameters.iter().rev() {
			let name_index = self.program.add_constant(Value::Utf8(param.name.clone()));
			self.writer.emit_opcode(Opcode::StoreVar);
			self.writer.emit_u16(name_index);
		}
		// Compile function body
		for stmt in def.body {
			self.compile_statement(stmt)?;
		}
		// Exit function scope
		self.writer.emit_opcode(Opcode::ExitScope);
		// Emit implicit return
		self.writer.emit_opcode(Opcode::Return);
		// Record function length
		let func_end = self.writer.position();
		self.program.functions[func_index as usize].bytecode_len =
			func_end - self.program.functions[func_index as usize].bytecode_offset;
		// Patch jump
		self.writer.patch_jump(jump_pos);
		Ok(())
	}
	/// Compile an if statement.
	fn compile_if(&mut self, if_stmt: IfAst) -> Result<()> {
		// Compile condition and push result to operand stack
		self.compile_condition_expr(if_stmt.condition)?;
		// JumpIfNot to else branch (or end if no else)
		self.writer.emit_opcode(Opcode::JumpIfNot);
		let else_jump = self.writer.position();
		self.writer.emit_u16(0); // Placeholder
		// Compile then branch
		self.writer.emit_opcode(Opcode::EnterScope);
		for stmt in if_stmt.then_branch {
			self.compile_statement(stmt)?;
		}
		self.writer.emit_opcode(Opcode::ExitScope);
		if let Some(else_stmts) = if_stmt.else_branch {
			// Jump over else branch
			self.writer.emit_opcode(Opcode::Jump);
			let end_jump = self.writer.position();
			self.writer.emit_u16(0); // Placeholder
			// Patch else jump to here
			self.writer.patch_jump(else_jump);
			// Compile else branch
			self.writer.emit_opcode(Opcode::EnterScope);
			for stmt in else_stmts {
				self.compile_statement(stmt)?;
			}
			self.writer.emit_opcode(Opcode::ExitScope);
			// Patch end jump
			self.writer.patch_jump(end_jump);
		} else {
			// Patch else jump to end
			self.writer.patch_jump(else_jump);
		}
		Ok(())
	}
	/// Compile a function call.
	fn compile_call(&mut self, call: CallAst) -> Result<()> {
		// Find function
		let func_index =
			*self.functions.get(&call.function_name).ok_or_else(|| VmError::UndefinedFunction {
				name: call.function_name.clone(),
			})?;
		let func_def = &self.program.functions[func_index as usize];
		let expected_args = func_def.parameters.len();
		let got_args = call.arguments.len();
		if got_args != expected_args {
			return Err(VmError::WrongArgumentCount {
				name: call.function_name.clone(),
				expected: expected_args,
				got: got_args,
			});
		}
		// Push arguments onto operand stack
		for arg in call.arguments {
			self.compile_expr_to_operand(arg)?;
		}
		// Call function
		self.writer.emit_opcode(Opcode::Call);
		self.writer.emit_u16(func_index);
		Ok(())
	}
	/// Compile a loop statement.
	fn compile_loop(&mut self, loop_stmt: LoopAst) -> Result<()> {
		// Record loop start position (for continue)
		let loop_start = self.writer.position();
		// Push loop context
		self.loop_contexts.push(LoopContext {
			continue_target: loop_start,
			break_patches: Vec::new(),
		});
		// Enter scope
		self.writer.emit_opcode(Opcode::EnterScope);
		// Compile body
		for stmt in loop_stmt.body {
			self.compile_statement(stmt)?;
		}
		// Exit scope
		self.writer.emit_opcode(Opcode::ExitScope);
		// Jump back to loop start
		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		// Calculate relative offset (negative, jumping backward)
		let offset = (loop_start as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);
		// Patch all break jumps to point here
		let loop_end = self.writer.position();
		let context = self.loop_contexts.pop().expect("loop context");
		for break_pos in context.break_patches {
			self.writer.patch_jump_at(break_pos, loop_end);
		}
		Ok(())
	}
	/// Compile a break statement.
	fn compile_break(&mut self) -> Result<()> {
		// Get current loop context
		let context = self.loop_contexts.last_mut().ok_or_else(|| VmError::CompileError {
			message: "break outside of loop".to_string(),
		})?;
		// Emit jump with placeholder offset (will be patched later)
		self.writer.emit_opcode(Opcode::Jump);
		let jump_pos = self.writer.position();
		self.writer.emit_i16(0); // Placeholder
		// Record position for patching
		context.break_patches.push(jump_pos);
		Ok(())
	}
	/// Compile a continue statement.
	fn compile_continue(&mut self) -> Result<()> {
		// Get current loop context
		let context = self.loop_contexts.last().ok_or_else(|| VmError::CompileError {
			message: "continue outside of loop".to_string(),
		})?;
		// Jump back to loop start
		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		let offset = (context.continue_target as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);
		Ok(())
	}
	/// Compile a for loop.
	///
	/// Generates bytecode for:
	/// ```text
	/// for $var in <iterable> { body }
	/// ```
	///
	/// Bytecode structure:
	/// 1. Compile iterable and collect to Frame
	/// 2. Store Frame in __for_frame
	/// 3. Get frame length, store in __for_len
	/// 4. Initialize __for_idx = 0
	/// 5. loop_start: a. Load __for_idx and __for_len b. Compare __for_idx < __for_len (IntLt) c. JumpIfNot to
	///    loop_end d. EnterScope e. Load Frame, load __for_idx, FrameRow to get Record f. Store Record in $var g.
	///    Execute body h. ExitScope i. Load __for_idx, push 1, IntAdd, store back j. Jump to loop_start
	/// 6. loop_end:
	fn compile_for(&mut self, for_stmt: ForAst) -> Result<()> {
		// 1. Compile the iterable (pipeline) and collect to Frame
		self.compile_statement(*for_stmt.iterable)?;
		self.writer.emit_opcode(Opcode::Collect);
		// 2. Store Frame in __for_frame
		let frame_var = self.program.add_constant(Value::Utf8("__for_frame".to_string()));
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u16(frame_var);
		// 3. Get frame length and store in __for_len
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(frame_var);
		self.writer.emit_opcode(Opcode::FrameLen);
		let len_var = self.program.add_constant(Value::Utf8("__for_len".to_string()));
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u16(len_var);
		// 4. Initialize __for_idx = 0
		let idx_var = self.program.add_constant(Value::Utf8("__for_idx".to_string()));
		let zero_const = self.program.add_constant(Value::Int8(0));
		self.writer.emit_opcode(Opcode::PushConst);
		self.writer.emit_u16(zero_const);
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u16(idx_var);
		// 5. loop_start
		let loop_start = self.writer.position();
		// Push loop context for break/continue
		self.loop_contexts.push(LoopContext {
			continue_target: loop_start,
			break_patches: Vec::new(),
		});
		// 5a-b. Load __for_idx and __for_len, compare with IntLt
		// Stack order for IntLt: pops (b, a), computes a < b
		// We want: __for_idx < __for_len
		// So push __for_idx first (will be 'a'), then __for_len (will be 'b')
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(idx_var);
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(len_var);
		self.writer.emit_opcode(Opcode::IntLt);
		// 5c. JumpIfNot to loop_end
		self.writer.emit_opcode(Opcode::JumpIfNot);
		let end_jump = self.writer.position();
		self.writer.emit_u16(0); // Placeholder
		// 5d. EnterScope
		self.writer.emit_opcode(Opcode::EnterScope);
		// 5e. Load Frame, load __for_idx, FrameRow to get Record
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(frame_var);
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(idx_var);
		self.writer.emit_opcode(Opcode::FrameRow);
		// 5f. Store Record in $var
		let var_name_idx = self.program.add_constant(Value::Utf8(for_stmt.variable.clone()));
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u16(var_name_idx);
		// 5g. Execute body
		for stmt in for_stmt.body {
			self.compile_statement(stmt)?;
		}
		// 5h. ExitScope
		self.writer.emit_opcode(Opcode::ExitScope);
		// 5i. Increment __for_idx: load, push 1, add, store
		self.writer.emit_opcode(Opcode::LoadVar);
		self.writer.emit_u16(idx_var);
		let one_const = self.program.add_constant(Value::Int8(1));
		self.writer.emit_opcode(Opcode::PushConst);
		self.writer.emit_u16(one_const);
		self.writer.emit_opcode(Opcode::IntAdd);
		self.writer.emit_opcode(Opcode::StoreVar);
		self.writer.emit_u16(idx_var);
		// 5j. Jump back to loop_start
		let current = self.writer.position();
		self.writer.emit_opcode(Opcode::Jump);
		let offset = (loop_start as i32 - current as i32 - 3) as i16;
		self.writer.emit_i16(offset);
		// 6. loop_end: patch jumps
		let loop_end = self.writer.position();
		self.writer.patch_jump_at(end_jump, loop_end);
		// Patch all break jumps
		let context = self.loop_contexts.pop().expect("loop context");
		for break_pos in context.break_patches {
			self.writer.patch_jump_at(break_pos, loop_end);
		}
		Ok(())
	}
	/// Compile a module function call (e.g., console::log).
	fn compile_module_call(&mut self, call: ModuleCallAst) -> Result<()> {
		// Compile arguments
		for arg in &call.arguments {
			self.compile_expr_to_operand(arg.clone())?;
		}
		// Emit CallBuiltin with function name
		let func_name = format!("{}::{}", call.module, call.function);
		let name_index = self.program.add_constant(Value::Utf8(func_name));
		self.writer.emit_opcode(Opcode::CallBuiltin);
		self.writer.emit_u16(name_index);
		self.writer.emit_u8(call.arguments.len() as u8);
		Ok(())
	}
	/// Compile a pipeline.
	fn compile_pipeline(&mut self, pipeline: PipelineAst) -> Result<()> {
		for stage in pipeline.stages {
			match stage {
				StageAst::Scan(scan) => {
					// Check if this is a variable reference ($name) or inline
					if scan.table_name.starts_with('$') {
						if scan.table_name == "$inline" {
							// Empty/inline pipeline
							self.writer.emit_opcode(Opcode::Inline);
						} else {
							// Variable reference - load pipeline from variable
							let var_name = &scan.table_name[1..]; // Remove $ prefix
							let name_index = self
								.program
								.add_constant(Value::Utf8(var_name.to_string()));
							let var_id = self.program.next_var_id();
							self.writer.emit_opcode(Opcode::LoadPipelineById);
							self.writer.emit_u32(var_id);
						}
					} else {
						// Normal table scan
						let source_index = self.program.add_source(SourceDef {
							name: scan.table_name,
						});
						self.writer.emit_opcode(Opcode::Source);
						self.writer.emit_u16(source_index);
					}
				}
				StageAst::Filter(filter) => {
					// Convert AST expression to Expr
					let expr = self.ast_to_expr(filter.predicate)?;
					// Compile to CompiledFilter and store in program
					let compiled = compile_filter(expr);
					let filter_index = self.program.add_compiled_filter(compiled);
					// Push compiled filter index
					self.writer.emit_opcode(Opcode::PushExpr);
					self.writer.emit_u16(filter_index);
					// Apply filter operator
					self.writer.emit_opcode(Opcode::Apply);
					self.writer.emit_u8(OperatorKind::Filter as u8);
				}
				StageAst::Select(select) => {
					// Add column list to program
					let col_index = self.program.add_column_list(select.columns);
					// Push column list reference
					self.writer.emit_opcode(Opcode::PushColList);
					self.writer.emit_u16(col_index);
					// Apply select operator
					self.writer.emit_opcode(Opcode::Apply);
					self.writer.emit_u8(OperatorKind::Select as u8);
				}
				StageAst::Take(take) => {
					// Push limit constant
					let const_index = self.program.add_constant(Value::Int8(take.limit as i64));
					self.writer.emit_opcode(Opcode::PushConst);
					self.writer.emit_u16(const_index);
					// Apply take operator
					self.writer.emit_opcode(Opcode::Apply);
					self.writer.emit_u8(OperatorKind::Take as u8);
				}
				StageAst::Extend(extend) => {
					// Compile extension expressions
					let mut spec = Vec::new();
					for (name, expr_ast) in extend.extensions {
						let expr = self.ast_to_expr(expr_ast)?;
						// Compile to CompiledExpr and store in program
						let compiled = compile_expr(expr);
						let expr_index = self.program.add_compiled_expr(compiled);
						spec.push((name, expr_index));
					}
					// Add extension spec to program
					let spec_index = self.program.add_extension_spec(spec);
					// Push extension spec reference
					self.writer.emit_opcode(Opcode::PushExtSpec);
					self.writer.emit_u16(spec_index);
					// Apply extend operator
					self.writer.emit_opcode(Opcode::Apply);
					self.writer.emit_u8(OperatorKind::Extend as u8);
				}
				StageAst::Sort(sort) => {
					// Convert sort columns to SortSpec
					let sort_specs: Vec<SortSpec> = sort
						.columns
						.into_iter()
						.map(|(name, order)| SortSpec {
							column: name,
							order: match order {
								crate::dsl::ast::SortOrder::Asc => SortOrder::Asc,
								crate::dsl::ast::SortOrder::Desc => SortOrder::Desc,
							},
						})
						.collect();
					// Add sort spec to program
					let spec_index = self.program.add_sort_spec(sort_specs);
					// Push sort spec reference
					self.writer.emit_opcode(Opcode::PushSortSpec);
					self.writer.emit_u16(spec_index);
					// Apply sort operator
					self.writer.emit_opcode(Opcode::Apply);
					self.writer.emit_u8(OperatorKind::Sort as u8);
				}
			}
		}
		Ok(())
	}
	/// Convert AST expression to compiled Expr.
	fn ast_to_expr(&mut self, ast: ExprAst) -> Result<Expr> {
		match ast {
			ExprAst::Column {
				name,
				..
			} => Ok(Expr::ColumnRef(ColumnRef {
				index: 0, // Resolved at runtime
				name,
			})),
			ExprAst::Variable {
				name,
				..
			} => {
				// Variables in filter expressions are resolved at runtime via EvalContext
				Ok(Expr::VarRef(name))
			}
			ExprAst::Int {
				value,
				..
			} => Ok(Expr::Literal(Literal::Int8(value))),
			ExprAst::Float {
				value,
				..
			} => Ok(Expr::Literal(Literal::Float8(value))),
			ExprAst::String {
				value,
				..
			} => Ok(Expr::Literal(Literal::Utf8(value))),
			ExprAst::Bool {
				value,
				..
			} => Ok(Expr::Literal(Literal::Bool(value))),
			ExprAst::Null {
				..
			} => Ok(Expr::Literal(Literal::Null)),
			ExprAst::BinaryOp {
				op,
				left,
				right,
				..
			} => {
				let left = Box::new(self.ast_to_expr(*left)?);
				let right = Box::new(self.ast_to_expr(*right)?);
				Ok(Expr::BinaryOp {
					op,
					left,
					right,
				})
			}
			ExprAst::UnaryOp {
				op,
				operand,
				..
			} => {
				let operand = Box::new(self.ast_to_expr(*operand)?);
				Ok(Expr::UnaryOp {
					op,
					operand,
				})
			}
			ExprAst::Paren {
				inner,
				..
			} => self.ast_to_expr(*inner),
			ExprAst::Call {
				function_name,
				arguments,
				..
			} => {
				let args: Result<Vec<Expr>> =
					arguments.into_iter().map(|a| self.ast_to_expr(a)).collect();
				Ok(Expr::Call {
					function_name,
					arguments: args?,
				})
			}
			ExprAst::FieldAccess {
				object,
				field,
				..
			} => {
				let object = Box::new(self.ast_to_expr(*object)?);
				Ok(Expr::FieldAccess {
					object,
					field,
				})
			}
			ExprAst::Subquery {
				kind,
				pipeline,
				..
			} => {
				// Compile the subquery pipeline
				let subquery_index = self.compile_subquery_pipeline(*pipeline)?;
				let expr_kind = match kind {
					dsl::ast::SubqueryKind::Scalar => expr::SubqueryKind::Scalar,
					dsl::ast::SubqueryKind::Exists => expr::SubqueryKind::Exists,
					dsl::ast::SubqueryKind::NotExists => expr::SubqueryKind::NotExists,
				};
				Ok(Expr::Subquery {
					index: subquery_index,
					kind: expr_kind,
				})
			}
			ExprAst::InList {
				expr,
				values,
				negated,
				..
			} => {
				let compiled_expr = Box::new(self.ast_to_expr(*expr)?);
				let compiled_values =
					values.into_iter().map(|v| self.ast_to_expr(v)).collect::<Result<Vec<_>>>()?;
				Ok(Expr::InList {
					expr: compiled_expr,
					values: compiled_values,
					negated,
				})
			}
			ExprAst::InSubquery {
				expr,
				pipeline,
				negated,
				..
			} => {
				let compiled_expr = Box::new(self.ast_to_expr(*expr)?);
				let subquery_index = self.compile_subquery_pipeline(*pipeline)?;
				Ok(Expr::InSubquery {
					expr: compiled_expr,
					subquery_index,
					negated,
				})
			}
		}
	}

	/// Compile a subquery pipeline and return its index in the subqueries pool.
	fn compile_subquery_pipeline(&mut self, pipeline: dsl::ast::PipelineAst) -> Result<u16> {
		// Extract information from the pipeline stages
		let mut source_name = String::new();
		let mut filter_expr_index = None;
		let mut select_list_index = None;
		let mut take_limit = None;
		let mut filter_column_refs = Vec::new();

		for stage in pipeline.stages {
			match stage {
				dsl::ast::StageAst::Scan(scan) => {
					source_name = scan.table_name;
				}
				dsl::ast::StageAst::Filter(filter) => {
					// Collect column references from filter expression
					collect_column_refs(&filter.predicate, &mut filter_column_refs);
					let expr = self.ast_to_expr(filter.predicate)?;
					let compiled = expr::compile_filter(expr);
					filter_expr_index = Some(self.program.add_compiled_filter(compiled));
				}
				dsl::ast::StageAst::Select(select) => {
					select_list_index = Some(self.program.add_column_list(select.columns));
				}
				dsl::ast::StageAst::Take(take) => {
					take_limit = Some(take.limit);
				}
				_ => {
					// Other stages (extend, sort) not yet supported in subqueries
					return Err(VmError::CompileError {
						message: "unsupported stage type in subquery".to_string(),
					});
				}
			}
		}

		// Determine outer references - column refs that don't match common subquery source columns
		// We store all column refs and let the runtime determine which are outer refs
		// by checking if they exist in the source data
		let subquery_def = bytecode::program::SubqueryDef {
			source_name,
			filter_expr_index,
			select_list_index,
			take_limit,
			outer_refs: filter_column_refs,
		};

		Ok(self.program.add_subquery(subquery_def))
	}
	/// Compile a condition expression (pushes boolean to operand stack).
	fn compile_condition_expr(&mut self, expr: ExprAst) -> Result<()> {
		match expr {
			ExprAst::Bool {
				value,
				..
			} => {
				let const_index = self.program.add_constant(Value::Boolean(value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::Variable {
				name,
				..
			} => {
				// Load variable onto operand stack (should be boolean)
				let name_index = self.program.add_constant(Value::Utf8(name));
				self.writer.emit_opcode(Opcode::LoadVar);
				self.writer.emit_u16(name_index);
			}
			ExprAst::BinaryOp {
				op,
				left,
				right,
				..
			} => {
				// Compile left operand
				self.compile_expr_to_operand(*left)?;
				// Compile right operand
				self.compile_expr_to_operand(*right)?;
				// Emit comparison opcode
				match op {
					BinaryOp::Eq => {
						self.writer.emit_opcode(Opcode::IntEq);
					}
					BinaryOp::Lt => {
						self.writer.emit_opcode(Opcode::IntLt);
					}
					_ => {
						return Err(VmError::CompileError {
							message: format!(
								"comparison operator {:?} not yet supported in conditions",
								op
							),
						});
					}
				}
			}
			_ => {
				return Err(VmError::CompileError {
					message: "unsupported condition expression type".to_string(),
				});
			}
		}
		Ok(())
	}
	/// Compile an expression and push result to operand stack.
	fn compile_expr_to_operand(&mut self, expr: ExprAst) -> Result<()> {
		match expr {
			ExprAst::Int {
				value,
				..
			} => {
				let const_index = self.program.add_constant(Value::Int8(value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::Float {
				value,
				..
			} => {
				let ordered = OrderedF64::try_from(value).map_err(|_| VmError::CompileError {
					message: format!("invalid float value: {}", value),
				})?;
				let const_index = self.program.add_constant(Value::Float8(ordered));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::String {
				value,
				..
			} => {
				let const_index = self.program.add_constant(Value::Utf8(value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::Bool {
				value,
				..
			} => {
				let const_index = self.program.add_constant(Value::Boolean(value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::Null {
				..
			} => {
				let const_index = self.program.add_constant(Value::Undefined);
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			ExprAst::Variable {
				name,
				..
			} => {
				let name_index = self.program.add_constant(Value::Utf8(name));
				self.writer.emit_opcode(Opcode::LoadVar);
				self.writer.emit_u16(name_index);
			}
			ExprAst::Column {
				name,
				..
			} => {
				// Column reference - push as column ref
				let name_index = self.program.add_constant(Value::Utf8(name));
				self.writer.emit_opcode(Opcode::PushColRef);
				self.writer.emit_u16(name_index);
			}
			ExprAst::FieldAccess {
				object,
				field,
				..
			} => {
				// Compile the object expression (should push a Record onto operand stack)
				self.compile_expr_to_operand(*object)?;
				// Then get the field by name
				let field_idx = self.program.add_constant(Value::Utf8(field));
				self.writer.emit_opcode(Opcode::GetField);
				self.writer.emit_u16(field_idx);
			}
			ExprAst::BinaryOp {
				op,
				left,
				right,
				..
			} => {
				// Compile left and right operands
				self.compile_expr_to_operand(*left)?;
				self.compile_expr_to_operand(*right)?;
				// Emit the appropriate opcode
				match op {
					BinaryOp::Add => self.writer.emit_opcode(Opcode::IntAdd),
					BinaryOp::Sub => self.writer.emit_opcode(Opcode::IntSub),
					BinaryOp::Mul => self.writer.emit_opcode(Opcode::IntMul),
					BinaryOp::Div => self.writer.emit_opcode(Opcode::IntDiv),
					BinaryOp::Eq => self.writer.emit_opcode(Opcode::IntEq),
					BinaryOp::Lt => self.writer.emit_opcode(Opcode::IntLt),
					_ => {
						return Err(VmError::CompileError {
							message: format!(
								"binary operator {:?} not yet supported in scalar expressions",
								op
							),
						});
					}
				}
			}
			ExprAst::Paren {
				inner,
				..
			} => {
				// Parenthesized expression - just compile the inner expression
				self.compile_expr_to_operand(*inner)?;
			}
			ExprAst::Call {
				function_name,
				arguments,
				..
			} => {
				// Look up function
				let func_index = *self.functions.get(&function_name).ok_or_else(|| {
					VmError::UndefinedFunction {
						name: function_name.clone(),
					}
				})?;

				// Compile and push each argument
				for arg in arguments {
					self.compile_expr_to_operand(arg)?;
				}

				// Emit call
				self.writer.emit_opcode(Opcode::Call);
				self.writer.emit_u16(func_index);
			}
			ExprAst::UnaryOp {
				op,
				operand,
				..
			} => {
				// Compile the operand
				self.compile_expr_to_operand(*operand)?;
				// Emit the appropriate opcode
				match op {
					UnaryOp::Neg | UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => {
						return Err(VmError::CompileError {
							message: format!("unary operator {:?} not yet supported", op),
						});
					}
				}
			}
			ExprAst::Subquery {
				..
			}
			| ExprAst::InList {
				..
			}
			| ExprAst::InSubquery {
				..
			} => {
				return Err(VmError::CompileError {
					message: "subqueries are not supported in scalar expressions (only in filter predicates)".to_string(),
				});
			}
		}
		Ok(())
	}
}
impl Default for BytecodeCompiler {
	fn default() -> Self {
		Self::new()
	}
}
