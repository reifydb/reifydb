// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::ast::{
		Ast, AstBlock, AstCallFunction, AstDefFunction, AstFor, AstIf, AstLet, AstLiteral, AstLiteralUndefined,
		AstLoop, AstReturn, AstWhile, LetValue as AstLetValue,
	},
	convert_data_type_with_constraints,
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, ConditionalNode, DeclareNode, ElseIfBranch, ForNode, LetValue, LogicalPlan, LoopNode,
		WhileNode,
		function::{CallFunctionNode, DefineFunctionNode, FunctionParameter, ReturnNode},
	},
	token::token::{Literal, Token, TokenKind},
};

impl Compiler {
	pub(crate) fn compile_let<T: AsTransaction>(&self, ast: AstLet, tx: &mut T) -> crate::Result<LogicalPlan> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => LetValue::Expression(ExpressionCompiler::compile(*expr)?),
			AstLetValue::Statement(statement) => {
				let plan = self.compile(statement, tx)?;
				LetValue::Statement(plan)
			}
		};

		Ok(LogicalPlan::Declare(DeclareNode {
			name: Fragment::internal(ast.name.text().to_string()),
			value,
		}))
	}

	pub(crate) fn compile_if<T: AsTransaction>(&self, ast: AstIf, tx: &mut T) -> crate::Result<LogicalPlan> {
		// Compile the condition expression
		let condition = ExpressionCompiler::compile(*ast.condition)?;

		// Compile the then branch from block
		let then_branch = Box::new(self.compile_block_single(&ast.then_block, tx)?);

		// Compile else if branches
		let mut else_ifs = Vec::new();
		for else_if in ast.else_ifs {
			let condition = ExpressionCompiler::compile(*else_if.condition)?;
			let then_branch = Box::new(self.compile_block_single(&else_if.then_block, tx)?);

			else_ifs.push(ElseIfBranch {
				condition,
				then_branch,
			});
		}

		// Compile optional else branch
		let else_branch = if let Some(ref else_block) = ast.else_block {
			Some(Box::new(self.compile_block_single(else_block, tx)?))
		} else {
			let undefined_literal = Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
				kind: TokenKind::Literal(Literal::Undefined),
				fragment: Fragment::internal("undefined"),
			})));
			let wrapped_map = Self::wrap_scalar_in_map(undefined_literal);
			Some(Box::new(self.compile_map(wrapped_map)?))
		};

		Ok(LogicalPlan::Conditional(ConditionalNode {
			condition,
			then_branch,
			else_ifs,
			else_branch,
		}))
	}

	/// Compile a block as a single logical plan node.
	/// Takes the first expression from the first statement.
	fn compile_block_single<T: AsTransaction>(&self, block: &AstBlock, tx: &mut T) -> crate::Result<LogicalPlan> {
		if let Some(first_stmt) = block.statements.first() {
			if let Some(first_node) = first_stmt.nodes.first() {
				return self.compile_single(first_node.clone(), tx);
			}
		}
		// Empty block → undefined wrapped in MAP
		let undefined_literal = Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
			kind: TokenKind::Literal(Literal::Undefined),
			fragment: Fragment::internal("undefined"),
		})));
		let wrapped_map = Self::wrap_scalar_in_map(undefined_literal);
		self.compile_map(wrapped_map)
	}

	/// Compile all statements in a block into a Vec<Vec<LogicalPlan>>
	pub(crate) fn compile_block<T: AsTransaction>(
		&self,
		block: &AstBlock,
		tx: &mut T,
	) -> crate::Result<Vec<Vec<LogicalPlan>>> {
		let mut result = Vec::new();
		for stmt in &block.statements {
			let ast_stmt = stmt.clone();
			let plans = self.compile(ast_stmt, tx)?;
			result.push(plans);
		}
		Ok(result)
	}

	pub(crate) fn compile_loop<T: AsTransaction>(&self, ast: AstLoop, tx: &mut T) -> crate::Result<LogicalPlan> {
		let body = self.compile_block(&ast.body, tx)?;
		Ok(LogicalPlan::Loop(LoopNode {
			body,
		}))
	}

	pub(crate) fn compile_while<T: AsTransaction>(&self, ast: AstWhile, tx: &mut T) -> crate::Result<LogicalPlan> {
		let condition = ExpressionCompiler::compile(*ast.condition)?;
		let body = self.compile_block(&ast.body, tx)?;
		Ok(LogicalPlan::While(WhileNode {
			condition,
			body,
		}))
	}

	pub(crate) fn compile_for<T: AsTransaction>(&self, ast: AstFor, tx: &mut T) -> crate::Result<LogicalPlan> {
		let variable_name = {
			let text = ast.variable.token.fragment.text();
			let clean = if text.starts_with('$') {
				&text[1..]
			} else {
				text
			};
			Fragment::internal(clean)
		};
		let iterable_ast = *ast.iterable;
		let iterable_stmt = crate::ast::ast::AstStatement {
			nodes: vec![iterable_ast],
			has_pipes: false,
			is_output: false,
		};
		let iterable = self.compile(iterable_stmt, tx)?;
		let body = self.compile_block(&ast.body, tx)?;
		Ok(LogicalPlan::For(ForNode {
			variable_name,
			iterable,
			body,
		}))
	}

	/// Compile a function definition
	pub(crate) fn compile_def_function<T: AsTransaction>(
		&self,
		ast: AstDefFunction,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Convert function name
		let name = ast.name.token.fragment.clone();

		// Convert parameters
		let mut parameters = Vec::new();
		for param in ast.parameters {
			let param_name = param.variable.token.fragment.clone();
			let type_constraint = if let Some(ref ty) = param.type_annotation {
				Some(convert_data_type_with_constraints(ty)?)
			} else {
				None
			};
			parameters.push(FunctionParameter {
				name: param_name,
				type_constraint,
			});
		}

		// Convert optional return type
		let return_type = if let Some(ref ty) = ast.return_type {
			Some(convert_data_type_with_constraints(ty)?)
		} else {
			None
		};

		// Compile the body
		let body = self.compile_block(&ast.body, tx)?;

		Ok(LogicalPlan::DefineFunction(DefineFunctionNode {
			name,
			parameters,
			return_type,
			body,
		}))
	}

	/// Compile a return statement
	pub(crate) fn compile_return(&self, ast: AstReturn) -> crate::Result<LogicalPlan> {
		let value = if let Some(expr) = ast.value {
			Some(ExpressionCompiler::compile(*expr)?)
		} else {
			None
		};

		Ok(LogicalPlan::Return(ReturnNode {
			value,
		}))
	}

	/// Compile a function call (potentially user-defined)
	pub(crate) fn compile_call_function(&self, ast: AstCallFunction) -> crate::Result<LogicalPlan> {
		let name = ast.function.name.clone();

		// Compile arguments as expressions
		let mut arguments = Vec::new();
		for arg in ast.arguments.nodes {
			arguments.push(ExpressionCompiler::compile(arg)?);
		}

		Ok(LogicalPlan::CallFunction(CallFunctionNode {
			name,
			arguments,
		}))
	}
}
