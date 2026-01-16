// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::{
		ast::{Ast, AstIf, AstLet, AstLiteral, AstLiteralUndefined, LetValue as AstLetValue},
		tokenize::token::{Literal, Token, TokenKind},
	},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ConditionalNode, DeclareNode, ElseIfBranch, LetValue, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_let<T: IntoStandardTransaction>(
		&self,
		ast: AstLet,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
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

	pub(crate) fn compile_if<T: IntoStandardTransaction>(
		&self,
		ast: AstIf,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Compile the condition expression
		let condition = ExpressionCompiler::compile(*ast.condition)?;

		// Compile the then branch - should be a single expression
		let then_branch = Box::new(self.compile_single(*ast.then_block, tx)?);

		// Compile else if branches
		let mut else_ifs = Vec::new();
		for else_if in ast.else_ifs {
			let condition = ExpressionCompiler::compile(*else_if.condition)?;
			let then_branch = Box::new(self.compile_single(*else_if.then_block, tx)?);

			else_ifs.push(ElseIfBranch {
				condition,
				then_branch,
			});
		}

		// Compile optional else branch
		let else_branch = if let Some(else_block) = ast.else_block {
			Some(Box::new(self.compile_single(*else_block, tx)?))
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
}
