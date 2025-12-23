// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use ast::{
	Ast, AstLiteral, AstLiteralUndefined,
	tokenize::{Literal, Token, TokenKind},
};
use async_recursion::async_recursion;
use reifydb_catalog::CatalogQueryTransaction;
use reifydb_type::Fragment;

use crate::{
	ast,
	ast::{AstIf, AstLet, LetValue as AstLetValue},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ConditionalNode, DeclareNode, ElseIfBranch, LetValue, LogicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_let<T: CatalogQueryTransaction>(
		ast: AstLet,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => LetValue::Expression(ExpressionCompiler::compile(*expr)?),
			AstLetValue::Statement(statement) => {
				let plan = Self::compile(statement, tx).await?;
				LetValue::Statement(plan)
			}
		};

		Ok(LogicalPlan::Declare(DeclareNode {
			name: Fragment::internal(ast.name.text().to_string()),
			value,
			mutable: ast.mutable,
		}))
	}

	#[async_recursion]
	pub(crate) async fn compile_if<T: CatalogQueryTransaction + Send>(
		ast: AstIf,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Compile the condition expression
		let condition = ExpressionCompiler::compile(*ast.condition)?;

		// Compile the then branch - should be a single expression
		let then_branch = Box::new(Self::compile_single(*ast.then_block, tx).await?);

		// Compile else if branches
		let mut else_ifs = Vec::new();
		for else_if in ast.else_ifs {
			let condition = ExpressionCompiler::compile(*else_if.condition)?;
			let then_branch = Box::new(Self::compile_single(*else_if.then_block, tx).await?);

			else_ifs.push(ElseIfBranch {
				condition,
				then_branch,
			});
		}

		// Compile optional else branch
		let else_branch = if let Some(else_block) = ast.else_block {
			Some(Box::new(Self::compile_single(*else_block, tx).await?))
		} else {
			let undefined_literal = Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
				kind: TokenKind::Literal(Literal::Undefined),
				fragment: Fragment::internal("undefined"),
			})));
			let wrapped_map = Self::wrap_scalar_in_map(undefined_literal);
			Some(Box::new(Self::compile_map(wrapped_map, tx)?))
		};

		Ok(LogicalPlan::Conditional(ConditionalNode {
			condition,
			then_branch,
			else_ifs,
			else_branch,
		}))
	}
}
