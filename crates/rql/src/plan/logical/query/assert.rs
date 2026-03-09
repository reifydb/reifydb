// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstAssert,
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{AssertBlockNode, AssertNode, Compiler, LogicalPlan},
	token::token::{Literal, TokenKind},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_assert(&self, ast: AstAssert<'bump>) -> Result<LogicalPlan<'bump>> {
		let message = ast.message.and_then(|tok| {
			if matches!(tok.kind, TokenKind::Literal(Literal::Text)) {
				Some(tok.fragment.text().to_string())
			} else {
				None
			}
		});

		// Multi-statement or ASSERT ERROR: use block-based runtime recompilation
		if let Some(body) = ast.body {
			return Ok(LogicalPlan::AssertBlock(AssertBlockNode {
				rql: body,
				expect_error: ast.expect_error,
				message,
			}));
		}

		// Single-expression ASSERT (pipeline-compatible)
		Ok(LogicalPlan::Assert(AssertNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node.unwrap()))?,
			message,
			rql: ast.rql.to_string(),
		}))
	}
}
