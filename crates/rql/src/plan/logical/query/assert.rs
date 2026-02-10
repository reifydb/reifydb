// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstAssert,
	bump::BumpBox,
	expression::ExpressionCompiler,
	plan::logical::{AssertNode, Compiler, LogicalPlan},
	token::token::Literal,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_assert(&self, ast: AstAssert<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		let message = ast.message.and_then(|tok| {
			if matches!(tok.kind, crate::token::token::TokenKind::Literal(Literal::Text)) {
				Some(tok.fragment.text().to_string())
			} else {
				None
			}
		});

		Ok(LogicalPlan::Assert(AssertNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(ast.node))?,
			message,
		}))
	}
}
