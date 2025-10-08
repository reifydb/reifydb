// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_type::Fragment;

use crate::{
	ast::{AstIf, AstLet, LetValue as AstLetValue},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, ConditionalNode, DeclareNode, ElseIfBranch, LetValue, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_let<'a, T: CatalogQueryTransaction>(
		ast: AstLet<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => LetValue::Expression(ExpressionCompiler::compile(*expr)?),
			AstLetValue::Statement(statement) => {
				let plan = Self::compile(statement, tx)?;
				LetValue::Statement(plan)
			}
		};

		Ok(LogicalPlan::Declare(DeclareNode {
			name: Fragment::owned_internal(ast.name.text().to_string()),
			value,
			mutable: ast.mutable,
		}))
	}

	pub(crate) fn compile_if<'a, T: CatalogQueryTransaction>(
		ast: AstIf<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		// Compile the condition expression
		let condition = ExpressionCompiler::compile(*ast.condition)?;

		// Compile the then branch - should be a single expression
		let then_branch = Box::new(Self::compile_single(*ast.then_block, tx)?);

		// Compile else if branches
		let mut else_ifs = Vec::new();
		for else_if in ast.else_ifs {
			let condition = ExpressionCompiler::compile(*else_if.condition)?;
			let then_branch = Box::new(Self::compile_single(*else_if.then_block, tx)?);

			else_ifs.push(ElseIfBranch {
				condition,
				then_branch,
			});
		}

		// Compile optional else branch
		let else_branch = if let Some(else_block) = ast.else_block {
			Some(Box::new(Self::compile_single(*else_block, tx)?))
		} else {
			None
		};

		Ok(LogicalPlan::Conditional(ConditionalNode {
			condition,
			then_branch,
			else_ifs,
			else_branch,
		}))
	}
}
