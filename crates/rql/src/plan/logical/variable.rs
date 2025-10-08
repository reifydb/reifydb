// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_type::Fragment;

use crate::{
	ast::{AstLet, LetValue as AstLetValue},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, DeclareNode, LetValue, LogicalPlan},
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
}
