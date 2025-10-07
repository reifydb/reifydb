// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_type::Fragment;

use crate::{
	ast::AstLet,
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LetNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_let<'a, T: CatalogQueryTransaction>(
		ast: AstLet<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Let(LetNode {
			name: Fragment::owned_internal(ast.name.text().to_string()),
			value: ExpressionCompiler::compile(*ast.value)?,
			mutable: ast.mutable,
		}))
	}
}
