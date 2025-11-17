// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateFlow,
	plan::logical::{Compiler, CreateFlowNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_flow<'a, T: CatalogQueryTransaction>(
		ast: AstCreateFlow<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		// Use the flow identifier directly from AST
		let flow = ast.flow;

		// Compile the AS clause (required for flows)
		let with = Compiler::compile(ast.as_clause, tx)?;

		Ok(LogicalPlan::CreateFlow(CreateFlowNode {
			flow,
			if_not_exists: ast.if_not_exists,
			as_clause: with,
		}))
	}
}
