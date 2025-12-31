// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateFlow,
	plan::logical::{Compiler, CreateFlowNode, LogicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_create_flow<T: CatalogQueryTransaction + Send>(
		ast: AstCreateFlow,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Use the flow identifier directly from AST
		let flow = ast.flow;

		// Compile the AS clause (required for flows)
		let with = Compiler::compile(ast.as_clause, tx).await?;

		Ok(LogicalPlan::CreateFlow(CreateFlowNode {
			flow,
			if_not_exists: ast.if_not_exists,
			as_clause: with,
		}))
	}
}
