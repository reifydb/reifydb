// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	ast::ast::AstCreateFlow,
	plan::logical::{Compiler, CreateFlowNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_flow<T: IntoStandardTransaction>(
		&self,
		ast: AstCreateFlow,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Use the flow identifier directly from AST
		let flow = ast.flow;

		// Compile the AS clause (required for flows)
		let with = self.compile(ast.as_clause, tx)?;

		Ok(LogicalPlan::CreateFlow(CreateFlowNode {
			flow,
			if_not_exists: ast.if_not_exists,
			as_clause: with,
		}))
	}
}
