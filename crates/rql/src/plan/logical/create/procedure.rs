// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateProcedure,
	plan::logical::{Compiler, CreateProcedureNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_procedure(&self, ast: AstCreateProcedure<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateProcedure(CreateProcedureNode {
			procedure: ast.name,
			params: ast.params,
			body_source: ast.body_source,
			on_event: None,
			on_variant: None,
		}))
	}
}
