// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateProcedure;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreateProcedureNode, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_procedure(
		&self,
		ast: AstCreateProcedure<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::CreateProcedure(CreateProcedureNode {
			procedure: ast.name,
			params: ast.params,
			body_source: ast.body_source,
			on_event: None,
			on_variant: None,
			is_test: ast.is_test,
			run_as: ast.run_as,
		}))
	}
}
