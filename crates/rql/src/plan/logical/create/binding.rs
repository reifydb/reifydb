// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateBinding;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreateBindingNode, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_binding(
		&self,
		ast: AstCreateBinding<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::CreateBinding(CreateBindingNode {
			name: ast.name,
			procedure: ast.procedure,
			protocol: ast.protocol,
		}))
	}
}
