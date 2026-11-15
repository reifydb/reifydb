// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateSumType;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreateSumTypeNode, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_sumtype(
		&self,
		ast: AstCreateSumType<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::CreateSumType(CreateSumTypeNode {
			name: ast.name,
			if_not_exists: ast.if_not_exists,
			variants: ast.variants,
		}))
	}
}
