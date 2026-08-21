// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_distinct(&self, ast: AstDistinct<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast.columns,
			rql: ast.rql.to_string(),
		}))
	}
}
