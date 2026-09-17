// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_distinct(&self, ast: AstDistinct<'bump>) -> Result<LogicalPlan<'bump>> {
		let with = Self::compile_distinct_with(ast.with.as_ref())?;
		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast.columns,
			with,
			rql: ast.rql.to_string(),
		}))
	}
}
