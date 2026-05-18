// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_distinct(&self, ast: AstDistinct<'bump>) -> Result<LogicalPlan<'bump>> {
		let ttl = match ast.ttl {
			Some(ast_ttl) => Some(Self::compile_operator_ttl(ast_ttl)?),
			None => None,
		};

		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast.columns,
			ttl,
			rql: ast.rql.to_string(),
		}))
	}
}
