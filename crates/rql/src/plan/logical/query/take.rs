// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstTake,
	plan::logical::{Compiler, LogicalPlan, TakeNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_take(&self, ast: AstTake<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Take(TakeNode {
			take: ast.take,
		}))
	}
}
