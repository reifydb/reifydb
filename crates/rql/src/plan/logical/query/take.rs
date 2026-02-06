// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstTake,
	plan::logical::{Compiler, LogicalPlan, TakeNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_take(&self, ast: AstTake<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::Take(TakeNode {
			take: ast.take,
		}))
	}
}
