// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::AstTake,
	plan::logical::{Compiler, LogicalPlan, TakeNode},
};

impl Compiler {
	pub(crate) fn compile_take(&self, ast: AstTake) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Take(TakeNode {
			take: ast.take,
		}))
	}
}
