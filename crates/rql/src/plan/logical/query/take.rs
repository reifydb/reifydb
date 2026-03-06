// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::{AstTake, AstTakeValue},
	nodes::TakeLimit,
	plan::logical::{Compiler, LogicalPlan, TakeNode},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_take(&self, ast: AstTake<'bump>) -> Result<LogicalPlan<'bump>> {
		let take = match ast.take {
			AstTakeValue::Literal(n) => TakeLimit::Literal(n),
			AstTakeValue::Variable(tok) => {
				let name = tok.fragment.text().trim_start_matches('$').to_string();
				TakeLimit::Variable(name)
			}
		};
		Ok(LogicalPlan::Take(TakeNode {
			take,
		}))
	}
}
