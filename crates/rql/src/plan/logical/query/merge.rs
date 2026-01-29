// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::ast::AstMerge,
	plan::logical::{Compiler, LogicalPlan, MergeNode},
};

impl Compiler {
	pub(crate) fn compile_merge<T: AsTransaction>(&self, ast: AstMerge, tx: &mut T) -> crate::Result<LogicalPlan> {
		// Compile the subquery into logical plans
		let with = self.compile(ast.with.statement, tx)?;
		Ok(LogicalPlan::Merge(MergeNode {
			with,
		}))
	}
}
