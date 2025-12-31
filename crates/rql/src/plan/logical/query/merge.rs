// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::IntoStandardTransaction;

use crate::{
	ast::AstMerge,
	plan::logical::{Compiler, LogicalPlan, MergeNode},
};

impl Compiler {
	pub(crate) async fn compile_merge<T: IntoStandardTransaction>(
		&self,
		ast: AstMerge,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Compile the subquery into logical plans
		let with = self.compile(ast.with.statement, tx).await?;
		Ok(LogicalPlan::Merge(MergeNode {
			with,
		}))
	}
}
