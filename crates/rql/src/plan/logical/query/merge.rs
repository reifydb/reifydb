// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstMerge,
	plan::logical::{Compiler, LogicalPlan, MergeNode},
};

impl Compiler {
	pub(crate) async fn compile_merge<T: CatalogQueryTransaction>(
		ast: AstMerge,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Compile the subquery into logical plans
		let with = Self::compile(ast.with.statement, tx).await?;
		Ok(LogicalPlan::Merge(MergeNode {
			with,
		}))
	}
}
