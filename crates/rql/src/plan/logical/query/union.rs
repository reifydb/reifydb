// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstUnion,
	plan::logical::{Compiler, LogicalPlan, UnionNode},
};

impl Compiler {
	pub(crate) fn compile_union<'a, T: CatalogQueryTransaction>(
		ast: AstUnion<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		// Compile the subquery into logical plans
		let with = Self::compile(ast.with.statement, tx)?;
		Ok(LogicalPlan::Union(UnionNode {
			with,
		}))
	}
}
