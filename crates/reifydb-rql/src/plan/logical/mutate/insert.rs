// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstInsert,
	plan::logical::{Compiler, InsertNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_insert<'a>(
		ast: AstInsert<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Insert(InsertNode {
			schema: ast.schema.map(|s| s.fragment()),
			table: ast.table.fragment(),
		}))
	}
}
