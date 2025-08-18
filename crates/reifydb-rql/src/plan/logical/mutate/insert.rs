// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstInsert,
	plan::logical::{Compiler, InsertNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_insert(
		ast: AstInsert,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Insert(InsertNode {
			schema: ast.schema.map(|s| s.fragment()),
			table: ast.table.fragment(),
		}))
	}
}
