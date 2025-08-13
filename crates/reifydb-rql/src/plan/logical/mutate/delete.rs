// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstDelete,
	plan::logical::{Compiler, DeleteNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_delete(
		ast: AstDelete,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::Delete(DeleteNode {
			schema: ast.schema.map(|s| s.span()),
			table: ast.table.span(),
		}))
	}
}
