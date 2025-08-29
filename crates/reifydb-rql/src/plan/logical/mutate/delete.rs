// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstDelete,
	plan::logical::{Compiler, DeleteNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_delete<'a>(
		ast: AstDelete<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Delete(DeleteNode {
			schema: ast.schema.map(|s| s.fragment()),
			table: ast.table.map(|t| t.fragment()),
			input: None, /* Input will be set by the pipeline
			              * builder */
		}))
	}
}
