// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstUpdate,
	plan::logical::{Compiler, LogicalPlan, UpdateNode},
};

impl Compiler {
	pub(crate) fn compile_update<'a>(
		ast: AstUpdate<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Update(UpdateNode {
			schema: ast.schema.map(|s| s.fragment()),
			table: ast.table.map(|t| t.fragment()),
			input: None, /* Input will be set by the pipeline
			              * builder */
		}))
	}
}
