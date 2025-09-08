// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstCreateSchema,
	plan::logical::{Compiler, CreateSchemaNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_schema<'a>(
		ast: AstCreateSchema<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::CreateSchema(CreateSchemaNode {
			schema: ast.name.fragment(),
			if_not_exists: false,
		}))
	}
}
