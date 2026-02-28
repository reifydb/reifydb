// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateMigration,
	plan::logical::{Compiler, CreateMigrationNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_migration(&self, ast: AstCreateMigration<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateMigration(CreateMigrationNode {
			name: ast.name,
			body_source: ast.body_source,
			rollback_body_source: ast.rollback_body_source,
		}))
	}
}
