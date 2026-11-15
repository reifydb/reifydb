// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateMigration;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreateMigrationNode, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_migration(
		&self,
		ast: AstCreateMigration<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::CreateMigration(CreateMigrationNode {
			name: ast.name,
			body_source: ast.body_source,
			rollback_body_source: ast.rollback_body_source,
		}))
	}
}
