// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateSource;

use crate::{
	Result,
	plan::logical::{Compiler, CreateSourceNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_source(&self, ast: AstCreateSource<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateSource(CreateSourceNode {
			name: ast.name,
			connector: ast.connector,
			config: ast.config,
			target: ast.target,
		}))
	}
}
