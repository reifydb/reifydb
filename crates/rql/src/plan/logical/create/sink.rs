// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateSink;

use crate::{
	Result,
	plan::logical::{Compiler, CreateSinkNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_sink(&self, ast: AstCreateSink<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateSink(CreateSinkNode {
			name: ast.name,
			source: ast.source,
			connector: ast.connector,
			config: ast.config,
		}))
	}
}
