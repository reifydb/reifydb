// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateTest,
	plan::logical::{Compiler, CreateTestNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_test(&self, ast: AstCreateTest<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateTest(CreateTestNode {
			test: ast.name,
			cases: ast.cases,
			body_source: ast.body_source,
		}))
	}
}
