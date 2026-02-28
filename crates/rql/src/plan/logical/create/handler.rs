// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{ast::AstCreateHandler, identifier::MaybeQualifiedProcedureIdentifier},
	plan::logical::{Compiler, CreateProcedureNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_handler(&self, ast: AstCreateHandler<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateProcedure(CreateProcedureNode {
			procedure: MaybeQualifiedProcedureIdentifier {
				namespace: ast.name.namespace,
				name: ast.name.name,
			},
			params: vec![],
			body_source: ast.body_source,
			on_event: Some(ast.on_event),
			on_variant: Some(ast.on_variant),
		}))
	}
}
