// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{Ast, AstAlterSequence},
		identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedColumnPrimitive},
	},
	bump::BumpFragment,
	expression::ExpressionCompiler,
	plan::logical::{AlterSequenceNode, Compiler, LogicalPlan, resolver},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_sequence(&self, ast: AstAlterSequence<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		let namespace = if ast.sequence.namespace.is_empty() {
			vec![BumpFragment::internal(self.bump, resolver::DEFAULT_NAMESPACE)]
		} else {
			ast.sequence.namespace.clone()
		};
		let sequence_name = ast.sequence.name;

		let column = MaybeQualifiedColumnIdentifier {
			primitive: MaybeQualifiedColumnPrimitive::Primitive {
				namespace,
				primitive: sequence_name,
			},
			name: ast.column,
		};

		Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence: ast.sequence,
			column,
			value: ExpressionCompiler::compile(Ast::Literal(ast.value))?,
		}))
	}
}
