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
		let namespace = ast
			.sequence
			.namespace
			.unwrap_or_else(|| BumpFragment::internal(self.bump, resolver::DEFAULT_NAMESPACE));
		let sequence_name = ast.sequence.name;

		// Create a maybe qualified column identifier
		// The column belongs to the same table as the sequence
		let column = MaybeQualifiedColumnIdentifier {
			primitive: MaybeQualifiedColumnPrimitive::Primitive {
				namespace: Some(namespace),
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
