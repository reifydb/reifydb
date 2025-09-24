// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
use reifydb_type::Fragment;

use crate::{
	ast::{Ast, AstAlterSequence},
	expression::ExpressionCompiler,
	plan::logical::{AlterSequenceNode, Compiler, LogicalPlan, resolver},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a, T: CatalogQueryTransaction>(
		ast: AstAlterSequence<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let (namespace, sequence_name) =
			{
				// Use the resolve's resolve_maybe_sequence method if
				// we add one For now, just use default namespace
				// through resolve
				let namespace =
					ast.sequence.namespace.as_ref().cloned().unwrap_or_else(|| {
						Fragment::borrowed_internal(resolver::DEFAULT_NAMESPACE)
					});
				(namespace, ast.sequence.name.clone())
			};

		// Create a fully qualified column identifier
		// The column belongs to the same table as the sequence
		let column = ColumnIdentifier {
			source: ColumnSource::Source {
				namespace,
				source: sequence_name,
			},
			name: ast.column.clone(),
		};

		Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence: ast.sequence.clone(),
			column,
			value: ExpressionCompiler::compile(Ast::Literal(ast.value))?,
		}))
	}
}
