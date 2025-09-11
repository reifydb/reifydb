// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::identifier::{
	ColumnIdentifier, ColumnSource, SequenceIdentifier,
};
use reifydb_type::Fragment;

use crate::{
	ast::{Ast, AstAlterSequence},
	expression::ExpressionCompiler,
	plan::logical::{
		AlterSequenceNode, Compiler, LogicalPlan,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstAlterSequence<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		let (schema, sequence_name) = {
			// Use the resolver's resolve_maybe_sequence method if
			// we add one For now, just use default schema
			// through resolver
			let schema = ast.sequence.schema.unwrap_or_else(|| {
				Fragment::borrowed_internal(
					resolver.default_schema(),
				)
			});
			(schema, ast.sequence.name.clone())
		};

		let sequence = SequenceIdentifier::new(
			schema.clone(),
			sequence_name.clone(),
		);

		// Create a fully qualified column identifier
		// The column belongs to the same table as the sequence
		let column = ColumnIdentifier {
			source: ColumnSource::Source {
				schema,
				source: sequence_name,
			},
			name: ast.column.clone(),
		};

		Ok(LogicalPlan::AlterSequence(AlterSequenceNode {
			sequence,
			column,
			value: ExpressionCompiler::compile(Ast::Literal(
				ast.value,
			))?,
		}))
	}
}
