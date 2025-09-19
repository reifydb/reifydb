// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstDelete,
	plan::logical::{
		Compiler, DeleteRingBufferNode, DeleteTableNode, LogicalPlan, identifier::SourceIdentifier,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_delete<'a, 't, T: CatalogQueryTransaction>(
		ast: AstDelete<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve the unresolved source to a table or ring buffer
		if let Some(unresolved) = &ast.target {
			// Create a source identifier from the unresolved source
			let source_id = resolver.resolve_unresolved_source(&unresolved)?;

			// Determine if it's a table or ring buffer based on the source type
			match source_id {
				SourceIdentifier::Table(table_id) => Ok(LogicalPlan::DeleteTable(DeleteTableNode {
					target: Some(table_id),
					input: None,
				})),
				SourceIdentifier::RingBuffer(ring_buffer_id) => {
					Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
						target: ring_buffer_id,
						input: None,
					}))
				}
				_ => {
					// Source is not a table or ring buffer (might be view, etc.)
					Err(crate::error::IdentifierError::SourceNotFound(
						crate::error::SourceNotFoundError {
							namespace: unresolved
								.namespace
								.as_ref()
								.map(|n| n.text())
								.unwrap_or(resolver.default_namespace())
								.to_string(),
							name: unresolved.name.text().to_string(),
							fragment: unresolved.name.clone().into_owned(),
						},
					)
					.into())
				}
			}
		} else {
			// No target specified - use DeleteTable with None
			Ok(LogicalPlan::DeleteTable(DeleteTableNode {
				target: None,
				input: None,
			}))
		}
	}
}
