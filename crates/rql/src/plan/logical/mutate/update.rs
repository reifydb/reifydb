// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstUpdate,
	plan::logical::{Compiler, LogicalPlan, UpdateNode, UpdateRingBufferNode, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_update<'a, 't, T: CatalogQueryTransaction>(
		ast: AstUpdate<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Get the target, if None it means the target will come from a pipeline
		let Some(unresolved) = &ast.target else {
			// For pipeline case, we don't know if it's a table or ring buffer yet
			return Ok(LogicalPlan::Update(UpdateNode {
				target: None,
				input: None,
			}));
		};

		// Try to resolve as table first (most common case)
		match resolver.resolve_source_as_table(unresolved.namespace.as_ref(), &unresolved.name, true) {
			Ok(target) => Ok(LogicalPlan::Update(UpdateNode {
				target: Some(target),
				input: None,
			})),
			Err(table_error) => {
				// Table not found, try ring buffer
				match resolver.resolve_source_as_ring_buffer(
					unresolved.namespace.as_ref(),
					&unresolved.name,
					true,
				) {
					Ok(target) => Ok(LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
						target,
						input: None,
					})),
					// Ring buffer also not found, return the table error as it's more common
					Err(_) => Err(table_error),
				}
			}
		}
	}
}
