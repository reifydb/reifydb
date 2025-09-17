// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstInsert,
	plan::logical::{Compiler, InsertRingBufferNode, InsertTableNode, LogicalPlan, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_insert<'a, 't, T: CatalogQueryTransaction>(
		ast: AstInsert<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Get the target, if None it means the target will come from a pipeline
		let Some(unresolved_target) = ast.target else {
			// TODO: Handle pipeline case where target comes from previous operation
			unimplemented!("Pipeline insert target not yet implemented");
		};

		// Try to resolve as table first (most common case)
		match resolver.resolve_source_as_table(
			unresolved_target.namespace.as_ref(),
			&unresolved_target.name,
			true,
		) {
			Ok(target) => Ok(LogicalPlan::InsertTable(InsertTableNode {
				target,
			})),
			Err(table_error) => {
				// Table not found, try ring buffer
				match resolver.resolve_source_as_ring_buffer(
					unresolved_target.namespace.as_ref(),
					&unresolved_target.name,
					true,
				) {
					Ok(target) => Ok(LogicalPlan::InsertRingBuffer(InsertRingBufferNode {
						target,
					})),
					// Ring buffer also not found, return the table error as it's more common
					Err(_) => Err(table_error),
				}
			}
		}
	}
}
