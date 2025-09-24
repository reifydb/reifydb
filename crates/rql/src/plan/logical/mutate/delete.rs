// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::{
		AstDelete,
		identifier::{MaybeQualifiedRingBufferIdentifier, MaybeQualifiedTableIdentifier},
	},
	plan::logical::{Compiler, DeleteRingBufferNode, DeleteTableNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_delete<'a, T: CatalogQueryTransaction>(
		ast: AstDelete<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		if let Some(unresolved) = &ast.target {
			// Check in the catalog whether the target is a table or ring buffer
			let namespace_name = unresolved.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
			let target_name = unresolved.name.text();

			// Try to find namespace
			let namespace_id = if let Some(ns) = tx.find_namespace_by_name(namespace_name)? {
				ns.id
			} else {
				// If namespace doesn't exist, default to table (will error during physical plan)
				let mut target = MaybeQualifiedTableIdentifier::new(unresolved.name.clone());
				if let Some(ns) = unresolved.namespace.clone() {
					target = target.with_namespace(ns);
				}
				return Ok(LogicalPlan::DeleteTable(DeleteTableNode {
					target: Some(target),
					input: None,
				}));
			};

			// Check if it's a ring buffer first
			if tx.find_ring_buffer_by_name(namespace_id, target_name)?.is_some() {
				let mut target = MaybeQualifiedRingBufferIdentifier::new(unresolved.name.clone());
				if let Some(ns) = unresolved.namespace.clone() {
					target = target.with_namespace(ns);
				}
				Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
					target,
					input: None,
				}))
			} else {
				// Assume it's a table (will error during physical plan if not found)
				let mut target = MaybeQualifiedTableIdentifier::new(unresolved.name.clone());
				if let Some(ns) = unresolved.namespace.clone() {
					target = target.with_namespace(ns);
				}
				Ok(LogicalPlan::DeleteTable(DeleteTableNode {
					target: Some(target),
					input: None,
				}))
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
