// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::{
		AstInsert,
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedTableIdentifier,
		},
	},
	plan::logical::{Compiler, InsertDictionaryNode, InsertRingBufferNode, InsertTableNode, LogicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_insert<'a, T: CatalogQueryTransaction>(
		ast: AstInsert<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		// Get the target, if None it means the target will come from a pipeline
		let Some(unresolved_target) = ast.target else {
			// TODO: Handle pipeline case where target comes from previous operation
			unimplemented!("Pipeline insert target not yet implemented");
		};

		// Check in the catalog whether the target is a table or ring buffer
		let namespace_name = unresolved_target.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let target_name = unresolved_target.name.text();

		// Try to find namespace
		let namespace_id = if let Some(ns) = tx.find_namespace_by_name(namespace_name).await? {
			ns.id
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(unresolved_target.name.clone());
			if let Some(ns) = unresolved_target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::InsertTable(InsertTableNode {
				target,
			}));
		};

		// Check if it's a ring buffer first
		if tx.find_ringbuffer_by_name(namespace_id, target_name).await?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(unresolved_target.name.clone());
			if let Some(ns) = unresolved_target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::InsertRingBuffer(InsertRingBufferNode {
				target,
			}));
		}

		// Check if it's a dictionary
		if tx.find_dictionary_by_name(namespace_id, target_name).await?.is_some() {
			let mut target = MaybeQualifiedDictionaryIdentifier::new(unresolved_target.name.clone());
			if let Some(ns) = unresolved_target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::InsertDictionary(InsertDictionaryNode {
				target,
			}));
		}

		// Assume it's a table (will error during physical plan if not found)
		let mut target = MaybeQualifiedTableIdentifier::new(unresolved_target.name.clone());
		if let Some(ns) = unresolved_target.namespace.clone() {
			target = target.with_namespace(ns);
		}
		Ok(LogicalPlan::InsertTable(InsertTableNode {
			target,
		}))
	}
}
