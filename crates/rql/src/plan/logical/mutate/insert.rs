// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	ast::{
		ast::AstInsert,
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedTableIdentifier,
		},
	},
	plan::logical::{Compiler, InsertDictionaryNode, InsertRingBufferNode, InsertTableNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_insert<T: IntoStandardTransaction>(
		&self,
		ast: AstInsert,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Get the target, if None it means the target will come from a pipeline
		let Some(unresolved_target) = ast.target else {
			// TODO: Handle pipeline case where target comes from previous operation
			unimplemented!("Pipeline insert target not yet implemented");
		};

		// Check in the catalog whether the target is a table or ring buffer
		let namespace_name = unresolved_target.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let target_name = unresolved_target.name.text();

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name)? {
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
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(unresolved_target.name.clone());
			if let Some(ns) = unresolved_target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::InsertRingBuffer(InsertRingBufferNode {
				target,
			}));
		}

		// Check if it's a dictionary
		if self.catalog.find_dictionary_by_name(tx, namespace_id, target_name)?.is_some() {
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
