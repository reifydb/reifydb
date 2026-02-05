// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

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
	pub(crate) fn compile_insert<T: AsTransaction>(
		&self,
		ast: AstInsert,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		let unresolved_target = ast.target;

		// Compile the source (the FROM clause)
		let source = self.compile_single(*ast.source, tx)?;

		// Check in the catalog whether the target is a table, ring buffer, or dictionary
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
				source: Box::new(source),
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
				source: Box::new(source),
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
				source: Box::new(source),
			}));
		}

		// Assume it's a table (will error during physical plan if not found)
		let mut target = MaybeQualifiedTableIdentifier::new(unresolved_target.name.clone());
		if let Some(ns) = unresolved_target.namespace.clone() {
			target = target.with_namespace(ns);
		}
		Ok(LogicalPlan::InsertTable(InsertTableNode {
			target,
			source: Box::new(source),
		}))
	}
}
