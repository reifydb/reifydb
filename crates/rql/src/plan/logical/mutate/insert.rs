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
	bump::BumpBox,
	plan::logical::{Compiler, InsertDictionaryNode, InsertRingBufferNode, InsertTableNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_insert<T: AsTransaction>(
		&self,
		ast: AstInsert<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		let unresolved_target = ast.target;

		// Compile the source (the FROM clause)
		let source = self.compile_single(BumpBox::into_inner(ast.source), tx)?;

		// Check in the catalog whether the target is a table, ring buffer, or dictionary
		let namespace_name = unresolved_target.namespace.first().map(|n| n.text().to_string());
		let namespace_name_str = namespace_name.as_deref().unwrap_or("default");
		let target_name = unresolved_target.name.text();
		let name = unresolved_target.name;
		let namespace = unresolved_target.namespace;

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name_str)? {
			ns.id
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertTable(InsertTableNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		};

		// Check if it's a ring buffer first
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertRingBuffer(InsertRingBufferNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		}

		// Check if it's a dictionary
		if self.catalog.find_dictionary_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedDictionaryIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::InsertDictionary(InsertDictionaryNode {
				target,
				source: BumpBox::new_in(source, self.bump),
			}));
		}

		// Assume it's a table (will error during physical plan if not found)
		let mut target = MaybeQualifiedTableIdentifier::new(name);
		if !namespace.is_empty() {
			target = target.with_namespace(namespace);
		}
		Ok(LogicalPlan::InsertTable(InsertTableNode {
			target,
			source: BumpBox::new_in(source, self.bump),
		}))
	}
}
