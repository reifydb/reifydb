// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::IntoStandardTransaction;

use crate::{
	ast::{
		AstUpdate,
		identifier::{MaybeQualifiedRingBufferIdentifier, MaybeQualifiedTableIdentifier},
	},
	plan::logical::{Compiler, LogicalPlan, UpdateRingBufferNode, UpdateTableNode},
};

impl Compiler {
	pub(crate) fn compile_update<T: IntoStandardTransaction>(
		&self,
		ast: AstUpdate,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Get the target, if None it means the target will come from a pipeline
		let Some(unresolved) = &ast.target else {
			// For pipeline case, we don't know if it's a table or ring buffer yet
			return Ok(LogicalPlan::Update(UpdateTableNode {
				target: None,
				input: None,
			}));
		};

		// Check in the catalog whether the target is a table or ring buffer
		let namespace_name = unresolved.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let target_name = unresolved.name.text();

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name)? {
			ns.id
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(unresolved.name.clone());
			if let Some(ns) = unresolved.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: None,
			}));
		};

		// Check if it's a ring buffer first
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(unresolved.name.clone());
			if let Some(ns) = unresolved.namespace.clone() {
				target = target.with_namespace(ns);
			}
			Ok(LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
				target,
				input: None,
			}))
		} else {
			// Assume it's a table (will error during physical plan if not found)
			let mut target = MaybeQualifiedTableIdentifier::new(unresolved.name.clone());
			if let Some(ns) = unresolved.namespace.clone() {
				target = target.with_namespace(ns);
			}
			Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: None,
			}))
		}
	}
}
