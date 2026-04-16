// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{binding::Binding, change::CatalogTrackBindingChangeOperations, id::BindingId};
use reifydb_type::Result;

use crate::{
	change::{Change, OperationType::*, TransactionalBindingChanges},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackBindingChangeOperations for AdminTransaction {
	fn track_binding_created(&mut self, binding: Binding) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(binding),
			op: Create,
		};
		self.changes.add_binding_change(change);
		Ok(())
	}

	fn track_binding_deleted(&mut self, binding: Binding) -> Result<()> {
		let change = Change {
			pre: Some(binding),
			post: None,
			op: Delete,
		};
		self.changes.add_binding_change(change);
		Ok(())
	}
}

impl TransactionalBindingChanges for AdminTransaction {
	fn find_binding(&self, id: BindingId) -> Option<&Binding> {
		for change in self.changes.binding.iter().rev() {
			if let Some(binding) = &change.post {
				if binding.id == id {
					return Some(binding);
				}
			} else if let Some(binding) = &change.pre
				&& binding.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_binding_deleted(&self, id: BindingId) -> bool {
		self.changes
			.binding
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|b| b.id) == Some(id))
	}
}
