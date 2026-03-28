// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackProcedureChangeOperations,
	id::{NamespaceId, ProcedureId},
	procedure::Procedure,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalProcedureChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackProcedureChangeOperations for AdminTransaction {
	fn track_procedure_created(&mut self, procedure: Procedure) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(procedure),
			op: Create,
		};
		self.changes.add_procedure_change(change);
		Ok(())
	}

	fn track_procedure_updated(&mut self, pre: Procedure, post: Procedure) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_procedure_change(change);
		Ok(())
	}

	fn track_procedure_deleted(&mut self, procedure: Procedure) -> Result<()> {
		let change = Change {
			pre: Some(procedure),
			post: None,
			op: Delete,
		};
		self.changes.add_procedure_change(change);
		Ok(())
	}
}

impl TransactionalProcedureChanges for AdminTransaction {
	fn find_procedure(&self, id: ProcedureId) -> Option<&Procedure> {
		for change in self.changes.procedure.iter().rev() {
			if let Some(procedure) = &change.post {
				if procedure.id == id {
					return Some(procedure);
				}
			} else if let Some(procedure) = &change.pre {
				if procedure.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Procedure> {
		self.changes
			.procedure
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|p| p.namespace == namespace && p.name == name))
	}

	fn is_procedure_deleted(&self, id: ProcedureId) -> bool {
		self.changes
			.procedure
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|p| p.id) == Some(id))
	}

	fn is_procedure_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.procedure.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|p| p.namespace == namespace && p.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackProcedureChangeOperations for SubscriptionTransaction {
	fn track_procedure_created(&mut self, procedure: Procedure) -> Result<()> {
		self.inner.track_procedure_created(procedure)
	}

	fn track_procedure_updated(&mut self, pre: Procedure, post: Procedure) -> Result<()> {
		self.inner.track_procedure_updated(pre, post)
	}

	fn track_procedure_deleted(&mut self, procedure: Procedure) -> Result<()> {
		self.inner.track_procedure_deleted(procedure)
	}
}

impl TransactionalProcedureChanges for SubscriptionTransaction {
	fn find_procedure(&self, id: ProcedureId) -> Option<&Procedure> {
		self.inner.find_procedure(id)
	}

	fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Procedure> {
		self.inner.find_procedure_by_name(namespace, name)
	}

	fn is_procedure_deleted(&self, id: ProcedureId) -> bool {
		self.inner.is_procedure_deleted(id)
	}

	fn is_procedure_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_procedure_deleted_by_name(namespace, name)
	}
}
