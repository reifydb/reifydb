// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackProcedureChangeOperations,
	id::{NamespaceId, ProcedureId},
	procedure::ProcedureDef,
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalProcedureChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackProcedureChangeOperations for AdminTransaction {
	fn track_procedure_def_created(&mut self, procedure: ProcedureDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(procedure),
			op: Create,
		};
		self.changes.add_procedure_def_change(change);
		Ok(())
	}

	fn track_procedure_def_updated(&mut self, pre: ProcedureDef, post: ProcedureDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_procedure_def_change(change);
		Ok(())
	}

	fn track_procedure_def_deleted(&mut self, procedure: ProcedureDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(procedure),
			post: None,
			op: Delete,
		};
		self.changes.add_procedure_def_change(change);
		Ok(())
	}
}

impl TransactionalProcedureChanges for AdminTransaction {
	fn find_procedure(&self, id: ProcedureId) -> Option<&ProcedureDef> {
		for change in self.changes.procedure_def.iter().rev() {
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

	fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&ProcedureDef> {
		self.changes
			.procedure_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|p| p.namespace == namespace && p.name == name))
	}

	fn is_procedure_deleted(&self, id: ProcedureId) -> bool {
		self.changes
			.procedure_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|p| p.id) == Some(id))
	}

	fn is_procedure_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.procedure_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|p| p.namespace == namespace && p.name == name)
					.unwrap_or(false)
		})
	}
}
