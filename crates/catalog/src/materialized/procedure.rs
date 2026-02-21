// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::ProcedureDef,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionProcedureDef};

impl MaterializedCatalog {
	/// Find a procedure by ID at a specific version
	pub fn find_procedure_at(&self, procedure: ProcedureId, version: CommitVersion) -> Option<ProcedureDef> {
		self.procedures.get(&procedure).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a procedure by name in a namespace at a specific version
	pub fn find_procedure_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<ProcedureDef> {
		self.procedures_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let procedure_id = *entry.value();
			self.find_procedure_at(procedure_id, version)
		})
	}

	/// Find a procedure by ID (returns latest version)
	pub fn find_procedure(&self, procedure: ProcedureId) -> Option<ProcedureDef> {
		self.procedures.get(&procedure).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Find a procedure by name in a namespace (returns latest version)
	pub fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<ProcedureDef> {
		self.procedures_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let procedure_id = *entry.value();
			self.find_procedure(procedure_id)
		})
	}

	pub fn set_procedure(&self, id: ProcedureId, version: CommitVersion, procedure: Option<ProcedureDef>) {
		if let Some(entry) = self.procedures.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.procedures_by_name.remove(&(pre.namespace, pre.name.clone()));
			}
		}

		let multi = self.procedures.get_or_insert_with(id, MultiVersionProcedureDef::new);
		if let Some(new) = procedure {
			self.procedures_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
