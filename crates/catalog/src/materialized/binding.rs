// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		binding::Binding,
		id::{BindingId, ProcedureId},
	},
};

use super::{MaterializedCatalog, MultiVersionBinding};

impl MaterializedCatalog {
	pub fn find_binding_at(&self, id: BindingId, version: CommitVersion) -> Option<Binding> {
		self.bindings.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_binding(&self, id: BindingId) -> Option<Binding> {
		self.bindings.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn list_bindings_for_procedure_at(
		&self,
		procedure_id: ProcedureId,
		version: CommitVersion,
	) -> Vec<Binding> {
		if let Some(entry) = self.bindings_by_procedure.get(&procedure_id) {
			entry.value().iter().filter_map(|id| self.find_binding_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn set_binding(&self, id: BindingId, version: CommitVersion, binding: Option<Binding>) {
		if let Some(entry) = self.bindings.get(&id)
			&& let Some(pre) = entry.value().get_latest()
			&& let Some(ids_entry) = self.bindings_by_procedure.get(&pre.procedure_id)
		{
			let mut ids = ids_entry.value().clone();
			ids.retain(|existing| *existing != id);
			drop(ids_entry);
			if ids.is_empty() {
				self.bindings_by_procedure.remove(&pre.procedure_id);
			} else {
				self.bindings_by_procedure.insert(pre.procedure_id, ids);
			}
		}

		let multi = self.bindings.get_or_insert_with(id, MultiVersionBinding::new);
		if let Some(new) = binding {
			if let Some(entry) = self.bindings_by_procedure.get(&new.procedure_id) {
				let mut ids = entry.value().clone();
				if !ids.contains(&id) {
					ids.push(id);
				}
				drop(entry);
				self.bindings_by_procedure.insert(new.procedure_id, ids);
			} else {
				self.bindings_by_procedure.insert(new.procedure_id, vec![id]);
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
