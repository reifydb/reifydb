// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{ProcedureDef, ProcedureTrigger},
	},
};
use reifydb_type::value::sumtype::SumTypeId;

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

	/// List all procedures for a specific event variant at a specific version
	pub fn list_procedures_for_variant_at(
		&self,
		sumtype_id: SumTypeId,
		variant_tag: u8,
		version: CommitVersion,
	) -> Vec<ProcedureDef> {
		let key = (sumtype_id, variant_tag);
		if let Some(entry) = self.procedures_by_variant.get(&key) {
			entry.value().iter().filter_map(|id| self.find_procedure_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn set_procedure(&self, id: ProcedureId, version: CommitVersion, procedure: Option<ProcedureDef>) {
		if let Some(entry) = self.procedures.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.procedures_by_name.remove(&(pre.namespace, pre.name.clone()));

				// Remove from variant index if it had an event binding
				if let ProcedureTrigger::Event {
					sumtype_id,
					variant_tag,
				} = &pre.trigger
				{
					let variant_key = (*sumtype_id, *variant_tag);
					if let Some(ids_entry) = self.procedures_by_variant.get(&variant_key) {
						let mut ids = ids_entry.value().clone();
						ids.retain(|existing| *existing != id);
						drop(ids_entry);
						self.procedures_by_variant.insert(variant_key, ids);
					}
				}
			}
		}

		let multi = self.procedures.get_or_insert_with(id, MultiVersionProcedureDef::new);
		if let Some(new) = procedure {
			self.procedures_by_name.insert((new.namespace, new.name.clone()), id);

			// Add to variant index if it has an event binding
			if let ProcedureTrigger::Event {
				sumtype_id,
				variant_tag,
			} = &new.trigger
			{
				let variant_key = (*sumtype_id, *variant_tag);
				if let Some(entry) = self.procedures_by_variant.get(&variant_key) {
					let mut ids = entry.value().clone();
					if !ids.contains(&id) {
						ids.push(id);
					}
					drop(entry);
					self.procedures_by_variant.insert(variant_key, ids);
				} else {
					self.procedures_by_variant.insert(variant_key, vec![id]);
				}
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
