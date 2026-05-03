// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::Procedure,
	},
};
use reifydb_type::value::sumtype::VariantRef;

use crate::materialized::{MaterializedCatalog, MultiVersionProcedure};

impl MaterializedCatalog {
	pub fn find_procedure_at(&self, procedure: ProcedureId, version: CommitVersion) -> Option<Procedure> {
		self.procedures.get(&procedure).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_procedure_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Procedure> {
		self.procedures_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let procedure_id = *entry.value();
			self.find_procedure_at(procedure_id, version)
		})
	}

	pub fn find_procedure(&self, procedure: ProcedureId) -> Option<Procedure> {
		self.procedures.get(&procedure).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Procedure> {
		self.procedures_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let procedure_id = *entry.value();
			self.find_procedure(procedure_id)
		})
	}

	pub fn list_procedures_for_variant_at(&self, variant: VariantRef, version: CommitVersion) -> Vec<Procedure> {
		if let Some(entry) = self.procedures_by_variant.get(&variant) {
			entry.value().iter().filter_map(|id| self.find_procedure_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn set_procedure(&self, id: ProcedureId, version: CommitVersion, procedure: Option<Procedure>) {
		if let Some(entry) = self.procedures.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.procedures_by_name.remove(&(pre.namespace(), pre.name().to_string()));

			if let Some(variant) = pre.event_variant()
				&& let Some(ids_entry) = self.procedures_by_variant.get(&variant)
			{
				let mut ids = ids_entry.value().clone();
				ids.retain(|existing| *existing != id);
				drop(ids_entry);
				self.procedures_by_variant.insert(variant, ids);
			}
		}

		let multi = self.procedures.get_or_insert_with(id, MultiVersionProcedure::new);
		if let Some(new) = procedure {
			self.procedures_by_name.insert((new.namespace(), new.name().to_string()), id);

			if let Some(variant) = new.event_variant() {
				if let Some(entry) = self.procedures_by_variant.get(&variant) {
					let mut ids = entry.value().clone();
					if !ids.contains(&id) {
						ids.push(id);
					}
					drop(entry);
					self.procedures_by_variant.insert(variant, ids);
				} else {
					self.procedures_by_variant.insert(variant, vec![id]);
				}
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
