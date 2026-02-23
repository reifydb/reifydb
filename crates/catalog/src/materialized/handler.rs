// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		handler::HandlerDef,
		id::{HandlerId, NamespaceId},
	},
};
use reifydb_type::value::sumtype::SumTypeId;

use crate::materialized::{MaterializedCatalog, MultiVersionHandlerDef};

impl MaterializedCatalog {
	/// Find a handler by ID at a specific version
	pub fn find_handler_at(&self, handler: HandlerId, version: CommitVersion) -> Option<HandlerDef> {
		self.handlers.get(&handler).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a handler by name in a namespace at a specific version
	pub fn find_handler_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<HandlerDef> {
		self.handlers_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let handler_id = *entry.value();
			self.find_handler_at(handler_id, version)
		})
	}

	/// List all handlers for a specific event variant at a specific version
	pub fn list_handlers_for_variant_at(
		&self,
		sumtype_id: SumTypeId,
		variant_tag: u8,
		version: CommitVersion,
	) -> Vec<HandlerDef> {
		let key = (sumtype_id, variant_tag);
		if let Some(entry) = self.handlers_by_variant.get(&key) {
			entry.value().iter().filter_map(|id| self.find_handler_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn set_handler(&self, id: HandlerId, version: CommitVersion, handler: Option<HandlerDef>) {
		if let Some(entry) = self.handlers.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.handlers_by_name.remove(&(pre.namespace, pre.name.clone()));

				// Remove from variant index
				let variant_key = (pre.on_sumtype_id, pre.on_variant_tag);
				if let Some(ids_entry) = self.handlers_by_variant.get(&variant_key) {
					let mut ids = ids_entry.value().clone();
					ids.retain(|existing| *existing != id);
					drop(ids_entry);
					self.handlers_by_variant.insert(variant_key, ids);
				}
			}
		}

		let multi = self.handlers.get_or_insert_with(id, MultiVersionHandlerDef::new);
		if let Some(new) = handler {
			self.handlers_by_name.insert((new.namespace, new.name.clone()), id);

			// Add to variant index
			let variant_key = (new.on_sumtype_id, new.on_variant_tag);
			if let Some(entry) = self.handlers_by_variant.get(&variant_key) {
				let mut ids = entry.value().clone();
				if !ids.contains(&id) {
					ids.push(id);
				}
				drop(entry);
				self.handlers_by_variant.insert(variant_key, ids);
			} else {
				self.handlers_by_variant.insert(variant_key, vec![id]);
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
