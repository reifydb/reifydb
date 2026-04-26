// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, RelationshipId, TableId},
		relationship::Relationship,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionRelationship};

impl MaterializedCatalog {
	pub fn find_relationship_at(&self, id: RelationshipId, version: CommitVersion) -> Option<Relationship> {
		self.relationships.get(&id).and_then(|entry| entry.value().get(version))
	}

	pub fn find_relationship(&self, id: RelationshipId) -> Option<Relationship> {
		self.relationships.get(&id).and_then(|entry| entry.value().get_latest())
	}

	pub fn find_relationship_by_name_at(
		&self,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
		version: CommitVersion,
	) -> Option<Relationship> {
		let key = (namespace, source_table, name.to_string());
		self.relationships_by_name
			.get(&key)
			.and_then(|entry| self.find_relationship_at(*entry.value(), version))
	}

	pub fn find_relationship_by_name(
		&self,
		namespace: NamespaceId,
		source_table: TableId,
		name: &str,
	) -> Option<Relationship> {
		let key = (namespace, source_table, name.to_string());
		self.relationships_by_name.get(&key).and_then(|entry| self.find_relationship(*entry.value()))
	}

	pub fn list_relationships_from_at(&self, source_table: TableId, version: CommitVersion) -> Vec<Relationship> {
		match self.relationships_by_source.get(&source_table) {
			Some(entry) => {
				entry.value().iter().filter_map(|id| self.find_relationship_at(*id, version)).collect()
			}
			None => Vec::new(),
		}
	}

	pub fn list_relationships_from(&self, source_table: TableId) -> Vec<Relationship> {
		match self.relationships_by_source.get(&source_table) {
			Some(entry) => entry.value().iter().filter_map(|id| self.find_relationship(*id)).collect(),
			None => Vec::new(),
		}
	}

	pub fn set_relationship(&self, id: RelationshipId, version: CommitVersion, relationship: Option<Relationship>) {
		// Pre-prune indexes from the prior latest state so rename / source-move don't leak.
		if let Some(entry) = self.relationships.get(&id)
			&& let Some(prior) = entry.value().get_latest()
		{
			let name_key = (prior.namespace, prior.source_table, prior.name.clone());
			self.relationships_by_name.remove(&name_key);

			if let Some(src_entry) = self.relationships_by_source.get(&prior.source_table) {
				let new_list: Vec<RelationshipId> =
					src_entry.value().iter().copied().filter(|x| *x != id).collect();
				if new_list.is_empty() {
					self.relationships_by_source.remove(&prior.source_table);
				} else {
					self.relationships_by_source.insert(prior.source_table, new_list);
				}
			}
		}

		let multi = self.relationships.get_or_insert_with(id, MultiVersionRelationship::new);
		match relationship {
			Some(new) => {
				let name_key = (new.namespace, new.source_table, new.name.clone());
				let source_table = new.source_table;

				multi.value().insert(version, new);

				self.relationships_by_name.insert(name_key, id);

				let src_entry = self.relationships_by_source.get_or_insert_with(source_table, Vec::new);
				let mut new_list = src_entry.value().clone();
				if !new_list.contains(&id) {
					new_list.push(id);
					self.relationships_by_source.insert(source_table, new_list);
				}
			}
			None => {
				multi.value().remove(version);
			}
		}
	}
}
