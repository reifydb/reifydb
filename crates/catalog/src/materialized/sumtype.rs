// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
};
use reifydb_type::value::sumtype::SumTypeId;

use crate::materialized::{MaterializedCatalog, MultiVersionSumTypeDef};

impl MaterializedCatalog {
	pub fn find_sumtype_at(&self, id: SumTypeId, version: CommitVersion) -> Option<SumTypeDef> {
		self.sumtypes.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_sumtype_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<SumTypeDef> {
		self.sumtypes_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let id = *entry.value();
			self.find_sumtype_at(id, version)
		})
	}

	pub fn find_sumtype(&self, id: SumTypeId) -> Option<SumTypeDef> {
		self.sumtypes.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_sumtype_by_name(&self, namespace: NamespaceId, name: &str) -> Option<SumTypeDef> {
		self.sumtypes_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let id = *entry.value();
			self.find_sumtype(id)
		})
	}

	pub fn set_sumtype(&self, id: SumTypeId, version: CommitVersion, def: Option<SumTypeDef>) {
		if let Some(entry) = self.sumtypes.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				self.sumtypes_by_name.remove(&(pre.namespace, pre.name.clone()));
			}
		}

		let multi = self.sumtypes.get_or_insert_with(id, MultiVersionSumTypeDef::new);
		if let Some(new) = def {
			self.sumtypes_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::sumtype::SumTypeKind;

	use super::*;

	fn create_test_sumtype(id: SumTypeId, namespace: NamespaceId, name: &str) -> SumTypeDef {
		SumTypeDef {
			id,
			namespace,
			name: name.to_string(),
			variants: vec![],
			kind: SumTypeKind::Enum,
		}
	}

	#[test]
	fn test_set_and_find_sumtype() {
		let catalog = MaterializedCatalog::new();
		let id = SumTypeId(1);
		let namespace = NamespaceId(1);
		let def = create_test_sumtype(id, namespace, "Status");

		catalog.set_sumtype(id, CommitVersion(1), Some(def.clone()));

		let found = catalog.find_sumtype_at(id, CommitVersion(1));
		assert_eq!(found, Some(def.clone()));

		let found = catalog.find_sumtype_at(id, CommitVersion(5));
		assert_eq!(found, Some(def));

		let found = catalog.find_sumtype_at(id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_sumtype_by_name() {
		let catalog = MaterializedCatalog::new();
		let id = SumTypeId(1);
		let namespace = NamespaceId(1);
		let def = create_test_sumtype(id, namespace, "Direction");

		catalog.set_sumtype(id, CommitVersion(1), Some(def.clone()));

		let found = catalog.find_sumtype_by_name_at(namespace, "Direction", CommitVersion(1));
		assert_eq!(found, Some(def));

		let found = catalog.find_sumtype_by_name_at(namespace, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		let found = catalog.find_sumtype_by_name_at(NamespaceId(2), "Direction", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_sumtype_deletion() {
		let catalog = MaterializedCatalog::new();
		let id = SumTypeId(1);
		let namespace = NamespaceId(1);
		let def = create_test_sumtype(id, namespace, "Deletable");

		catalog.set_sumtype(id, CommitVersion(1), Some(def.clone()));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(1)), Some(def.clone()));
		assert!(catalog.find_sumtype_by_name_at(namespace, "Deletable", CommitVersion(1)).is_some());

		catalog.set_sumtype(id, CommitVersion(2), None);

		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(2)), None);
		assert!(catalog.find_sumtype_by_name_at(namespace, "Deletable", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(1)), Some(def));
	}

	#[test]
	fn test_sumtype_versioning() {
		let catalog = MaterializedCatalog::new();
		let id = SumTypeId(1);
		let namespace = NamespaceId(1);

		let v1 = create_test_sumtype(id, namespace, "v1");
		let mut v2 = v1.clone();
		v2.name = "v2".to_string();
		let mut v3 = v2.clone();
		v3.name = "v3".to_string();

		catalog.set_sumtype(id, CommitVersion(10), Some(v1.clone()));
		catalog.set_sumtype(id, CommitVersion(20), Some(v2.clone()));
		catalog.set_sumtype(id, CommitVersion(30), Some(v3.clone()));

		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(5)), None);
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(10)), Some(v1.clone()));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(15)), Some(v1));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(20)), Some(v2.clone()));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(25)), Some(v2));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(30)), Some(v3.clone()));
		assert_eq!(catalog.find_sumtype_at(id, CommitVersion(100)), Some(v3));
	}
}
