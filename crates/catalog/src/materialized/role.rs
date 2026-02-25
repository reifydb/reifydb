// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::user::{RoleDef, RoleId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionRoleDef};

impl MaterializedCatalog {
	/// Find a role by ID at a specific version
	pub fn find_role_at(&self, id: RoleId, version: CommitVersion) -> Option<RoleDef> {
		self.roles.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a role by name at a specific version
	pub fn find_role_by_name_at(&self, name: &str, version: CommitVersion) -> Option<RoleDef> {
		self.roles_by_name.get(name).and_then(|entry| {
			let role_id = *entry.value();
			self.find_role_at(role_id, version)
		})
	}

	/// Find a role by ID (returns latest version)
	pub fn find_role(&self, id: RoleId) -> Option<RoleDef> {
		self.roles.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_role(&self, id: RoleId, version: CommitVersion, role: Option<RoleDef>) {
		if let Some(entry) = self.roles.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				self.roles_by_name.remove(&pre.name);
			}
		}

		let multi = self.roles.get_or_insert_with(id, MultiVersionRoleDef::new);
		if let Some(new) = role {
			self.roles_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
