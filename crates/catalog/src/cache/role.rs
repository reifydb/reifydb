// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::identity::{Role, RoleId},
};

use crate::cache::{CatalogCache, MultiVersionRole};

impl CatalogCache {
	pub fn find_role_at(&self, id: RoleId, version: CommitVersion) -> Option<Role> {
		self.roles.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_role_by_name_at(&self, name: &str, version: CommitVersion) -> Option<Role> {
		self.roles_by_name.get(name).and_then(|entry| {
			let role_id = *entry.value();
			self.find_role_at(role_id, version)
		})
	}

	pub fn find_role(&self, id: RoleId) -> Option<Role> {
		self.roles.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_role(&self, id: RoleId, version: CommitVersion, role: Option<Role>) {
		if let Some(entry) = self.roles.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.roles_by_name.remove(&pre.name);
		}

		let multi = self.roles.get_or_insert_with(id, MultiVersionRole::new);
		if let Some(new) = role {
			self.roles_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
