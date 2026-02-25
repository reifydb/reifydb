// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::user::{UserId, UserRoleDef},
};

use crate::materialized::{MaterializedCatalog, MultiVersionUserRoleDef};

impl MaterializedCatalog {
	/// Find a user-role by composite key at a specific version
	pub fn find_user_role_at(&self, user: UserId, role: u64, version: CommitVersion) -> Option<UserRoleDef> {
		self.user_roles.get(&(user, role)).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find all user-roles for a user at a specific version
	pub fn find_user_roles_for_user_at(&self, user: UserId, version: CommitVersion) -> Vec<UserRoleDef> {
		self.user_roles
			.iter()
			.filter(|entry| entry.key().0 == user)
			.filter_map(|entry| entry.value().get(version))
			.collect()
	}

	pub fn set_user_role(&self, user: UserId, role: u64, version: CommitVersion, user_role: Option<UserRoleDef>) {
		let key = (user, role);
		let multi = self.user_roles.get_or_insert_with(key, MultiVersionUserRoleDef::new);
		if let Some(new) = user_role {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
