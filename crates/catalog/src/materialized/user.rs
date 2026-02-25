// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::user::{UserDef, UserId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionUserDef};

impl MaterializedCatalog {
	/// Find a user by ID at a specific version
	pub fn find_user_at(&self, id: UserId, version: CommitVersion) -> Option<UserDef> {
		self.users.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a user by name at a specific version
	pub fn find_user_by_name_at(&self, name: &str, version: CommitVersion) -> Option<UserDef> {
		self.users_by_name.get(name).and_then(|entry| {
			let user_id = *entry.value();
			self.find_user_at(user_id, version)
		})
	}

	/// Find a user by ID (returns latest version)
	pub fn find_user(&self, id: UserId) -> Option<UserDef> {
		self.users.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_user(&self, id: UserId, version: CommitVersion, user: Option<UserDef>) {
		if let Some(entry) = self.users.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				self.users_by_name.remove(&pre.name);
			}
		}

		let multi = self.users.get_or_insert_with(id, MultiVersionUserDef::new);
		if let Some(new) = user {
			self.users_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
