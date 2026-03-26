// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::identity::GrantedRole};
use reifydb_type::value::identity::IdentityId;

use crate::materialized::{MaterializedCatalog, MultiVersionGrantedRole};

impl MaterializedCatalog {
	/// Find an identity-role by composite key at a specific version
	pub fn find_granted_role_at(
		&self,
		identity: IdentityId,
		role: u64,
		version: CommitVersion,
	) -> Option<GrantedRole> {
		self.granted_roles.get(&(identity, role)).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find all identity-roles for an identity at a specific version
	pub fn find_granted_roles_at(&self, identity: IdentityId, version: CommitVersion) -> Vec<GrantedRole> {
		self.granted_roles
			.iter()
			.filter(|entry| entry.key().0 == identity)
			.filter_map(|entry| entry.value().get(version))
			.collect()
	}

	pub fn set_granted_role(
		&self,
		identity: IdentityId,
		role: u64,
		version: CommitVersion,
		granted_role: Option<GrantedRole>,
	) {
		let key = (identity, role);
		let multi = self.granted_roles.get_or_insert_with(key, MultiVersionGrantedRole::new);
		if let Some(new) = granted_role {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
