// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::identity::IdentityRoleDef};
use reifydb_type::value::identity::IdentityId;

use crate::materialized::{MaterializedCatalog, MultiVersionIdentityRoleDef};

impl MaterializedCatalog {
	/// Find an identity-role by composite key at a specific version
	pub fn find_identity_role_at(
		&self,
		identity: IdentityId,
		role: u64,
		version: CommitVersion,
	) -> Option<IdentityRoleDef> {
		self.identity_roles.get(&(identity, role)).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find all identity-roles for an identity at a specific version
	pub fn find_identity_roles_at(&self, identity: IdentityId, version: CommitVersion) -> Vec<IdentityRoleDef> {
		self.identity_roles
			.iter()
			.filter(|entry| entry.key().0 == identity)
			.filter_map(|entry| entry.value().get(version))
			.collect()
	}

	pub fn set_identity_role(
		&self,
		identity: IdentityId,
		role: u64,
		version: CommitVersion,
		identity_role: Option<IdentityRoleDef>,
	) {
		let key = (identity, role);
		let multi = self.identity_roles.get_or_insert_with(key, MultiVersionIdentityRoleDef::new);
		if let Some(new) = identity_role {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
