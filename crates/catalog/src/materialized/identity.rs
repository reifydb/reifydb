// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::identity::Identity};
use reifydb_type::value::identity::IdentityId;

use crate::materialized::{MaterializedCatalog, MultiVersionIdentity};

impl MaterializedCatalog {
	/// Find an identity by IdentityId at a specific version
	pub fn find_identity_at(&self, id: IdentityId, version: CommitVersion) -> Option<Identity> {
		self.identities.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find an identity by name at a specific version
	pub fn find_identity_by_name_at(&self, name: &str, version: CommitVersion) -> Option<Identity> {
		self.identities_by_name.get(name).and_then(|entry| {
			let identity_id = *entry.value();
			self.find_identity_at(identity_id, version)
		})
	}

	/// Find an identity by IdentityId (returns latest version)
	pub fn find_identity(&self, id: IdentityId) -> Option<Identity> {
		self.identities.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_identity(&self, id: IdentityId, version: CommitVersion, ident: Option<Identity>) {
		if let Some(entry) = self.identities.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.identities_by_name.remove(&pre.name);
		}

		let multi = self.identities.get_or_insert_with(id, MultiVersionIdentity::new);
		if let Some(new) = ident {
			self.identities_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
