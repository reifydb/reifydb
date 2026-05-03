// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::authentication::{Authentication, AuthenticationId},
};
use reifydb_type::value::identity::IdentityId;

use crate::materialized::{MaterializedCatalog, MultiVersionAuthentication};

impl MaterializedCatalog {
	pub fn find_authentication_at(&self, id: AuthenticationId, version: CommitVersion) -> Option<Authentication> {
		self.authentications.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_authentication_by_identity_and_method_at(
		&self,
		identity: IdentityId,
		method: &str,
		version: CommitVersion,
	) -> Option<Authentication> {
		self.authentications_by_identity_method.get(&(identity, method.to_string())).and_then(|entry| {
			let auth_id = *entry.value();
			self.find_authentication_at(auth_id, version)
		})
	}

	pub fn find_authentication(&self, id: AuthenticationId) -> Option<Authentication> {
		self.authentications.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn list_authentications_by_method_at(&self, method: &str, version: CommitVersion) -> Vec<Authentication> {
		self.authentications
			.iter()
			.filter_map(|entry| entry.value().get(version))
			.filter(|a| a.method == method)
			.collect()
	}

	pub fn set_authentication(&self, id: AuthenticationId, version: CommitVersion, auth: Option<Authentication>) {
		if let Some(entry) = self.authentications.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.authentications_by_identity_method.remove(&(pre.identity, pre.method.clone()));
		}

		let multi = self.authentications.get_or_insert_with(id, MultiVersionAuthentication::new);
		if let Some(new) = auth {
			self.authentications_by_identity_method.insert((new.identity, new.method.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
