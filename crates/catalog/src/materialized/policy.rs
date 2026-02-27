// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::policy::{SecurityPolicyDef, SecurityPolicyId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionSecurityPolicyDef};

impl MaterializedCatalog {
	/// Find a security policy by ID at a specific version
	pub fn find_security_policy_at(
		&self,
		id: SecurityPolicyId,
		version: CommitVersion,
	) -> Option<SecurityPolicyDef> {
		self.security_policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a security policy by name at a specific version
	pub fn find_security_policy_by_name_at(&self, name: &str, version: CommitVersion) -> Option<SecurityPolicyDef> {
		self.security_policies_by_name.get(name).and_then(|entry| {
			let policy_id = *entry.value();
			self.find_security_policy_at(policy_id, version)
		})
	}

	/// Find a security policy by ID (returns latest version)
	pub fn find_security_policy(&self, id: SecurityPolicyId) -> Option<SecurityPolicyDef> {
		self.security_policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_security_policy(
		&self,
		id: SecurityPolicyId,
		version: CommitVersion,
		policy: Option<SecurityPolicyDef>,
	) {
		if let Some(entry) = self.security_policies.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				if let Some(name) = &pre.name {
					self.security_policies_by_name.remove(name);
				}
			}
		}

		let multi = self.security_policies.get_or_insert_with(id, MultiVersionSecurityPolicyDef::new);
		if let Some(new) = policy {
			if let Some(name) = &new.name {
				self.security_policies_by_name.insert(name.clone(), id);
			}
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
