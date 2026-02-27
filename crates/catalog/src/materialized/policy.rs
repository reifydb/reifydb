// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::policy::{PolicyDef, PolicyId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionPolicyDef};

impl MaterializedCatalog {
	/// Find a policy by ID at a specific version
	pub fn find_policy_at(&self, id: PolicyId, version: CommitVersion) -> Option<PolicyDef> {
		self.policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a policy by name at a specific version
	pub fn find_policy_by_name_at(&self, name: &str, version: CommitVersion) -> Option<PolicyDef> {
		self.policies_by_name.get(name).and_then(|entry| {
			let policy_id = *entry.value();
			self.find_policy_at(policy_id, version)
		})
	}

	/// Find a policy by ID (returns latest version)
	pub fn find_policy(&self, id: PolicyId) -> Option<PolicyDef> {
		self.policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_policy(&self, id: PolicyId, version: CommitVersion, policy: Option<PolicyDef>) {
		if let Some(entry) = self.policies.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				if let Some(name) = &pre.name {
					self.policies_by_name.remove(name);
				}
			}
		}

		let multi = self.policies.get_or_insert_with(id, MultiVersionPolicyDef::new);
		if let Some(new) = policy {
			if let Some(name) = &new.name {
				self.policies_by_name.insert(name.clone(), id);
			}
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
