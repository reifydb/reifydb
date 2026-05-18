// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::policy::{Policy, PolicyId, PolicyOperation},
};

use crate::cache::{CatalogCache, MultiVersionPolicy};

impl CatalogCache {
	pub fn list_all_policies(&self) -> Vec<Policy> {
		self.policies.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	pub fn list_all_policies_at(&self, version: CommitVersion) -> Vec<Policy> {
		self.policies.iter().filter_map(|entry| entry.value().get(version)).collect()
	}

	pub fn list_policy_operations(&self, policy_id: PolicyId) -> Option<Vec<PolicyOperation>> {
		self.policy_operations.get(&policy_id).map(|entry| entry.value().clone())
	}

	pub fn set_policy_operations(&self, policy_id: PolicyId, ops: Vec<PolicyOperation>) {
		self.policy_operations.insert(policy_id, ops);
	}

	pub fn remove_policy_operations(&self, policy_id: PolicyId) {
		self.policy_operations.remove(&policy_id);
	}

	pub fn find_policy_at(&self, id: PolicyId, version: CommitVersion) -> Option<Policy> {
		self.policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_policy_by_name_at(&self, name: &str, version: CommitVersion) -> Option<Policy> {
		self.policies_by_name.get(name).and_then(|entry| {
			let policy_id = *entry.value();
			self.find_policy_at(policy_id, version)
		})
	}

	pub fn find_policy(&self, id: PolicyId) -> Option<Policy> {
		self.policies.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn set_policy(&self, id: PolicyId, version: CommitVersion, policy: Option<Policy>) {
		if let Some(entry) = self.policies.get(&id)
			&& let Some(pre) = entry.value().get_latest()
			&& let Some(name) = &pre.name
		{
			self.policies_by_name.remove(name);
		}

		let multi = self.policies.get_or_insert_with(id, MultiVersionPolicy::new);
		if let Some(new) = policy {
			if let Some(name) = &new.name {
				self.policies_by_name.insert(name.clone(), id);
			}
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
			self.policy_operations.remove(&id);
		}
	}
}
