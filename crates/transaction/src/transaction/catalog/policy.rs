// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackPolicyChangeOperations,
	policy::{Policy, PolicyId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalPolicyChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackPolicyChangeOperations for AdminTransaction {
	fn track_policy_created(&mut self, policy: Policy) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(policy),
			op: Create,
		};
		self.changes.add_policy_change(change);
		Ok(())
	}

	fn track_policy_updated(&mut self, pre: Policy, post: Policy) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_policy_change(change);
		Ok(())
	}

	fn track_policy_deleted(&mut self, policy: Policy) -> Result<()> {
		let change = Change {
			pre: Some(policy),
			post: None,
			op: Delete,
		};
		self.changes.add_policy_change(change);
		Ok(())
	}
}

impl TransactionalPolicyChanges for AdminTransaction {
	fn find_policy(&self, id: PolicyId) -> Option<&Policy> {
		for change in self.changes.policy.iter().rev() {
			if let Some(policy) = &change.post {
				if policy.id == id {
					return Some(policy);
				}
			} else if let Some(policy) = &change.pre {
				if policy.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_policy_by_name(&self, name: &str) -> Option<&Policy> {
		self.changes
			.policy
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|p| p.name.as_deref() == Some(name)))
	}

	fn is_policy_deleted(&self, id: PolicyId) -> bool {
		self.changes
			.policy
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|p| p.id) == Some(id))
	}

	fn is_policy_deleted_by_name(&self, name: &str) -> bool {
		self.changes.policy.iter().rev().any(|change| {
			change.op == Delete
				&& change.pre.as_ref().map(|p| p.name.as_deref() == Some(name)).unwrap_or(false)
		})
	}
}

impl CatalogTrackPolicyChangeOperations for SubscriptionTransaction {
	fn track_policy_created(&mut self, policy: Policy) -> Result<()> {
		self.inner.track_policy_created(policy)
	}

	fn track_policy_updated(&mut self, pre: Policy, post: Policy) -> Result<()> {
		self.inner.track_policy_updated(pre, post)
	}

	fn track_policy_deleted(&mut self, policy: Policy) -> Result<()> {
		self.inner.track_policy_deleted(policy)
	}
}

impl TransactionalPolicyChanges for SubscriptionTransaction {
	fn find_policy(&self, id: PolicyId) -> Option<&Policy> {
		self.inner.find_policy(id)
	}

	fn find_policy_by_name(&self, name: &str) -> Option<&Policy> {
		self.inner.find_policy_by_name(name)
	}

	fn is_policy_deleted(&self, id: PolicyId) -> bool {
		self.inner.is_policy_deleted(id)
	}

	fn is_policy_deleted_by_name(&self, name: &str) -> bool {
		self.inner.is_policy_deleted_by_name(name)
	}
}
