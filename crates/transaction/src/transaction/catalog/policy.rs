// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackPolicyChangeOperations,
	policy::{PolicyDef, PolicyId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalPolicyChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackPolicyChangeOperations for AdminTransaction {
	fn track_policy_def_created(&mut self, policy: PolicyDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(policy),
			op: Create,
		};
		self.changes.add_policy_def_change(change);
		Ok(())
	}

	fn track_policy_def_updated(&mut self, pre: PolicyDef, post: PolicyDef) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_policy_def_change(change);
		Ok(())
	}

	fn track_policy_def_deleted(&mut self, policy: PolicyDef) -> Result<()> {
		let change = Change {
			pre: Some(policy),
			post: None,
			op: Delete,
		};
		self.changes.add_policy_def_change(change);
		Ok(())
	}
}

impl TransactionalPolicyChanges for AdminTransaction {
	fn find_policy(&self, id: PolicyId) -> Option<&PolicyDef> {
		for change in self.changes.policy_def.iter().rev() {
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

	fn find_policy_by_name(&self, name: &str) -> Option<&PolicyDef> {
		self.changes
			.policy_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|p| p.name.as_deref() == Some(name)))
	}

	fn is_policy_deleted(&self, id: PolicyId) -> bool {
		self.changes
			.policy_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|p| p.id) == Some(id))
	}

	fn is_policy_deleted_by_name(&self, name: &str) -> bool {
		self.changes.policy_def.iter().rev().any(|change| {
			change.op == Delete
				&& change.pre.as_ref().map(|p| p.name.as_deref() == Some(name)).unwrap_or(false)
		})
	}
}
