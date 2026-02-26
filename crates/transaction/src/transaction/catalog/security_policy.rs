// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSecurityPolicyChangeOperations,
	security_policy::{SecurityPolicyDef, SecurityPolicyId},
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSecurityPolicyChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackSecurityPolicyChangeOperations for AdminTransaction {
	fn track_security_policy_def_created(&mut self, policy: SecurityPolicyDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(policy),
			op: Create,
		};
		self.changes.add_security_policy_def_change(change);
		Ok(())
	}

	fn track_security_policy_def_updated(
		&mut self,
		pre: SecurityPolicyDef,
		post: SecurityPolicyDef,
	) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_security_policy_def_change(change);
		Ok(())
	}

	fn track_security_policy_def_deleted(&mut self, policy: SecurityPolicyDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(policy),
			post: None,
			op: Delete,
		};
		self.changes.add_security_policy_def_change(change);
		Ok(())
	}
}

impl TransactionalSecurityPolicyChanges for AdminTransaction {
	fn find_security_policy(&self, id: SecurityPolicyId) -> Option<&SecurityPolicyDef> {
		for change in self.changes.security_policy_def.iter().rev() {
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

	fn find_security_policy_by_name(&self, name: &str) -> Option<&SecurityPolicyDef> {
		self.changes
			.security_policy_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|p| p.name.as_deref() == Some(name)))
	}

	fn is_security_policy_deleted(&self, id: SecurityPolicyId) -> bool {
		self.changes
			.security_policy_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|p| p.id) == Some(id))
	}

	fn is_security_policy_deleted_by_name(&self, name: &str) -> bool {
		self.changes.security_policy_def.iter().rev().any(|change| {
			change.op == Delete
				&& change.pre.as_ref().map(|p| p.name.as_deref() == Some(name)).unwrap_or(false)
		})
	}
}
