// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackIdentityRoleChangeOperations,
	identity::{IdentityRoleDef, RoleId},
};
use reifydb_type::{Result, value::identity::IdentityId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalIdentityRoleChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackIdentityRoleChangeOperations for AdminTransaction {
	fn track_identity_role_def_created(&mut self, identity_role: IdentityRoleDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(identity_role),
			op: Create,
		};
		self.changes.add_identity_role_def_change(change);
		Ok(())
	}

	fn track_identity_role_def_deleted(&mut self, identity_role: IdentityRoleDef) -> Result<()> {
		let change = Change {
			pre: Some(identity_role),
			post: None,
			op: Delete,
		};
		self.changes.add_identity_role_def_change(change);
		Ok(())
	}
}

impl TransactionalIdentityRoleChanges for AdminTransaction {
	fn find_identity_role(&self, identity: IdentityId, role: RoleId) -> Option<&IdentityRoleDef> {
		for change in self.changes.identity_role_def.iter().rev() {
			if let Some(ir) = &change.post {
				if ir.identity == identity && ir.role_id == role {
					return Some(ir);
				}
			} else if let Some(ir) = &change.pre {
				if ir.identity == identity && ir.role_id == role && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_identity_role_deleted(&self, identity: IdentityId, role: RoleId) -> bool {
		self.changes.identity_role_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|ir| ir.identity == identity && ir.role_id == role)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackIdentityRoleChangeOperations for SubscriptionTransaction {
	fn track_identity_role_def_created(&mut self, identity_role: IdentityRoleDef) -> Result<()> {
		self.inner.track_identity_role_def_created(identity_role)
	}

	fn track_identity_role_def_deleted(&mut self, identity_role: IdentityRoleDef) -> Result<()> {
		self.inner.track_identity_role_def_deleted(identity_role)
	}
}

impl TransactionalIdentityRoleChanges for SubscriptionTransaction {
	fn find_identity_role(&self, identity: IdentityId, role: RoleId) -> Option<&IdentityRoleDef> {
		self.inner.find_identity_role(identity, role)
	}

	fn is_identity_role_deleted(&self, identity: IdentityId, role: RoleId) -> bool {
		self.inner.is_identity_role_deleted(identity, role)
	}
}
