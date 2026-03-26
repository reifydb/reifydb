// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackGrantedRoleChangeOperations,
	identity::{GrantedRole, RoleId},
};
use reifydb_type::{Result, value::identity::IdentityId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalGrantedRoleChanges,
	},
	interceptor::granted_role::{GrantedRolePostCreateContext, GrantedRolePreDeleteContext},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackGrantedRoleChangeOperations for AdminTransaction {
	fn track_granted_role_created(&mut self, granted_role: GrantedRole) -> Result<()> {
		self.interceptors.granted_role_post_create.execute(GrantedRolePostCreateContext::new(&granted_role))?;
		let change = Change {
			pre: None,
			post: Some(granted_role),
			op: Create,
		};
		self.changes.add_granted_role_change(change);
		Ok(())
	}

	fn track_granted_role_deleted(&mut self, granted_role: GrantedRole) -> Result<()> {
		self.interceptors.granted_role_pre_delete.execute(GrantedRolePreDeleteContext::new(&granted_role))?;
		let change = Change {
			pre: Some(granted_role),
			post: None,
			op: Delete,
		};
		self.changes.add_granted_role_change(change);
		Ok(())
	}
}

impl TransactionalGrantedRoleChanges for AdminTransaction {
	fn find_granted_role(&self, identity: IdentityId, role: RoleId) -> Option<&GrantedRole> {
		for change in self.changes.granted_role.iter().rev() {
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

	fn find_granted_roles_for_identity(&self, identity: IdentityId) -> Vec<&GrantedRole> {
		let mut result = Vec::new();
		for change in &self.changes.granted_role {
			if let Some(ir) = &change.post {
				if ir.identity == identity && change.op == Create {
					result.push(ir);
				}
			}
		}
		result
	}

	fn is_granted_role_deleted(&self, identity: IdentityId, role: RoleId) -> bool {
		self.changes.granted_role.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|ir| ir.identity == identity && ir.role_id == role)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackGrantedRoleChangeOperations for SubscriptionTransaction {
	fn track_granted_role_created(&mut self, granted_role: GrantedRole) -> Result<()> {
		self.inner.track_granted_role_created(granted_role)
	}

	fn track_granted_role_deleted(&mut self, granted_role: GrantedRole) -> Result<()> {
		self.inner.track_granted_role_deleted(granted_role)
	}
}

impl TransactionalGrantedRoleChanges for SubscriptionTransaction {
	fn find_granted_role(&self, identity: IdentityId, role: RoleId) -> Option<&GrantedRole> {
		self.inner.find_granted_role(identity, role)
	}

	fn find_granted_roles_for_identity(&self, identity: IdentityId) -> Vec<&GrantedRole> {
		self.inner.find_granted_roles_for_identity(identity)
	}

	fn is_granted_role_deleted(&self, identity: IdentityId, role: RoleId) -> bool {
		self.inner.is_granted_role_deleted(identity, role)
	}
}
