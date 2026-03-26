// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackRoleChangeOperations,
	identity::{Role, RoleId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalRoleChanges,
	},
	interceptor::role::{RolePostCreateContext, RolePostUpdateContext, RolePreDeleteContext, RolePreUpdateContext},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackRoleChangeOperations for AdminTransaction {
	fn track_role_created(&mut self, role: Role) -> Result<()> {
		self.interceptors.role_post_create.execute(RolePostCreateContext::new(&role))?;
		let change = Change {
			pre: None,
			post: Some(role),
			op: Create,
		};
		self.changes.add_role_change(change);
		Ok(())
	}

	fn track_role_updated(&mut self, pre: Role, post: Role) -> Result<()> {
		self.interceptors.role_pre_update.execute(RolePreUpdateContext::new(&pre))?;
		self.interceptors.role_post_update.execute(RolePostUpdateContext::new(&pre, &post))?;
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_role_change(change);
		Ok(())
	}

	fn track_role_deleted(&mut self, role: Role) -> Result<()> {
		self.interceptors.role_pre_delete.execute(RolePreDeleteContext::new(&role))?;
		let change = Change {
			pre: Some(role),
			post: None,
			op: Delete,
		};
		self.changes.add_role_change(change);
		Ok(())
	}
}

impl TransactionalRoleChanges for AdminTransaction {
	fn find_role(&self, id: RoleId) -> Option<&Role> {
		for change in self.changes.role.iter().rev() {
			if let Some(role) = &change.post {
				if role.id == id {
					return Some(role);
				}
			} else if let Some(role) = &change.pre {
				if role.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_role_by_name(&self, name: &str) -> Option<&Role> {
		self.changes.role.iter().rev().find_map(|change| change.post.as_ref().filter(|r| r.name == name))
	}

	fn is_role_deleted(&self, id: RoleId) -> bool {
		self.changes
			.role
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|r| r.id) == Some(id))
	}

	fn is_role_deleted_by_name(&self, name: &str) -> bool {
		self.changes.role.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|r| r.name == name).unwrap_or(false)
		})
	}
}

impl CatalogTrackRoleChangeOperations for SubscriptionTransaction {
	fn track_role_created(&mut self, role: Role) -> Result<()> {
		self.inner.track_role_created(role)
	}

	fn track_role_updated(&mut self, pre: Role, post: Role) -> Result<()> {
		self.inner.track_role_updated(pre, post)
	}

	fn track_role_deleted(&mut self, role: Role) -> Result<()> {
		self.inner.track_role_deleted(role)
	}
}

impl TransactionalRoleChanges for SubscriptionTransaction {
	fn find_role(&self, id: RoleId) -> Option<&Role> {
		self.inner.find_role(id)
	}

	fn find_role_by_name(&self, name: &str) -> Option<&Role> {
		self.inner.find_role_by_name(name)
	}

	fn is_role_deleted(&self, id: RoleId) -> bool {
		self.inner.is_role_deleted(id)
	}

	fn is_role_deleted_by_name(&self, name: &str) -> bool {
		self.inner.is_role_deleted_by_name(name)
	}
}
