// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackUserRoleChangeOperations,
	user::{RoleId, UserId, UserRoleDef},
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalUserRoleChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackUserRoleChangeOperations for AdminTransaction {
	fn track_user_role_def_created(&mut self, user_role: UserRoleDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(user_role),
			op: Create,
		};
		self.changes.add_user_role_def_change(change);
		Ok(())
	}

	fn track_user_role_def_deleted(&mut self, user_role: UserRoleDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(user_role),
			post: None,
			op: Delete,
		};
		self.changes.add_user_role_def_change(change);
		Ok(())
	}
}

impl TransactionalUserRoleChanges for AdminTransaction {
	fn find_user_role(&self, user: UserId, role: RoleId) -> Option<&UserRoleDef> {
		for change in self.changes.user_role_def.iter().rev() {
			if let Some(ur) = &change.post {
				if ur.user_id == user && ur.role_id == role {
					return Some(ur);
				}
			} else if let Some(ur) = &change.pre {
				if ur.user_id == user && ur.role_id == role && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_user_role_deleted(&self, user: UserId, role: RoleId) -> bool {
		self.changes.user_role_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|ur| ur.user_id == user && ur.role_id == role)
					.unwrap_or(false)
		})
	}
}
