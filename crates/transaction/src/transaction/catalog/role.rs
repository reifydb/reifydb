// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackRoleChangeOperations,
	user::{RoleDef, RoleId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalRoleChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackRoleChangeOperations for AdminTransaction {
	fn track_role_def_created(&mut self, role: RoleDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(role),
			op: Create,
		};
		self.changes.add_role_def_change(change);
		Ok(())
	}

	fn track_role_def_updated(&mut self, pre: RoleDef, post: RoleDef) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_role_def_change(change);
		Ok(())
	}

	fn track_role_def_deleted(&mut self, role: RoleDef) -> Result<()> {
		let change = Change {
			pre: Some(role),
			post: None,
			op: Delete,
		};
		self.changes.add_role_def_change(change);
		Ok(())
	}
}

impl TransactionalRoleChanges for AdminTransaction {
	fn find_role(&self, id: RoleId) -> Option<&RoleDef> {
		for change in self.changes.role_def.iter().rev() {
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

	fn find_role_by_name(&self, name: &str) -> Option<&RoleDef> {
		self.changes.role_def.iter().rev().find_map(|change| change.post.as_ref().filter(|r| r.name == name))
	}

	fn is_role_deleted(&self, id: RoleId) -> bool {
		self.changes
			.role_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|r| r.id) == Some(id))
	}

	fn is_role_deleted_by_name(&self, name: &str) -> bool {
		self.changes.role_def.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|r| r.name == name).unwrap_or(false)
		})
	}
}
