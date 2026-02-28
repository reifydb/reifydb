// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackUserChangeOperations,
	user::{UserDef, UserId},
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalUserChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackUserChangeOperations for AdminTransaction {
	fn track_user_def_created(&mut self, user: UserDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(user),
			op: Create,
		};
		self.changes.add_user_def_change(change);
		Ok(())
	}

	fn track_user_def_updated(&mut self, pre: UserDef, post: UserDef) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_user_def_change(change);
		Ok(())
	}

	fn track_user_def_deleted(&mut self, user: UserDef) -> Result<()> {
		let change = Change {
			pre: Some(user),
			post: None,
			op: Delete,
		};
		self.changes.add_user_def_change(change);
		Ok(())
	}
}

impl TransactionalUserChanges for AdminTransaction {
	fn find_user(&self, id: UserId) -> Option<&UserDef> {
		for change in self.changes.user_def.iter().rev() {
			if let Some(user) = &change.post {
				if user.id == id {
					return Some(user);
				}
			} else if let Some(user) = &change.pre {
				if user.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_user_by_name(&self, name: &str) -> Option<&UserDef> {
		self.changes.user_def.iter().rev().find_map(|change| change.post.as_ref().filter(|u| u.name == name))
	}

	fn is_user_deleted(&self, id: UserId) -> bool {
		self.changes
			.user_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|u| u.id) == Some(id))
	}

	fn is_user_deleted_by_name(&self, name: &str) -> bool {
		self.changes.user_def.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|u| u.name == name).unwrap_or(false)
		})
	}
}
