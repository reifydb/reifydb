// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackUserAuthenticationChangeOperations,
	user::UserId,
	user_authentication::{UserAuthenticationDef, UserAuthenticationId},
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalUserAuthenticationChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackUserAuthenticationChangeOperations for AdminTransaction {
	fn track_user_authentication_def_created(&mut self, auth: UserAuthenticationDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(auth),
			op: Create,
		};
		self.changes.add_user_authentication_def_change(change);
		Ok(())
	}

	fn track_user_authentication_def_deleted(&mut self, auth: UserAuthenticationDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(auth),
			post: None,
			op: Delete,
		};
		self.changes.add_user_authentication_def_change(change);
		Ok(())
	}
}

impl TransactionalUserAuthenticationChanges for AdminTransaction {
	fn find_user_authentication(&self, id: UserAuthenticationId) -> Option<&UserAuthenticationDef> {
		for change in self.changes.user_authentication_def.iter().rev() {
			if let Some(auth) = &change.post {
				if auth.id == id {
					return Some(auth);
				}
			} else if let Some(auth) = &change.pre {
				if auth.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_user_authentication_by_user_and_method(
		&self,
		user_id: UserId,
		method: &str,
	) -> Option<&UserAuthenticationDef> {
		self.changes
			.user_authentication_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|a| a.user_id == user_id && a.method == method))
	}

	fn is_user_authentication_deleted(&self, id: UserAuthenticationId) -> bool {
		self.changes
			.user_authentication_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|a| a.id) == Some(id))
	}
}
