// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	authentication::{AuthenticationDef, AuthenticationId},
	change::CatalogTrackAuthenticationChangeOperations,
	user::UserId,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalAuthenticationChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackAuthenticationChangeOperations for AdminTransaction {
	fn track_authentication_def_created(&mut self, auth: AuthenticationDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(auth),
			op: Create,
		};
		self.changes.add_authentication_def_change(change);
		Ok(())
	}

	fn track_authentication_def_deleted(&mut self, auth: AuthenticationDef) -> Result<()> {
		let change = Change {
			pre: Some(auth),
			post: None,
			op: Delete,
		};
		self.changes.add_authentication_def_change(change);
		Ok(())
	}
}

impl TransactionalAuthenticationChanges for AdminTransaction {
	fn find_authentication(&self, id: AuthenticationId) -> Option<&AuthenticationDef> {
		for change in self.changes.authentication_def.iter().rev() {
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

	fn find_authentication_by_user_and_method(&self, user_id: UserId, method: &str) -> Option<&AuthenticationDef> {
		self.changes
			.authentication_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|a| a.user_id == user_id && a.method == method))
	}

	fn is_authentication_deleted(&self, id: AuthenticationId) -> bool {
		self.changes
			.authentication_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|a| a.id) == Some(id))
	}
}

impl CatalogTrackAuthenticationChangeOperations for SubscriptionTransaction {
	fn track_authentication_def_created(&mut self, auth: AuthenticationDef) -> Result<()> {
		self.inner.track_authentication_def_created(auth)
	}

	fn track_authentication_def_deleted(&mut self, auth: AuthenticationDef) -> Result<()> {
		self.inner.track_authentication_def_deleted(auth)
	}
}

impl TransactionalAuthenticationChanges for SubscriptionTransaction {
	fn find_authentication(&self, id: AuthenticationId) -> Option<&AuthenticationDef> {
		self.inner.find_authentication(id)
	}

	fn find_authentication_by_user_and_method(&self, user_id: UserId, method: &str) -> Option<&AuthenticationDef> {
		self.inner.find_authentication_by_user_and_method(user_id, method)
	}

	fn is_authentication_deleted(&self, id: AuthenticationId) -> bool {
		self.inner.is_authentication_deleted(id)
	}
}
