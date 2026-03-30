// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	authentication::{Authentication, AuthenticationId},
	change::CatalogTrackAuthenticationChangeOperations,
};
use reifydb_type::{Result, value::identity::IdentityId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalAuthenticationChanges,
	},
	interceptor::authentication::{AuthenticationPostCreateContext, AuthenticationPreDeleteContext},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackAuthenticationChangeOperations for AdminTransaction {
	fn track_authentication_created(&mut self, auth: Authentication) -> Result<()> {
		self.interceptors.authentication_post_create.execute(AuthenticationPostCreateContext::new(&auth))?;
		let change = Change {
			pre: None,
			post: Some(auth),
			op: Create,
		};
		self.changes.add_authentication_change(change);
		Ok(())
	}

	fn track_authentication_deleted(&mut self, auth: Authentication) -> Result<()> {
		self.interceptors.authentication_pre_delete.execute(AuthenticationPreDeleteContext::new(&auth))?;
		let change = Change {
			pre: Some(auth),
			post: None,
			op: Delete,
		};
		self.changes.add_authentication_change(change);
		Ok(())
	}
}

impl TransactionalAuthenticationChanges for AdminTransaction {
	fn find_authentication(&self, id: AuthenticationId) -> Option<&Authentication> {
		for change in self.changes.authentication.iter().rev() {
			if let Some(auth) = &change.post {
				if auth.id == id {
					return Some(auth);
				}
			} else if let Some(auth) = &change.pre
				&& auth.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_authentication_by_identity_and_method(
		&self,
		identity: IdentityId,
		method: &str,
	) -> Option<&Authentication> {
		self.changes.authentication.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|a| a.identity == identity && a.method == method)
		})
	}

	fn is_authentication_deleted(&self, id: AuthenticationId) -> bool {
		self.changes
			.authentication
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|a| a.id) == Some(id))
	}
}

impl CatalogTrackAuthenticationChangeOperations for SubscriptionTransaction {
	fn track_authentication_created(&mut self, auth: Authentication) -> Result<()> {
		self.inner.track_authentication_created(auth)
	}

	fn track_authentication_deleted(&mut self, auth: Authentication) -> Result<()> {
		self.inner.track_authentication_deleted(auth)
	}
}

impl TransactionalAuthenticationChanges for SubscriptionTransaction {
	fn find_authentication(&self, id: AuthenticationId) -> Option<&Authentication> {
		self.inner.find_authentication(id)
	}

	fn find_authentication_by_identity_and_method(
		&self,
		identity: IdentityId,
		method: &str,
	) -> Option<&Authentication> {
		self.inner.find_authentication_by_identity_and_method(identity, method)
	}

	fn is_authentication_deleted(&self, id: AuthenticationId) -> bool {
		self.inner.is_authentication_deleted(id)
	}
}
