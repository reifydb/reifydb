// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	authentication_method::{AuthenticationMethod, AuthenticationMethodId},
	change::CatalogTrackAuthenticationMethodChangeOperations,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalAuthenticationMethodChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackAuthenticationMethodChangeOperations for AdminTransaction {
	fn track_authentication_method_created(&mut self, method: AuthenticationMethod) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(method),
			op: Create,
		};
		self.changes.add_authentication_method_change(change);
		Ok(())
	}

	fn track_authentication_method_updated(
		&mut self,
		pre: AuthenticationMethod,
		post: AuthenticationMethod,
	) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_authentication_method_change(change);
		Ok(())
	}

	fn track_authentication_method_deleted(&mut self, method: AuthenticationMethod) -> Result<()> {
		let change = Change {
			pre: Some(method),
			post: None,
			op: Delete,
		};
		self.changes.add_authentication_method_change(change);
		Ok(())
	}
}

impl TransactionalAuthenticationMethodChanges for AdminTransaction {
	fn find_authentication_method(&self, id: AuthenticationMethodId) -> Option<&AuthenticationMethod> {
		for change in self.changes.authentication_method.iter().rev() {
			if let Some(method) = &change.post {
				if method.id == id {
					return Some(method);
				}
			} else if let Some(method) = &change.pre
				&& method.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_authentication_method_by_name(&self, name: &str) -> Option<&AuthenticationMethod> {
		for change in self.changes.authentication_method.iter().rev() {
			if let Some(method) = &change.post {
				if method.name == name {
					return Some(method);
				}
			} else if let Some(method) = &change.pre
				&& method.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_authentication_method_deleted_by_name(&self, name: &str) -> bool {
		self.changes.authentication_method.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|m| m.name == name).unwrap_or(false)
		})
	}
}
