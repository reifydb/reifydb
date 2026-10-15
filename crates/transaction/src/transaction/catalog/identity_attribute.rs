// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackIdentityAttributeChangeOperations,
	identity::{IdentityAttribute, IdentityAttributeId},
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalIdentityAttributeChanges,
	},
	interceptor::identity_attribute::{IdentityAttributePostCreateContext, IdentityAttributePreDeleteContext},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackIdentityAttributeChangeOperations for AdminTransaction {
	fn track_identity_attribute_created(&mut self, attribute: IdentityAttribute) -> Result<()> {
		self.interceptors
			.identity_attribute_post_create
			.execute(IdentityAttributePostCreateContext::new(&attribute))?;
		let change = Change {
			pre: None,
			post: Some(attribute),
			op: Create,
		};
		self.changes.add_identity_attribute_change(change);
		Ok(())
	}

	fn track_identity_attribute_deleted(&mut self, attribute: IdentityAttribute) -> Result<()> {
		self.interceptors
			.identity_attribute_pre_delete
			.execute(IdentityAttributePreDeleteContext::new(&attribute))?;
		let change = Change {
			pre: Some(attribute),
			post: None,
			op: Delete,
		};
		self.changes.add_identity_attribute_change(change);
		Ok(())
	}
}

impl TransactionalIdentityAttributeChanges for AdminTransaction {
	fn find_identity_attribute(&self, id: IdentityAttributeId) -> Option<&IdentityAttribute> {
		for change in self.changes.identity_attribute.iter().rev() {
			if let Some(attribute) = &change.post {
				if attribute.id == id {
					return Some(attribute);
				}
			} else if let Some(attribute) = &change.pre
				&& attribute.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_identity_attribute_by_name(&self, name: &str) -> Option<&IdentityAttribute> {
		for change in self.changes.identity_attribute.iter().rev() {
			if let Some(attribute) = &change.post {
				if attribute.name == name {
					return Some(attribute);
				}
			} else if let Some(attribute) = &change.pre
				&& attribute.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_identity_attribute_deleted(&self, id: IdentityAttributeId) -> bool {
		self.changes
			.identity_attribute
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|a| a.id) == Some(id))
	}

	fn is_identity_attribute_deleted_by_name(&self, name: &str) -> bool {
		self.changes.identity_attribute.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|a| a.name == name).unwrap_or(false)
		})
	}
}
