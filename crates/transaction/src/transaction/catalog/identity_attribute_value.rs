// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_core::interface::catalog::{
	change::CatalogTrackIdentityAttributeValueChangeOperations,
	identity::{IdentityAttributeId, IdentityAttributeValue},
};
use reifydb_value::{Result, value::identity::IdentityId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalIdentityAttributeValueChanges,
	},
	interceptor::identity_attribute_value::{
		IdentityAttributeValuePostCreateContext, IdentityAttributeValuePreDeleteContext,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackIdentityAttributeValueChangeOperations for AdminTransaction {
	fn track_identity_attribute_value_created(&mut self, value: IdentityAttributeValue) -> Result<()> {
		self.interceptors
			.identity_attribute_value_post_create
			.execute(IdentityAttributeValuePostCreateContext::new(&value))?;
		let change = Change {
			pre: None,
			post: Some(value),
			op: Create,
		};
		self.changes.add_identity_attribute_value_change(change);
		Ok(())
	}

	fn track_identity_attribute_value_deleted(&mut self, value: IdentityAttributeValue) -> Result<()> {
		self.interceptors
			.identity_attribute_value_pre_delete
			.execute(IdentityAttributeValuePreDeleteContext::new(&value))?;
		let change = Change {
			pre: Some(value),
			post: None,
			op: Delete,
		};
		self.changes.add_identity_attribute_value_change(change);
		Ok(())
	}
}

impl TransactionalIdentityAttributeValueChanges for AdminTransaction {
	fn find_identity_attribute_value(
		&self,
		identity: IdentityId,
		attribute: IdentityAttributeId,
	) -> Option<&IdentityAttributeValue> {
		for change in self.changes.identity_attribute_value.iter().rev() {
			if let Some(v) = &change.post {
				if v.identity == identity && v.attribute == attribute {
					return Some(v);
				}
			} else if let Some(v) = &change.pre
				&& v.identity == identity && v.attribute == attribute
				&& change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_identity_attribute_values_for_identity(&self, identity: IdentityId) -> Vec<&IdentityAttributeValue> {
		let mut result = Vec::new();
		let mut seen = HashSet::new();
		for change in self.changes.identity_attribute_value.iter().rev() {
			let attribute = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.filter(|v| v.identity == identity)
				.map(|v| v.attribute);
			let Some(attribute) = attribute else {
				continue;
			};
			if !seen.insert(attribute) {
				continue;
			}
			if let Some(v) = &change.post {
				result.push(v);
			}
		}
		result
	}

	fn find_identity_attribute_values_for_attribute(
		&self,
		attribute: IdentityAttributeId,
	) -> Vec<&IdentityAttributeValue> {
		let mut result = Vec::new();
		let mut seen = HashSet::new();
		for change in self.changes.identity_attribute_value.iter().rev() {
			let identity = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.filter(|v| v.attribute == attribute)
				.map(|v| v.identity);
			let Some(identity) = identity else {
				continue;
			};
			if !seen.insert(identity) {
				continue;
			}
			if let Some(v) = &change.post {
				result.push(v);
			}
		}
		result
	}

	fn is_identity_attribute_value_deleted(&self, identity: IdentityId, attribute: IdentityAttributeId) -> bool {
		self.changes.identity_attribute_value.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|v| v.identity == identity && v.attribute == attribute)
					.unwrap_or(false)
		})
	}
}
