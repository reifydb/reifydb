// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSumTypeChangeOperations, id::NamespaceId, sumtype::SumType,
};
use reifydb_type::{Result, value::sumtype::SumTypeId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSumTypeChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackSumTypeChangeOperations for AdminTransaction {
	fn track_sumtype_created(&mut self, sumtype: SumType) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(sumtype),
			op: Create,
		};
		self.changes.add_sumtype_change(change);
		Ok(())
	}

	fn track_sumtype_updated(&mut self, pre: SumType, post: SumType) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_sumtype_change(change);
		Ok(())
	}

	fn track_sumtype_deleted(&mut self, sumtype: SumType) -> Result<()> {
		let change = Change {
			pre: Some(sumtype),
			post: None,
			op: Delete,
		};
		self.changes.add_sumtype_change(change);
		Ok(())
	}
}

impl TransactionalSumTypeChanges for AdminTransaction {
	fn find_sumtype(&self, id: SumTypeId) -> Option<&SumType> {
		for change in self.changes.sumtype.iter().rev() {
			if let Some(def) = &change.post {
				if def.id == id {
					return Some(def);
				}
			} else if let Some(def) = &change.pre
				&& def.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_sumtype_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SumType> {
		self.changes
			.sumtype
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|d| d.namespace == namespace && d.name == name))
	}

	fn is_sumtype_deleted(&self, id: SumTypeId) -> bool {
		self.changes
			.sumtype
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|d| d.id) == Some(id))
	}

	fn is_sumtype_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.sumtype.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|d| d.namespace == namespace && d.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackSumTypeChangeOperations for SubscriptionTransaction {
	fn track_sumtype_created(&mut self, sumtype: SumType) -> Result<()> {
		self.inner.track_sumtype_created(sumtype)
	}

	fn track_sumtype_updated(&mut self, pre: SumType, post: SumType) -> Result<()> {
		self.inner.track_sumtype_updated(pre, post)
	}

	fn track_sumtype_deleted(&mut self, sumtype: SumType) -> Result<()> {
		self.inner.track_sumtype_deleted(sumtype)
	}
}

impl TransactionalSumTypeChanges for SubscriptionTransaction {
	fn find_sumtype(&self, id: SumTypeId) -> Option<&SumType> {
		self.inner.find_sumtype(id)
	}

	fn find_sumtype_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SumType> {
		self.inner.find_sumtype_by_name(namespace, name)
	}

	fn is_sumtype_deleted(&self, id: SumTypeId) -> bool {
		self.inner.is_sumtype_deleted(id)
	}

	fn is_sumtype_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_sumtype_deleted_by_name(namespace, name)
	}
}
