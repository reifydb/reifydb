// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackTestChangeOperations,
	id::{NamespaceId, TestId},
	test::Test,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalTestChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackTestChangeOperations for AdminTransaction {
	fn track_test_created(&mut self, test: Test) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(test),
			op: Create,
		};
		self.changes.add_test_change(change);
		Ok(())
	}

	fn track_test_deleted(&mut self, test: Test) -> Result<()> {
		let change = Change {
			pre: Some(test),
			post: None,
			op: Delete,
		};
		self.changes.add_test_change(change);
		Ok(())
	}
}

impl TransactionalTestChanges for AdminTransaction {
	fn find_test(&self, id: TestId) -> Option<&Test> {
		for change in self.changes.test.iter().rev() {
			if let Some(test) = &change.post {
				if test.id == id {
					return Some(test);
				}
			} else if let Some(test) = &change.pre {
				if test.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_test_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Test> {
		self.changes
			.test
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|t| t.namespace == namespace && t.name == name))
	}

	fn is_test_deleted(&self, id: TestId) -> bool {
		self.changes
			.test
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|t| t.id) == Some(id))
	}

	fn is_test_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.test.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|t| t.namespace == namespace && t.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackTestChangeOperations for SubscriptionTransaction {
	fn track_test_created(&mut self, test: Test) -> Result<()> {
		self.inner.track_test_created(test)
	}

	fn track_test_deleted(&mut self, test: Test) -> Result<()> {
		self.inner.track_test_deleted(test)
	}
}

impl TransactionalTestChanges for SubscriptionTransaction {
	fn find_test(&self, id: TestId) -> Option<&Test> {
		self.inner.find_test(id)
	}

	fn find_test_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Test> {
		self.inner.find_test_by_name(namespace, name)
	}

	fn is_test_deleted(&self, id: TestId) -> bool {
		self.inner.is_test_deleted(id)
	}

	fn is_test_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_test_deleted_by_name(namespace, name)
	}
}
