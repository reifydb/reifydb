// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, TestId},
		test::Test,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionTest};

impl MaterializedCatalog {
	/// Find a test by ID at a specific version
	pub fn find_test_at(&self, test: TestId, version: CommitVersion) -> Option<Test> {
		self.tests.get(&test).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a test by name in a namespace at a specific version
	pub fn find_test_by_name_at(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Test> {
		self.tests_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let test_id = *entry.value();
			self.find_test_at(test_id, version)
		})
	}

	/// List all tests in a namespace at a specific version
	pub fn list_tests_in_namespace_at(&self, namespace: NamespaceId, version: CommitVersion) -> Vec<Test> {
		self.tests_by_name
			.iter()
			.filter(|entry| entry.key().0 == namespace)
			.filter_map(|entry| self.find_test_at(*entry.value(), version))
			.collect()
	}

	/// List all tests at a specific version
	pub fn list_all_tests_at(&self, version: CommitVersion) -> Vec<Test> {
		self.tests.iter().filter_map(|entry| entry.value().get(version)).collect()
	}

	pub fn set_test(&self, id: TestId, version: CommitVersion, test: Option<Test>) {
		if let Some(entry) = self.tests.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			// Remove old name from index
			self.tests_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.tests.get_or_insert_with(id, MultiVersionTest::new);
		if let Some(new) = test {
			self.tests_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
