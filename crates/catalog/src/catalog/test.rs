// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackTestChangeOperations,
	id::{NamespaceId, TestId},
	test::TestDef,
};
use reifydb_transaction::{
	change::TransactionalTestChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{Result, catalog::Catalog, store::sequence::system::SystemSequence};

/// Test creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct TestToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub cases: Option<String>,
	pub body: String,
}

impl Catalog {
	#[instrument(name = "catalog::test::find", level = "trace", skip(self, txn))]
	pub fn find_test(&self, txn: &mut Transaction<'_>, id: TestId) -> Result<Option<TestDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(test) = self.materialized.find_test_at(id, cmd.version()) {
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(test) = TransactionalTestChanges::find_test(admin, id) {
					return Ok(Some(test.clone()));
				}
				if TransactionalTestChanges::is_test_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(test) = self.materialized.find_test_at(id, admin.version()) {
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(test) = self.materialized.find_test_at(id, qry.version()) {
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Subscription(sub) => {
				if let Some(test) = TransactionalTestChanges::find_test(sub, id) {
					return Ok(Some(test.clone()));
				}
				if TransactionalTestChanges::is_test_deleted(sub, id) {
					return Ok(None);
				}
				if let Some(test) = self.materialized.find_test_at(id, sub.version()) {
					return Ok(Some(test));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::test::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_test_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<TestDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(test) =
					self.materialized.find_test_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(test) = TransactionalTestChanges::find_test_by_name(admin, namespace, name)
				{
					return Ok(Some(test.clone()));
				}
				if TransactionalTestChanges::is_test_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}
				if let Some(test) =
					self.materialized.find_test_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(test) =
					self.materialized.find_test_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(test));
				}
				Ok(None)
			}
			Transaction::Subscription(sub) => {
				if let Some(test) = TransactionalTestChanges::find_test_by_name(sub, namespace, name) {
					return Ok(Some(test.clone()));
				}
				if TransactionalTestChanges::is_test_deleted_by_name(sub, namespace, name) {
					return Ok(None);
				}
				if let Some(test) =
					self.materialized.find_test_by_name_at(namespace, name, sub.version())
				{
					return Ok(Some(test));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::test::list_in_namespace", level = "trace", skip(self, txn))]
	pub fn list_tests_in_namespace(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
	) -> Result<Vec<TestDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_tests_in_namespace_at(namespace, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut tests =
					self.materialized.list_tests_in_namespace_at(namespace, admin.version());
				// Add transactional additions
				for change in &admin.changes.test_def {
					if let Some(t) = &change.post {
						if t.namespace == namespace
							&& !tests.iter().any(|existing| existing.id == t.id)
						{
							tests.push(t.clone());
						}
					}
				}
				Ok(tests)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_tests_in_namespace_at(namespace, qry.version()))
			}
			Transaction::Subscription(sub) => {
				let mut tests = self.materialized.list_tests_in_namespace_at(namespace, sub.version());
				// Add transactional additions
				for change in &sub.as_admin_mut().changes.test_def {
					if let Some(t) = &change.post {
						if t.namespace == namespace
							&& !tests.iter().any(|existing| existing.id == t.id)
						{
							tests.push(t.clone());
						}
					}
				}
				Ok(tests)
			}
		}
	}

	#[instrument(name = "catalog::test::list_all", level = "trace", skip(self, txn))]
	pub fn list_all_tests(&self, txn: &mut Transaction<'_>) -> Result<Vec<TestDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.materialized.list_all_tests_at(cmd.version())),
			Transaction::Admin(admin) => {
				let mut tests = self.materialized.list_all_tests_at(admin.version());
				for change in &admin.changes.test_def {
					if let Some(t) = &change.post {
						if !tests.iter().any(|existing| existing.id == t.id) {
							tests.push(t.clone());
						}
					}
				}
				Ok(tests)
			}
			Transaction::Query(qry) => Ok(self.materialized.list_all_tests_at(qry.version())),
			Transaction::Subscription(sub) => {
				let mut tests = self.materialized.list_all_tests_at(sub.version());
				for change in &sub.as_admin_mut().changes.test_def {
					if let Some(t) = &change.post {
						if !tests.iter().any(|existing| existing.id == t.id) {
							tests.push(t.clone());
						}
					}
				}
				Ok(tests)
			}
		}
	}

	#[instrument(name = "catalog::test::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_test(&self, txn: &mut AdminTransaction, to_create: TestToCreate) -> Result<TestDef> {
		let id = SystemSequence::next_test_id(txn)?;

		let test = TestDef {
			id,
			namespace: to_create.namespace,
			name: to_create.name.text().to_string(),
			cases: to_create.cases,
			body: to_create.body,
		};

		txn.track_test_def_created(test.clone())?;

		Ok(test)
	}
}
