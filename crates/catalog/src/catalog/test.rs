// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackTestChangeOperations,
	id::{NamespaceId, TestId},
	test::Test,
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
	pub fn find_test(&self, txn: &mut Transaction<'_>, id: TestId) -> Result<Option<Test>> {
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
			Transaction::Test(t) => {
				if let Some(t) = TransactionalTestChanges::find_test(t.inner, id) {
					return Ok(Some(t.clone()));
				}
				if TransactionalTestChanges::is_test_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(t) = self.materialized.find_test_at(id, t.inner.version()) {
					return Ok(Some(t));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(test) = self.materialized.find_test_at(id, rep.version()) {
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
	) -> Result<Option<Test>> {
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
			Transaction::Test(t) => {
				if let Some(t) = TransactionalTestChanges::find_test_by_name(t.inner, namespace, name) {
					return Ok(Some(t.clone()));
				}
				if TransactionalTestChanges::is_test_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(t) =
					self.materialized.find_test_by_name_at(namespace, name, t.inner.version())
				{
					return Ok(Some(t));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(test) =
					self.materialized.find_test_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(test));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::test::list_in_namespace", level = "trace", skip(self, txn))]
	pub fn list_tests_in_namespace(&self, txn: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<Test>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_tests_in_namespace_at(namespace, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut tests =
					self.materialized.list_tests_in_namespace_at(namespace, admin.version());
				// Add transactional additions
				for change in &admin.changes.test {
					if let Some(t) = &change.post
						&& t.namespace == namespace && !tests
						.iter()
						.any(|existing| existing.id == t.id)
					{
						tests.push(t.clone());
					}
				}
				// Remove tests deleted in this transaction
				tests.retain(|t| !admin.is_test_deleted(t.id));
				Ok(tests)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_tests_in_namespace_at(namespace, qry.version()))
			}
			Transaction::Test(t) => {
				let mut tests =
					self.materialized.list_tests_in_namespace_at(namespace, t.inner.version());
				// Add transactional additions
				for change in &t.inner.changes.test {
					if let Some(tst) = &change.post
						&& tst.namespace == namespace && !tests
						.iter()
						.any(|existing| existing.id == tst.id)
					{
						tests.push(tst.clone());
					}
				}
				// Remove tests deleted in this transaction
				tests.retain(|tst| !t.inner.is_test_deleted(tst.id));
				Ok(tests)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_tests_in_namespace_at(namespace, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::test::list_all", level = "trace", skip(self, txn))]
	pub fn list_all_tests(&self, txn: &mut Transaction<'_>) -> Result<Vec<Test>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.materialized.list_all_tests_at(cmd.version())),
			Transaction::Admin(admin) => {
				let mut tests = self.materialized.list_all_tests_at(admin.version());
				for change in &admin.changes.test {
					if let Some(t) = &change.post
						&& !tests.iter().any(|existing| existing.id == t.id)
					{
						tests.push(t.clone());
					}
				}
				// Remove tests deleted in this transaction
				tests.retain(|t| !admin.is_test_deleted(t.id));
				Ok(tests)
			}
			Transaction::Query(qry) => Ok(self.materialized.list_all_tests_at(qry.version())),
			Transaction::Test(t) => {
				let mut tests = self.materialized.list_all_tests_at(t.inner.version());
				for change in &t.inner.changes.test {
					if let Some(tst) = &change.post
						&& !tests.iter().any(|existing| existing.id == tst.id)
					{
						tests.push(tst.clone());
					}
				}
				// Remove tests deleted in this transaction
				tests.retain(|tst| !t.inner.is_test_deleted(tst.id));
				Ok(tests)
			}
			Transaction::Replica(rep) => Ok(self.materialized.list_all_tests_at(rep.version())),
		}
	}

	#[instrument(name = "catalog::test::drop", level = "debug", skip(self, txn))]
	pub fn drop_test(&self, txn: &mut AdminTransaction, id: TestId) -> Result<()> {
		if let Some(test) = self.find_test(&mut Transaction::Admin(&mut *txn), id)? {
			txn.track_test_deleted(test)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::test::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_test(&self, txn: &mut AdminTransaction, to_create: TestToCreate) -> Result<Test> {
		let id = SystemSequence::next_test_id(txn)?;

		let test = Test {
			id,
			namespace: to_create.namespace,
			name: to_create.name.text().to_string(),
			cases: to_create.cases,
			body: to_create.body,
		};

		txn.track_test_created(test.clone())?;

		Ok(test)
	}
}
