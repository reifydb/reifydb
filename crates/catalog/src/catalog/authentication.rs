// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::{
	authentication::{Authentication, AuthenticationId},
	change::CatalogTrackAuthenticationChangeOperations,
};
use reifydb_transaction::{
	change::TransactionalAuthenticationChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::value::identity::IdentityId;
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::authentication::create", level = "debug", skip(self, txn))]
	pub fn create_authentication(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<Authentication> {
		let auth = CatalogStore::create_authentication(txn, identity, method, properties)?;
		txn.track_authentication_created(auth.clone())?;
		Ok(auth)
	}

	#[instrument(name = "catalog::authentication::find", level = "trace", skip(self, txn))]
	pub fn find_authentication(
		&self,
		txn: &mut Transaction<'_>,
		id: AuthenticationId,
	) -> Result<Option<Authentication>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(auth) = self.materialized.find_authentication_at(id, cmd.version()) {
					return Ok(Some(auth));
				}

				if let Some(auth) =
					CatalogStore::find_authentication(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"Authentication '{}' found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(auth) = TransactionalAuthenticationChanges::find_authentication(admin, id) {
					return Ok(Some(auth.clone()));
				}

				if TransactionalAuthenticationChanges::is_authentication_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(auth) = self.materialized.find_authentication_at(id, admin.version()) {
					return Ok(Some(auth));
				}

				if let Some(auth) =
					CatalogStore::find_authentication(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"Authentication '{}' found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(auth) = self.materialized.find_authentication_at(id, qry.version()) {
					return Ok(Some(auth));
				}

				if let Some(auth) =
					CatalogStore::find_authentication(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!(
						"Authentication '{}' found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(auth) = TransactionalAuthenticationChanges::find_authentication(t.inner, id)
				{
					return Ok(Some(auth.clone()));
				}

				if TransactionalAuthenticationChanges::is_authentication_deleted(t.inner, id) {
					return Ok(None);
				}

				if let Some(auth) = self.materialized.find_authentication_at(id, t.inner.version()) {
					return Ok(Some(auth));
				}

				if let Some(auth) =
					CatalogStore::find_authentication(&mut Transaction::Admin(&mut *t.inner), id)?
				{
					warn!(
						"Authentication '{}' found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(auth) = self.materialized.find_authentication_at(id, rep.version()) {
					return Ok(Some(auth));
				}

				if let Some(auth) =
					CatalogStore::find_authentication(&mut Transaction::Replica(&mut *rep), id)?
				{
					warn!(
						"Authentication '{}' found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::authentication::find_by_identity_and_method", level = "trace", skip(self, txn))]
	pub fn find_authentication_by_identity_and_method(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
		method: &str,
	) -> Result<Option<Authentication>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(auth) = self.materialized.find_authentication_by_identity_and_method_at(
					identity,
					method,
					cmd.version(),
				) {
					return Ok(Some(auth));
				}

				if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
					&mut Transaction::Command(&mut *cmd),
					identity,
					method,
				)? {
					warn!(
						"Authentication for identity {} method '{}' found in storage but not in MaterializedCatalog",
						identity, method
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(auth) =
					TransactionalAuthenticationChanges::find_authentication_by_identity_and_method(
						admin, identity, method,
					) {
					return Ok(Some(auth.clone()));
				}

				if TransactionalAuthenticationChanges::is_authentication_deleted_by_identity_and_method(
					admin, identity, method,
				) {
					return Ok(None);
				}

				if let Some(auth) = self.materialized.find_authentication_by_identity_and_method_at(
					identity,
					method,
					admin.version(),
				) {
					return Ok(Some(auth));
				}

				if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
					&mut Transaction::Admin(&mut *admin),
					identity,
					method,
				)? {
					warn!(
						"Authentication for identity {} method '{}' found in storage but not in MaterializedCatalog",
						identity, method
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(auth) = self.materialized.find_authentication_by_identity_and_method_at(
					identity,
					method,
					qry.version(),
				) {
					return Ok(Some(auth));
				}

				if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
					&mut Transaction::Query(&mut *qry),
					identity,
					method,
				)? {
					warn!(
						"Authentication for identity {} method '{}' found in storage but not in MaterializedCatalog",
						identity, method
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(auth) =
					TransactionalAuthenticationChanges::find_authentication_by_identity_and_method(
						t.inner, identity, method,
					) {
					return Ok(Some(auth.clone()));
				}

				if TransactionalAuthenticationChanges::is_authentication_deleted_by_identity_and_method(
					t.inner, identity, method,
				) {
					return Ok(None);
				}

				if let Some(auth) = self.materialized.find_authentication_by_identity_and_method_at(
					identity,
					method,
					t.inner.version(),
				) {
					return Ok(Some(auth));
				}

				if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
					&mut Transaction::Admin(&mut *t.inner),
					identity,
					method,
				)? {
					warn!(
						"Authentication for identity {} method '{}' found in storage but not in MaterializedCatalog",
						identity, method
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(auth) = self.materialized.find_authentication_by_identity_and_method_at(
					identity,
					method,
					rep.version(),
				) {
					return Ok(Some(auth));
				}

				if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
					&mut Transaction::Replica(&mut *rep),
					identity,
					method,
				)? {
					warn!(
						"Authentication for identity {} method '{}' found in storage but not in MaterializedCatalog",
						identity, method
					);
					return Ok(Some(auth));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::authentication::list_by_method", level = "trace", skip(self, txn))]
	pub fn list_authentications_by_method(
		&self,
		txn: &mut Transaction<'_>,
		method: &str,
	) -> Result<Vec<Authentication>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_authentications_by_method_at(method, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut auths =
					self.materialized.list_authentications_by_method_at(method, admin.version());
				for change in &admin.changes.authentication {
					if let Some(auth) = &change.post
						&& auth.method == method && !auths
						.iter()
						.any(|existing| existing.id == auth.id)
					{
						auths.push(auth.clone());
					}
				}
				auths.retain(|a| !admin.is_authentication_deleted(a.id));
				Ok(auths)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_authentications_by_method_at(method, qry.version()))
			}
			Transaction::Test(t) => {
				let mut auths =
					self.materialized.list_authentications_by_method_at(method, t.inner.version());
				for change in &t.inner.changes.authentication {
					if let Some(auth) = &change.post
						&& auth.method == method && !auths
						.iter()
						.any(|existing| existing.id == auth.id)
					{
						auths.push(auth.clone());
					}
				}
				auths.retain(|a| !t.inner.is_authentication_deleted(a.id));
				Ok(auths)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_authentications_by_method_at(method, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::authentication::drop", level = "debug", skip(self, txn))]
	pub fn drop_authentication(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		method: &str,
	) -> Result<()> {
		if let Some(auth) = CatalogStore::find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut *txn),
			identity,
			method,
		)? {
			CatalogStore::drop_authentication(txn, auth.id)?;
			txn.track_authentication_deleted(auth)?;
		}
		Ok(())
	}
}
