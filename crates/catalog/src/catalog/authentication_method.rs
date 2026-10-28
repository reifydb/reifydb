// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	authentication_method::{AuthenticationMethod, AuthenticationMethodId, AuthenticationMethodToCreate},
	change::CatalogTrackAuthenticationMethodChangeOperations,
};
use reifydb_transaction::{
	change::TransactionalAuthenticationMethodChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{error::Error, fragment::Fragment};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::authentication_method::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_authentication_method_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: &str,
	) -> Result<Option<AuthenticationMethod>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				if let Some(method) =
					TransactionalAuthenticationMethodChanges::find_authentication_method_by_name(
						admin, name,
					) {
					return Ok(Some(method.clone()));
				}

				if TransactionalAuthenticationMethodChanges::is_authentication_method_deleted_by_name(
					admin, name,
				) {
					return Ok(None);
				}

				if let Some(method) =
					self.cache.find_authentication_method_by_name_at(name, admin.version())
				{
					return Ok(Some(method));
				}

				if let Some(method) = CatalogStore::find_authentication_method_by_name(
					&mut Transaction::Admin(&mut *admin),
					name,
				)? {
					warn!(
						"Authentication method '{}' found in storage but not in CatalogCache",
						name
					);
					return Ok(Some(method));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(method) =
					self.cache.find_authentication_method_by_name_at(name, cmd.version())
				{
					return Ok(Some(method));
				}

				if let Some(method) = CatalogStore::find_authentication_method_by_name(
					&mut Transaction::Command(&mut *cmd),
					name,
				)? {
					warn!(
						"Authentication method '{}' found in storage but not in CatalogCache",
						name
					);
					return Ok(Some(method));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(method) =
					self.cache.find_authentication_method_by_name_at(name, qry.version())
				{
					return Ok(Some(method));
				}

				if let Some(method) = CatalogStore::find_authentication_method_by_name(
					&mut Transaction::Query(&mut *qry),
					name,
				)? {
					warn!(
						"Authentication method '{}' found in storage but not in CatalogCache",
						name
					);
					return Ok(Some(method));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(method) =
					TransactionalAuthenticationMethodChanges::find_authentication_method_by_name(
						t.inner, name,
					) {
					return Ok(Some(method.clone()));
				}

				if TransactionalAuthenticationMethodChanges::is_authentication_method_deleted_by_name(
					t.inner, name,
				) {
					return Ok(None);
				}

				CatalogStore::find_authentication_method_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					name,
				)
			}
		}
	}

	pub fn get_authentication_method_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: &str,
	) -> Result<AuthenticationMethod> {
		self.find_authentication_method_by_name(txn, name)?.ok_or_else(|| not_found(name))
	}

	#[instrument(name = "catalog::authentication_method::create", level = "info", skip(self, txn, to_create))]
	pub fn create_authentication_method(
		&self,
		txn: &mut AdminTransaction,
		to_create: AuthenticationMethodToCreate,
	) -> Result<AuthenticationMethod> {
		let method = CatalogStore::create_authentication_method(txn, to_create)?;
		txn.track_authentication_method_created(method.clone())?;
		Ok(method)
	}

	#[instrument(name = "catalog::authentication_method::alter", level = "debug", skip(self, txn, method))]
	pub fn alter_authentication_method(
		&self,
		txn: &mut AdminTransaction,
		method: AuthenticationMethod,
	) -> Result<()> {
		let pre = CatalogStore::find_authentication_method(&mut Transaction::Admin(&mut *txn), method.id)?
			.ok_or_else(|| not_found(&method.name))?;

		CatalogStore::alter_authentication_method(txn, &method)?;
		txn.track_authentication_method_updated(pre, method)?;
		Ok(())
	}

	#[instrument(name = "catalog::authentication_method::drop", level = "info", skip(self, txn))]
	pub fn drop_authentication_method(&self, txn: &mut AdminTransaction, id: AuthenticationMethodId) -> Result<()> {
		if let Some(method) = CatalogStore::find_authentication_method(&mut Transaction::Admin(&mut *txn), id)?
		{
			CatalogStore::drop_authentication_method(txn, id)?;
			txn.track_authentication_method_deleted(method)?;
		} else {
			CatalogStore::drop_authentication_method(txn, id)?;
		}
		Ok(())
	}

	pub fn list_all_authentication_methods(&self, txn: &mut Transaction<'_>) -> Result<Vec<AuthenticationMethod>> {
		if let Transaction::Test(t) = txn {
			let mut methods = CatalogStore::list_all_authentication_methods(&mut Transaction::Test(
				Box::new(t.reborrow()),
			))?;
			methods.sort_by_key(|m| m.id);
			return Ok(methods);
		}
		let cached = self.cache.list_all_authentication_methods_at(txn.version());
		if !cached.is_empty() {
			return Ok(cached);
		}
		CatalogStore::list_all_authentication_methods(txn)
	}
}

fn not_found(name: &str) -> Error {
	CatalogError::NotFound {
		kind: CatalogObjectKind::AuthenticationMethod,
		namespace: "system".to_string(),
		name: name.to_string(),
		fragment: Fragment::None,
	}
	.into()
}
