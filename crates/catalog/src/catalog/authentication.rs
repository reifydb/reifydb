// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::{
	authentication::AuthenticationDef, change::CatalogTrackAuthenticationChangeOperations,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::identity::IdentityId;
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::authentication::create", level = "debug", skip(self, txn))]
	pub fn create_authentication(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<AuthenticationDef> {
		let auth = CatalogStore::create_authentication(txn, identity, method, properties)?;
		txn.track_authentication_def_created(auth.clone())?;
		Ok(auth)
	}

	#[instrument(name = "catalog::authentication::find_by_identity_and_method", level = "trace", skip(self, txn))]
	pub fn find_authentication_by_identity_and_method(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
		method: &str,
	) -> Result<Option<AuthenticationDef>> {
		CatalogStore::find_authentication_by_identity_and_method(txn, identity, method)
	}

	#[instrument(name = "catalog::authentication::list_by_method", level = "trace", skip(self, txn))]
	pub fn list_authentications_by_method(
		&self,
		txn: &mut Transaction<'_>,
		method: &str,
	) -> Result<Vec<AuthenticationDef>> {
		CatalogStore::list_authentications_by_method(txn, method)
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
			txn.track_authentication_def_deleted(auth)?;
		}
		Ok(())
	}
}
