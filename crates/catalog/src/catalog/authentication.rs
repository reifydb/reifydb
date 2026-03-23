// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::{
	change::CatalogTrackUserAuthenticationChangeOperations, user::UserId,
	user_authentication::UserAuthenticationDef,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::user_authentication::create", level = "debug", skip(self, txn))]
	pub fn create_user_authentication(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<UserAuthenticationDef> {
		let auth = CatalogStore::create_user_authentication(txn, user_id, method, properties)?;
		txn.track_user_authentication_def_created(auth.clone())?;
		Ok(auth)
	}

	#[instrument(name = "catalog::user_authentication::find_by_user_and_method", level = "trace", skip(self, txn))]
	pub fn find_user_authentication_by_user_and_method(
		&self,
		txn: &mut Transaction<'_>,
		user_id: UserId,
		method: &str,
	) -> Result<Option<UserAuthenticationDef>> {
		CatalogStore::find_user_authentication_by_user_and_method(txn, user_id, method)
	}

	#[instrument(name = "catalog::user_authentication::list_by_method", level = "trace", skip(self, txn))]
	pub fn list_user_authentications_by_method(
		&self,
		txn: &mut Transaction<'_>,
		method: &str,
	) -> Result<Vec<UserAuthenticationDef>> {
		CatalogStore::list_user_authentications_by_method(txn, method)
	}

	#[instrument(name = "catalog::user_authentication::drop", level = "debug", skip(self, txn))]
	pub fn drop_user_authentication(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
	) -> Result<()> {
		if let Some(auth) = CatalogStore::find_user_authentication_by_user_and_method(
			&mut Transaction::Admin(&mut *txn),
			user_id,
			method,
		)? {
			CatalogStore::drop_user_authentication(txn, auth.id)?;
			txn.track_user_authentication_def_deleted(auth)?;
		}
		Ok(())
	}
}
