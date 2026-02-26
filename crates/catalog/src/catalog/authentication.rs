// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::{
	change::CatalogTrackUserAuthenticationChangeOperations, user::UserId,
	user_authentication::UserAuthenticationDef,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::user_authentication::create", level = "debug", skip(self, txn))]
	pub fn create_user_authentication(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
		properties: HashMap<String, String>,
	) -> crate::Result<UserAuthenticationDef> {
		let auth = CatalogStore::create_user_authentication(txn, user_id, method, properties)?;
		txn.track_user_authentication_def_created(auth.clone())?;
		Ok(auth)
	}

	#[instrument(name = "catalog::user_authentication::drop", level = "debug", skip(self, txn))]
	pub fn drop_user_authentication(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
	) -> crate::Result<()> {
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
