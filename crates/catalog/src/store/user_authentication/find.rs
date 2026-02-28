// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		user::UserId,
		user_authentication::{UserAuthenticationDef, UserAuthenticationId},
	},
	key::user_authentication::UserAuthenticationKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::user_authentication::{convert_user_authentication, schema::user_authentication},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_user_authentication(
		rx: &mut Transaction<'_>,
		id: UserAuthenticationId,
	) -> Result<Option<UserAuthenticationDef>> {
		Ok(rx.get(&UserAuthenticationKey::encoded(id))?.map(convert_user_authentication))
	}

	pub(crate) fn find_user_authentication_by_user_and_method(
		rx: &mut Transaction<'_>,
		user_id: UserId,
		method: &str,
	) -> Result<Option<UserAuthenticationDef>> {
		let mut stream = rx.range(UserAuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_user_id =
				user_authentication::SCHEMA.get_u64(&multi.values, user_authentication::USER_ID);
			let auth_method =
				user_authentication::SCHEMA.get_utf8(&multi.values, user_authentication::METHOD);
			if auth_user_id == user_id && auth_method == method {
				return Ok(Some(convert_user_authentication(multi)));
			}
		}

		Ok(None)
	}
}
