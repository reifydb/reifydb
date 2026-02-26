// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{user::UserId, user_authentication::UserAuthenticationDef},
	key::user_authentication::UserAuthenticationKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::user_authentication::{convert_user_authentication, schema::user_authentication},
};

impl CatalogStore {
	pub(crate) fn list_all_user_authentications(
		rx: &mut Transaction<'_>,
	) -> crate::Result<Vec<UserAuthenticationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(UserAuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_user_authentication(multi));
		}

		Ok(result)
	}

	#[allow(dead_code)]
	pub(crate) fn list_user_authentications_by_user(
		rx: &mut Transaction<'_>,
		user_id: UserId,
	) -> crate::Result<Vec<UserAuthenticationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(UserAuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_user_id =
				user_authentication::SCHEMA.get_u64(&multi.values, user_authentication::USER_ID);
			if auth_user_id == user_id {
				result.push(convert_user_authentication(multi));
			}
		}

		Ok(result)
	}
}
