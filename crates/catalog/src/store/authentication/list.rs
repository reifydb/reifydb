// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{authentication::AuthenticationDef, user::UserId},
	key::authentication::AuthenticationKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::authentication::{convert_authentication, schema::authentication},
};

impl CatalogStore {
	pub(crate) fn list_all_authentications(rx: &mut Transaction<'_>) -> Result<Vec<AuthenticationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_authentication(multi));
		}

		Ok(result)
	}

	#[allow(dead_code)]
	pub(crate) fn list_authentications_by_user(
		rx: &mut Transaction<'_>,
		user_id: UserId,
	) -> Result<Vec<AuthenticationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_user_id = authentication::SCHEMA.get_u64(&multi.row, authentication::USER_ID);
			if auth_user_id == user_id {
				result.push(convert_authentication(multi));
			}
		}

		Ok(result)
	}
}
