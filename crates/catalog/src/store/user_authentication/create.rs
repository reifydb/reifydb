// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_auth::error::AuthError;
use reifydb_core::{
	interface::catalog::{user::UserId, user_authentication::UserAuthenticationDef},
	key::user_authentication::UserAuthenticationKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore,
	store::{
		sequence::system::SystemSequence,
		user_authentication::schema::user_authentication::{ID, METHOD, PROPERTIES, SCHEMA, USER_ID},
	},
};

impl CatalogStore {
	pub(crate) fn create_user_authentication(
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
		properties: HashMap<String, String>,
	) -> crate::Result<UserAuthenticationDef> {
		let id = SystemSequence::next_user_authentication_id(txn)?;

		// Serialize properties as JSON
		let properties_json =
			serde_json::to_string(&properties).map_err(|e| AuthError::SerializeProperties {
				reason: e.to_string(),
			})?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, id);
		SCHEMA.set_u64(&mut row, USER_ID, user_id);
		SCHEMA.set_utf8(&mut row, METHOD, method);
		SCHEMA.set_utf8(&mut row, PROPERTIES, &properties_json);

		txn.set(&UserAuthenticationKey::encoded(id), row)?;

		Ok(UserAuthenticationDef {
			id,
			user_id,
			method: method.to_string(),
			properties,
		})
	}
}
