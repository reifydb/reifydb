// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_auth::error::AuthError;
use reifydb_core::{
	interface::catalog::{authentication::AuthenticationDef, user::UserId},
	key::authentication::AuthenticationKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	store::{
		authentication::schema::authentication::{ID, METHOD, PROPERTIES, SCHEMA, USER_ID},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_authentication(
		txn: &mut AdminTransaction,
		user_id: UserId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<AuthenticationDef> {
		let id = SystemSequence::next_authentication_id(txn)?;

		// Serialize properties as JSON
		let properties_json = to_string(&properties).map_err(|e| AuthError::SerializeProperties {
			reason: e.to_string(),
		})?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, id);
		SCHEMA.set_u64(&mut row, USER_ID, user_id);
		SCHEMA.set_utf8(&mut row, METHOD, method);
		SCHEMA.set_utf8(&mut row, PROPERTIES, &properties_json);

		txn.set(&AuthenticationKey::encoded(id), row)?;

		Ok(AuthenticationDef {
			id,
			user_id,
			method: method.to_string(),
			properties,
		})
	}
}
