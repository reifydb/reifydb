// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{catalog::user_authentication::UserAuthenticationDef, store::MultiVersionValues};
use serde_json::from_str;

use crate::store::user_authentication::schema::user_authentication;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_user_authentication(multi: MultiVersionValues) -> UserAuthenticationDef {
	let row = multi.values;
	let id = user_authentication::SCHEMA.get_u64(&row, user_authentication::ID);
	let user_id = user_authentication::SCHEMA.get_u64(&row, user_authentication::USER_ID);
	let method = user_authentication::SCHEMA.get_utf8(&row, user_authentication::METHOD).to_string();
	let properties_json = user_authentication::SCHEMA.get_utf8(&row, user_authentication::PROPERTIES).to_string();

	let properties: HashMap<String, String> = from_str(&properties_json).unwrap_or_default();

	UserAuthenticationDef {
		id,
		user_id,
		method,
		properties,
	}
}
