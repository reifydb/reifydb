// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{catalog::authentication::AuthenticationDef, store::MultiVersionValues};
use serde_json::from_str;

use crate::store::authentication::schema::authentication;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_authentication(multi: MultiVersionValues) -> AuthenticationDef {
	let row = multi.values;
	let id = authentication::SCHEMA.get_u64(&row, authentication::ID);
	let user_id = authentication::SCHEMA.get_u64(&row, authentication::USER_ID);
	let method = authentication::SCHEMA.get_utf8(&row, authentication::METHOD).to_string();
	let properties_json = authentication::SCHEMA.get_utf8(&row, authentication::PROPERTIES).to_string();

	let properties: HashMap<String, String> = from_str(&properties_json).unwrap_or_default();

	AuthenticationDef {
		id,
		user_id,
		method,
		properties,
	}
}
