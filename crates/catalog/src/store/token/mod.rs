// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{catalog::token::Token, store::MultiVersionRow};

use crate::store::token::schema::token;

pub mod create;
pub mod drop;
pub mod find;
pub mod schema;

pub(crate) fn convert_token(multi: MultiVersionRow) -> Token {
	let row = multi.row;
	let id = token::SCHEMA.get_u64(&row, token::ID);
	let token_value = token::SCHEMA.get_utf8(&row, token::TOKEN).to_string();
	let identity = token::SCHEMA.get_identity_id(&row, token::IDENTITY);
	let expires_at = token::SCHEMA.try_get_datetime(&row, token::EXPIRES_AT);
	let created_at = token::SCHEMA.get_datetime(&row, token::CREATED_AT);

	Token {
		id,
		token: token_value,
		identity,
		expires_at,
		created_at,
	}
}
