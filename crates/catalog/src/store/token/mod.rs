// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::token::Token, store::MultiVersionRow};
use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

use crate::store::token::shape::token;

pub mod create;
pub mod drop;
pub mod find;
pub mod shape;

pub(crate) fn convert_token(multi: MultiVersionRow) -> Token {
	let bytes = multi.bytes;
	let id = token::SHAPE.get::<u64>(&bytes, token::ID);
	let token_value = token::SHAPE.get_utf8(&bytes, token::TOKEN).to_string();
	let identity = token::SHAPE.get::<IdentityId>(&bytes, token::IDENTITY);
	let expires_at = token::SHAPE.try_get::<DateTime>(&bytes, token::EXPIRES_AT);
	let created_at = token::SHAPE.get::<DateTime>(&bytes, token::CREATED_AT);

	Token {
		id,
		token: token_value,
		identity,
		expires_at,
		created_at,
	}
}
