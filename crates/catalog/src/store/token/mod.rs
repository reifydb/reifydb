// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::token::Token, store::MultiVersionRow};

use crate::store::token::shape::token;

pub mod create;
pub mod drop;
pub mod find;
pub mod shape;

pub(crate) fn convert_token(multi: MultiVersionRow) -> Token {
	let bytes = multi.bytes;
	let id = token::get_id(&bytes);
	let token_value = token::get_token(&bytes).to_string();
	let identity = token::get_identity(&bytes);
	let expires_at = token::try_get_expires_at(&bytes);
	let created_at = token::get_created_at(&bytes);

	Token {
		id,
		token: token_value,
		identity,
		expires_at,
		created_at,
	}
}
