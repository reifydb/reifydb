// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_auth::crypto::constant_time_eq;
use reifydb_core::{interface::catalog::token::TokenDef, key::token::TokenKey};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::token::{convert_token, schema::token},
};

impl CatalogStore {
	/// Find a token by its value using constant-time comparison.
	pub(crate) fn find_token_by_value(rx: &mut Transaction<'_>, value: &str) -> Result<Option<TokenDef>> {
		let mut stream = rx.range(TokenKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let stored_token = token::SCHEMA.get_utf8(&multi.values, token::TOKEN);
			if constant_time_eq(stored_token.as_bytes(), value.as_bytes()) {
				return Ok(Some(convert_token(multi)));
			}
		}

		Ok(None)
	}
}
