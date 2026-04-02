// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::token::Token, key::token::TokenKey};
use reifydb_transaction::transaction::Transaction;
use subtle::ConstantTimeEq;

use crate::{
	CatalogStore, Result,
	store::token::{convert_token, shape::token},
};

impl CatalogStore {
	/// Find a token by its value using constant-time comparison.
	pub(crate) fn find_token_by_value(rx: &mut Transaction<'_>, value: &str) -> Result<Option<Token>> {
		let stream = rx.range(TokenKey::full_scan(), 1024)?;

		for entry in stream {
			let multi = entry?;
			let stored_token = token::SHAPE.get_utf8(&multi.row, token::TOKEN);
			if stored_token.as_bytes().ct_eq(value.as_bytes()).into() {
				return Ok(Some(convert_token(multi)));
			}
		}

		Ok(None)
	}
}
