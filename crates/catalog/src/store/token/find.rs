// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{interface::catalog::token::Token, key::identity::TokenKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use subtle::ConstantTimeEq;

use crate::{
	CatalogStore, Result,
	store::token::{convert_token, shape::token},
};

impl CatalogStore {
	pub(crate) fn find_token_by_value(rx: &mut Transaction<'_>, value: &str) -> Result<Option<Token>> {
		let stream = rx.range(TokenKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			let stored_token = token::get_token(EncodedCatalogRow::view(&multi.bytes));
			if stored_token.as_bytes().ct_eq(value.as_bytes()).into() {
				return Ok(Some(convert_token(multi)?));
			}
		}

		Ok(None)
	}
}
