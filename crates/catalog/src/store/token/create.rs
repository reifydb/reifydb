// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::token::Token, key::token::TokenKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

use crate::{
	CatalogStore, Result,
	store::{sequence::system::SystemSequence, token::shape::token},
};

impl CatalogStore {
	pub(crate) fn create_token(
		txn: &mut AdminTransaction,
		token: &str,
		identity: IdentityId,
		expires_at: Option<DateTime>,
		created_at: DateTime,
	) -> Result<Token> {
		let id = SystemSequence::next_token_id(txn)?;

		let mut row = token::allocate();
		token::set_id(&mut row, id);
		token::set_token(&mut row, token);
		token::set_identity(&mut row, identity);
		if let Some(expires) = expires_at {
			token::set_expires_at(&mut row, expires);
		} else {
			token::set_expires_at_none(&mut row);
		}
		token::set_created_at(&mut row, created_at);

		txn.set(&TokenKey::encoded(id), row.freeze())?;

		Ok(Token {
			id,
			token: token.to_string(),
			identity,
			expires_at,
			created_at,
		})
	}
}
