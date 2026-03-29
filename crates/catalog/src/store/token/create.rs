// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::token::Token, key::token::TokenKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

use crate::{
	CatalogStore, Result,
	store::{
		sequence::system::SystemSequence,
		token::shape::token::{CREATED_AT, EXPIRES_AT, ID, IDENTITY, SHAPE, TOKEN},
	},
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

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, id);
		SHAPE.set_utf8(&mut row, TOKEN, token);
		SHAPE.set_identity_id(&mut row, IDENTITY, identity);
		if let Some(expires) = expires_at {
			SHAPE.set_datetime(&mut row, EXPIRES_AT, expires);
		} else {
			SHAPE.set_none(&mut row, EXPIRES_AT);
		}
		SHAPE.set_datetime(&mut row, CREATED_AT, created_at);

		txn.set(&TokenKey::encoded(id), row)?;

		Ok(Token {
			id,
			token: token.to_string(),
			identity,
			expires_at,
			created_at,
		})
	}
}
