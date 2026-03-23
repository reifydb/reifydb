// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{token::TokenDef, user::UserId},
	key::token::TokenKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

use crate::{
	CatalogStore, Result,
	store::{
		sequence::system::SystemSequence,
		token::schema::token::{CREATED_AT, EXPIRES_AT, ID, IDENTITY, SCHEMA, TOKEN, USER},
	},
};

impl CatalogStore {
	pub(crate) fn create_token(
		txn: &mut AdminTransaction,
		token: &str,
		identity: IdentityId,
		user: UserId,
		expires_at: Option<DateTime>,
		created_at: DateTime,
	) -> Result<TokenDef> {
		let id = SystemSequence::next_token_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, id);
		SCHEMA.set_utf8(&mut row, TOKEN, token);
		SCHEMA.set_identity_id(&mut row, IDENTITY, identity);
		SCHEMA.set_u64(&mut row, USER, user);
		if let Some(expires) = expires_at {
			SCHEMA.set_datetime(&mut row, EXPIRES_AT, expires);
		} else {
			SCHEMA.set_none(&mut row, EXPIRES_AT);
		}
		SCHEMA.set_datetime(&mut row, CREATED_AT, created_at);

		txn.set(&TokenKey::encoded(id), row)?;

		Ok(TokenDef {
			id,
			token: token.to_string(),
			identity,
			user,
			expires_at,
			created_at,
		})
	}
}
