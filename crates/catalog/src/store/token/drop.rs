// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::token::TokenId, key::token::TokenKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

use crate::{CatalogStore, Result, store::token::schema::token};

impl CatalogStore {
	/// Drop a single token by ID.
	pub(crate) fn drop_token(txn: &mut AdminTransaction, id: TokenId) -> Result<()> {
		txn.remove(&TokenKey::encoded(id))?;
		Ok(())
	}

	/// Drop all tokens for a given identity.
	pub(crate) fn drop_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let mut stream = txn.range(TokenKey::full_scan(), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let token_identity = token::SCHEMA.get_identity_id(&multi.values, token::IDENTITY);
				if token_identity == identity {
					let id = token::SCHEMA.get_u64(&multi.values, token::ID);
					to_remove.push(id);
				}
			}
		}

		for id in to_remove {
			txn.remove(&TokenKey::encoded(id))?;
		}

		Ok(())
	}

	/// Drop all expired tokens.
	pub(crate) fn drop_expired_tokens(txn: &mut AdminTransaction, now: DateTime) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let mut stream = txn.range(TokenKey::full_scan(), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				if let Some(expires_at) =
					token::SCHEMA.try_get_datetime(&multi.values, token::EXPIRES_AT)
				{
					if expires_at < now {
						let id = token::SCHEMA.get_u64(&multi.values, token::ID);
						to_remove.push(id);
					}
				}
			}
		}

		for id in to_remove {
			txn.remove(&TokenKey::encoded(id))?;
		}

		Ok(())
	}
}
