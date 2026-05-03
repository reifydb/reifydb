// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::token::TokenId, key::token::TokenKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

use crate::{CatalogStore, Result, store::token::shape::token};

impl CatalogStore {
	pub(crate) fn drop_token(txn: &mut AdminTransaction, id: TokenId) -> Result<()> {
		txn.remove(&TokenKey::encoded(id))?;
		Ok(())
	}

	pub(crate) fn drop_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let stream = txn.range(TokenKey::full_scan(), 1024)?;
			for entry in stream {
				let multi = entry?;
				let token_identity = token::SHAPE.get_identity_id(&multi.row, token::IDENTITY);
				if token_identity == identity {
					let id = token::SHAPE.get_u64(&multi.row, token::ID);
					to_remove.push(id);
				}
			}
		}

		for id in to_remove {
			txn.remove(&TokenKey::encoded(id))?;
		}

		Ok(())
	}

	pub(crate) fn drop_expired_tokens(txn: &mut AdminTransaction, now: DateTime) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let stream = txn.range(TokenKey::full_scan(), 1024)?;
			for entry in stream {
				let multi = entry?;
				if let Some(expires_at) = token::SHAPE.try_get_datetime(&multi.row, token::EXPIRES_AT)
					&& expires_at < now
				{
					let id = token::SHAPE.get_u64(&multi.row, token::ID);
					to_remove.push(id);
				}
			}
		}

		for id in to_remove {
			txn.remove(&TokenKey::encoded(id))?;
		}

		Ok(())
	}
}
