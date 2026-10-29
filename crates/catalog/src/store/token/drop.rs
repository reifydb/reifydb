// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::token::{TokenId, TokenKind},
	key::identity::TokenKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::admin::AdminTransaction};
use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

use crate::{CatalogStore, Result, store::token::shape::token};

impl CatalogStore {
	pub(crate) fn drop_token(txn: &mut AdminTransaction, id: TokenId) -> Result<()> {
		txn.remove(vec![(TokenKey::new(id).into(), None)])?;
		Ok(())
	}

	pub(crate) fn drop_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		Self::drop_identity_tokens_where(txn, identity, |_| true)
	}

	pub(crate) fn drop_session_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		Self::drop_identity_tokens_where(txn, identity, |kind| kind == TokenKind::Session)
	}

	fn drop_identity_tokens_where(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		drops_kind: impl Fn(TokenKind) -> bool,
	) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let stream = txn.range(TokenKey::full_scan(), RangeScope::All, 1024)?;
			for entry in stream {
				let multi = entry?;
				let row = EncodedCatalogRow::view(&multi.bytes);
				if token::get_identity(row) == identity && drops_kind(token::get_kind(row)) {
					to_remove.push(token::get_id(row));
				}
			}
		}

		for id in to_remove {
			txn.remove(vec![(TokenKey::new(id).into(), None)])?;
		}

		Ok(())
	}

	pub(crate) fn drop_expired_tokens(txn: &mut AdminTransaction, now: DateTime) -> Result<()> {
		let mut to_remove = Vec::new();
		{
			let stream = txn.range(TokenKey::full_scan(), RangeScope::All, 1024)?;
			for entry in stream {
				let multi = entry?;
				if let Some(expires_at) =
					token::try_get_expires_at(EncodedCatalogRow::view(&multi.bytes))
					&& expires_at < now
				{
					let id = token::get_id(EncodedCatalogRow::view(&multi.bytes));
					to_remove.push(id);
				}
			}
		}

		for id in to_remove {
			txn.remove(vec![(TokenKey::new(id).into(), None)])?;
		}

		Ok(())
	}
}
