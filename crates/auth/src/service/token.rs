// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::{drop_expired_tokens, drop_token, drop_tokens_by_identity, find_token_by_value};
use reifydb_core::interface::{
	auth::AuthStep,
	catalog::token::{Token, TokenId},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};

use super::AuthService;

impl AuthService {
	pub fn validate_token(&self, token: &str) -> Result<Option<Token>, Error> {
		let mut txn = self.engine.begin_query()?;

		if let Some(def) = find_token_by_value(&mut Transaction::Query(&mut txn), token)? {
			if let Some(expires_at) = def.expires_at
				&& expires_at < self.now()?
			{
				return Ok(None);
			}

			let catalog = self.engine.catalog();
			let enabled = catalog
				.find_identity(&mut Transaction::Query(&mut txn), def.identity)?
				.is_some_and(|identity| identity.enabled);
			if !enabled {
				return Ok(None);
			}

			return Ok(Some(def));
		}

		self.validate_catalog_token(token)
	}

	fn validate_catalog_token(&self, token: &str) -> Result<Option<Token>, Error> {
		let Some(provider) = self.auth_registry.get("token") else {
			return Ok(None);
		};

		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let auths = catalog.list_authentications_by_method(&mut Transaction::Query(&mut txn), "token")?;

		let creds = HashMap::from([("token".to_string(), token.to_string())]);

		for auth in auths {
			if let Ok(AuthStep::Authenticated) = provider.authenticate(&auth.properties, &creds)
				&& let Some(ident) =
					catalog.find_identity(&mut Transaction::Query(&mut txn), auth.identity)?
				&& ident.enabled
			{
				return Ok(Some(Token {
					id: 0,
					token: token.to_string(),
					identity: ident.id,
					expires_at: None,
					created_at: DateTime::default(),
				}));
			}
		}

		Ok(None)
	}

	pub fn revoke_token(&self, token: &str) -> Result<bool, Error> {
		let Some(def) = self.find_token(token)? else {
			return Ok(false);
		};
		self.drop_and_commit(def.id)?;
		Ok(true)
	}

	#[inline]
	fn find_token(&self, token: &str) -> Result<Option<Token>, Error> {
		let mut txn = self.engine.begin_query()?;
		find_token_by_value(&mut Transaction::Query(&mut txn), token)
	}

	#[inline]
	fn drop_and_commit(&self, id: TokenId) -> Result<(), Error> {
		let mut admin = self.engine.begin_admin()?;
		drop_token(&mut admin, id)?;
		admin.commit()?;
		Ok(())
	}

	pub fn revoke_all(&self, identity: IdentityId) -> Result<(), Error> {
		let mut admin = self.engine.begin_admin()?;
		drop_tokens_by_identity(&mut admin, identity)?;
		admin.commit()?;
		Ok(())
	}

	pub fn cleanup_expired(&self) -> Result<(), Error> {
		self.challenges.cleanup_expired();

		let mut admin = self.engine.begin_admin()?;
		drop_expired_tokens(&mut admin, self.now()?)?;
		admin.commit()?;
		Ok(())
	}
}
