// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::{drop_expired_tokens, drop_token, drop_tokens_by_identity, find_token_by_value};
use reifydb_core::interface::{auth::AuthStep, catalog::token::Token};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};

use super::AuthService;

impl AuthService {
	/// Validate a bearer token and return the associated token definition.
	///
	/// Checks in order:
	/// 1. Persisted session tokens (from login)
	/// 2. Catalog tokens (from `CREATE AUTHENTICATION ... { method: token }`)
	pub fn validate_token(&self, token: &str) -> Option<Token> {
		// 1. Check persisted session tokens
		let mut txn = self.engine.begin_query().ok()?;

		if let Ok(Some(def)) = find_token_by_value(&mut Transaction::Query(&mut txn), token) {
			// Check expiry
			if let Some(expires_at) = def.expires_at {
				if expires_at < self.now() {
					return None;
				}
			}
			return Some(def);
		}

		// 2. Fall back to catalog-stored tokens
		self.validate_catalog_token(token)
	}

	/// Check if a token matches any catalog-stored token authentication.
	fn validate_catalog_token(&self, token: &str) -> Option<Token> {
		let provider = self.auth_registry.get("token")?;

		let mut txn = self.engine.begin_query().ok()?;
		let catalog = self.engine.catalog();

		let auths = catalog.list_authentications_by_method(&mut Transaction::Query(&mut txn), "token").ok()?;

		let creds = HashMap::from([("token".to_string(), token.to_string())]);

		for auth in auths {
			if let Ok(AuthStep::Authenticated) = provider.authenticate(&auth.properties, &creds) {
				// Look up the identity via materialized catalog (no transaction needed)
				if let Some(ident) = catalog.materialized.find_identity(auth.identity) {
					if ident.enabled {
						return Some(Token {
							id: 0,
							token: token.to_string(),
							identity: ident.id,
							expires_at: None,
							created_at: DateTime::default(),
						});
					}
				}
			}
		}

		None
	}

	/// Revoke a specific session token.
	pub fn revoke_token(&self, token: &str) -> bool {
		let mut txn = match self.engine.begin_query() {
			Ok(txn) => txn,
			Err(_) => return false,
		};

		let def = match find_token_by_value(&mut Transaction::Query(&mut txn), token) {
			Ok(Some(def)) => def,
			_ => return false,
		};
		drop(txn);

		let mut admin = match self.engine.begin_admin() {
			Ok(a) => a,
			Err(_) => return false,
		};

		if drop_token(&mut admin, def.id).is_err() {
			return false;
		}

		admin.commit().is_ok()
	}

	/// Revoke all session tokens for a given identity.
	pub fn revoke_all(&self, identity: IdentityId) {
		if let Ok(mut admin) = self.engine.begin_admin() {
			if drop_tokens_by_identity(&mut admin, identity).is_ok() {
				let _ = admin.commit();
			}
		}
	}

	/// Clean up expired sessions and challenges.
	pub fn cleanup_expired(&self) {
		if let Ok(mut admin) = self.engine.begin_admin() {
			if drop_expired_tokens(&mut admin, self.now()).is_ok() {
				let _ = admin.commit();
			}
		}
		self.challenges.cleanup_expired();
	}
}
