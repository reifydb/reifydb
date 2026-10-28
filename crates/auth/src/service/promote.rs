// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_catalog::{create_token, drop_tokens_by_identity};
use reifydb_core::retry::RetryStrategy;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	value::{
		duration::Duration,
		identity::{IdentityId, IdentityKind},
	},
};

use super::{AuthResponse, AuthService, generate_session_token};

impl AuthService {
	pub(super) fn is_guest(&self, caller: IdentityId) -> Result<bool, Error> {
		if caller.is_anonymous() {
			return Ok(false);
		}
		let mut txn = self.engine.begin_query()?;
		let found = self.engine.catalog().find_identity(&mut Transaction::Query(&mut txn), caller)?;
		Ok(found.is_some_and(|identity| identity.resolved_kind() == IdentityKind::Guest))
	}

	pub(super) fn promote_or_sign_up(
		&self,
		caller: IdentityId,
		name: &str,
		method: &str,
		properties: BTreeMap<String, String>,
		lookup: Option<(&str, &str)>,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		if self.is_guest(caller)? {
			return self.promote_guest(caller, name, method, properties, lookup, ttl);
		}
		self.sign_up(name, method, properties, lookup, ttl)
	}

	pub(super) fn promote_guest(
		&self,
		guest: IdentityId,
		name: &str,
		method: &str,
		properties: BTreeMap<String, String>,
		lookup: Option<(&str, &str)>,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		let token = generate_session_token(&self.rng);
		RetryStrategy::default().retry(&self.rng, "auth::promote_guest", || {
			let mut admin = self.engine.begin_admin()?;
			let catalog = self.engine.catalog();
			catalog.rename_identity(&mut admin, guest, name)?;
			catalog.promote_guest_to_user(&mut admin, guest)?;
			catalog.create_authentication(&mut admin, guest, method, properties.clone())?;
			if let Some((attribute, value)) = lookup {
				self.set_lookup_attribute(&mut admin, guest, attribute, value)?;
			}
			drop_tokens_by_identity(&mut admin, guest)?;
			create_token(&mut admin, &token, guest, self.expires_at(ttl)?, self.now()?)?;
			admin.commit()?;
			Ok(())
		})?;
		Ok(AuthResponse::Authenticated {
			identity: guest,
			token,
		})
	}

	fn sign_up(
		&self,
		name: &str,
		method: &str,
		properties: BTreeMap<String, String>,
		lookup: Option<(&str, &str)>,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		let token = generate_session_token(&self.rng);
		let identity = RetryStrategy::default().retry(&self.rng, "auth::sign_up", || {
			let mut admin = self.engine.begin_admin()?;
			let catalog = self.engine.catalog();
			let identity =
				catalog.create_identity(&mut admin, name, IdentityKind::User, &self.clock, &self.rng)?;
			catalog.create_authentication(&mut admin, identity.id, method, properties.clone())?;
			if let Some((attribute, value)) = lookup {
				self.set_lookup_attribute(&mut admin, identity.id, attribute, value)?;
			}
			create_token(&mut admin, &token, identity.id, self.expires_at(ttl)?, self.now()?)?;
			admin.commit()?;
			Ok(identity.id)
		})?;
		Ok(AuthResponse::Authenticated {
			identity,
			token,
		})
	}
}
