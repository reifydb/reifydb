// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{
	auth::{AuthStep, AuthenticationProvider},
	catalog::identity::Identity,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	reifydb_assertions,
	value::{Value, identity::IdentityId},
};
use subtle::ConstantTimeEq;
use tracing::warn;

use super::{AuthResponse, AuthService, GithubAuth, generate_session_token};
use crate::{
	challenge::ChallengeInfo,
	error::AuthError,
	github::{GithubUser, build_authorize_url},
};

pub(crate) const GITHUB_USER_ID_ATTRIBUTE: &str = "github_user_id";

impl AuthService {
	pub(crate) fn begin_github_login(&self) -> Result<AuthResponse, Error> {
		let Some(github) = &self.github else {
			return Ok(not_configured());
		};

		let state = generate_session_token(&self.rng);
		let payload = HashMap::from([
			("state".to_string(), state.clone()),
			("authorize_url".to_string(), build_authorize_url(&github.config, &state)),
		]);

		let challenge_id = self.challenges.create(
			"github".to_string(),
			"github".to_string(),
			payload.clone(),
			&self.clock,
			&self.rng,
		);

		Ok(AuthResponse::Challenge {
			challenge_id,
			payload,
		})
	}

	pub(crate) fn complete_github_login(
		&self,
		challenge: &ChallengeInfo,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let Some(github) = &self.github else {
			return Ok(not_configured());
		};

		if !state_matches(challenge, credentials) {
			return Ok(AuthResponse::Failed {
				reason: "invalid oauth state".to_string(),
			});
		}

		let Some(code) = credentials.get("code") else {
			return Ok(AuthResponse::Failed {
				reason: "missing oauth code".to_string(),
			});
		};

		let user = match self.verify_github_user(github, code) {
			Ok(user) => user,
			Err(e) => {
				warn!("github login failed: {:?}", e);
				return Ok(AuthResponse::Failed {
					reason: "github verification failed".to_string(),
				});
			}
		};

		match self.find_github_identity(&user)? {
			Some(ident) if !ident.enabled => Ok(AuthResponse::Failed {
				reason: "identity is disabled".to_string(),
			}),
			Some(ident) => self.authenticate_github_identity(ident.id, &user),
			None => self.auto_provision_github(&user),
		}
	}

	#[inline]
	fn verify_github_user(&self, github: &GithubAuth, code: &str) -> Result<GithubUser, Error> {
		let access_token = github.api.exchange_code(&github.config, code)?;
		github.api.fetch_user(&access_token)
	}

	#[inline]
	fn find_github_identity(&self, user: &GithubUser) -> Result<Option<Identity>, Error> {
		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();
		catalog.find_identity_by_attribute_value(
			&mut Transaction::Query(&mut txn),
			GITHUB_USER_ID_ATTRIBUTE,
			&Value::Utf8(user.id.to_string()),
		)
	}

	#[inline]
	fn authenticate_github_identity(&self, identity: IdentityId, user: &GithubUser) -> Result<AuthResponse, Error> {
		let stored_auth = {
			let mut txn = self.engine.begin_query()?;
			let catalog = self.engine.catalog();
			catalog.find_authentication_by_identity_and_method(
				&mut Transaction::Query(&mut txn),
				identity,
				"github",
			)?
		};
		let Some(stored_auth) = stored_auth else {
			return Ok(invalid_credentials());
		};

		let provider = self.github_provider()?;
		let credentials = HashMap::from([("github_user_id".to_string(), user.id.to_string())]);
		match provider.authenticate(&stored_auth.properties, &credentials)? {
			AuthStep::Authenticated => {
				self.record_github_login(identity, &user.login)?;
				self.finalize_authentication(identity)
			}
			_ => Ok(invalid_credentials()),
		}
	}

	#[inline]
	fn auto_provision_github(&self, user: &GithubUser) -> Result<AuthResponse, Error> {
		let provider = self.github_provider()?;
		let properties = provider.create(
			&self.rng,
			&HashMap::from([
				("user_id".to_string(), user.id.to_string()),
				("login".to_string(), user.login.clone()),
			]),
		)?;

		let identity = self.create_github_identity(user, properties)?;
		self.record_github_login(identity, &user.login)?;
		self.finalize_authentication(identity)
	}

	#[inline]
	fn create_github_identity(
		&self,
		user: &GithubUser,
		properties: HashMap<String, String>,
	) -> Result<IdentityId, Error> {
		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let identifier = format!("github:{}", user.id);
		let ident = catalog.create_identity(&mut admin, &identifier, &self.clock, &self.rng)?;
		catalog.create_authentication(&mut admin, ident.id, "github", properties)?;
		self.set_lookup_attribute(&mut admin, ident.id, GITHUB_USER_ID_ATTRIBUTE, &user.id.to_string())?;
		admin.commit()?;

		reifydb_assertions! {
			assert!(
				ident.id != IdentityId::default(),
				"auto-provisioning created the nil placeholder identity instead of a freshly generated one, so the provisioned principal would later be minted a session token bound to the default id and gain authorization (identifier={identifier:?})"
			);
		}
		Ok(ident.id)
	}

	#[inline]
	fn record_github_login(&self, identity: IdentityId, login: &str) -> Result<(), Error> {
		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let Some(attribute) =
			catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut admin), "github_login")?
		else {
			return Ok(());
		};

		catalog.set_identity_attribute_value(&mut admin, identity, &attribute, Value::Utf8(login.to_string()))?;
		admin.commit()?;
		Ok(())
	}

	#[inline]
	fn github_provider(&self) -> Result<&dyn AuthenticationProvider, Error> {
		self.auth_registry.get("github").ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: "github".to_string(),
			})
		})
	}
}

#[inline]
fn state_matches(challenge: &ChallengeInfo, credentials: &HashMap<String, String>) -> bool {
	match (challenge.payload.get("state"), credentials.get("state")) {
		(Some(expected), Some(provided)) => expected.as_bytes().ct_eq(provided.as_bytes()).into(),
		_ => false,
	}
}

#[inline]
fn not_configured() -> AuthResponse {
	AuthResponse::Failed {
		reason: "github authentication is not configured".to_string(),
	}
}

#[inline]
fn invalid_credentials() -> AuthResponse {
	AuthResponse::Failed {
		reason: "invalid credentials".to_string(),
	}
}
