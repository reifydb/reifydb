// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_runtime::context::rng::Rng;
use reifydb_value::{Result, error::Error};
use subtle::ConstantTimeEq;

use crate::error::GithubError;

pub struct GithubProvider;

impl AuthenticationProvider for GithubProvider {
	fn method(&self) -> &str {
		"github"
	}

	fn create(&self, _rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		let user_id = config.get("user_id").ok_or_else(|| Error::from(GithubError::MissingUserId))?;

		if user_id.is_empty() || !user_id.bytes().all(|b| b.is_ascii_digit()) {
			return Err(Error::from(GithubError::InvalidUserId {
				reason: "expected the numeric github account id".to_string(),
			}));
		}

		let mut properties = HashMap::from([("user_id".to_string(), user_id.clone())]);
		if let Some(login) = config.get("login") {
			properties.insert("login".to_string(), login.clone());
		}
		Ok(properties)
	}

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		let stored_id = stored.get("user_id").ok_or_else(|| Error::from(GithubError::MissingUserId))?;

		let Some(verified_id) = credentials.get("github_user_id") else {
			return Ok(AuthStep::Failed);
		};

		if stored_id.as_bytes().ct_eq(verified_id.as_bytes()).into() {
			Ok(AuthStep::Authenticated)
		} else {
			Ok(AuthStep::Failed)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_create_stores_user_id_and_login() {
		let config = HashMap::from([
			("user_id".to_string(), "583231".to_string()),
			("login".to_string(), "octocat".to_string()),
		]);

		let stored = GithubProvider.create(&Rng::default(), &config).unwrap();
		assert_eq!(stored.get("user_id").unwrap(), "583231");
		assert_eq!(stored.get("login").unwrap(), "octocat");
	}

	#[test]
	fn test_create_requires_user_id() {
		assert!(GithubProvider.create(&Rng::default(), &HashMap::new()).is_err());
	}

	#[test]
	fn test_create_rejects_non_numeric_user_id() {
		// The user_id is the immutable github account id; a login name here would
		// let an account be hijacked after a github username change or reuse.
		let config = HashMap::from([("user_id".to_string(), "octocat".to_string())]);
		assert!(GithubProvider.create(&Rng::default(), &config).is_err());
	}

	#[test]
	fn test_create_rejects_empty_user_id() {
		let config = HashMap::from([("user_id".to_string(), "".to_string())]);
		assert!(GithubProvider.create(&Rng::default(), &config).is_err());
	}

	#[test]
	fn test_authenticate_matching_user_id() {
		let stored = HashMap::from([("user_id".to_string(), "583231".to_string())]);
		let credentials = HashMap::from([("github_user_id".to_string(), "583231".to_string())]);

		let step = GithubProvider.authenticate(&stored, &credentials).unwrap();
		assert_eq!(step, AuthStep::Authenticated);
	}

	#[test]
	fn test_authenticate_mismatched_user_id_fails() {
		let stored = HashMap::from([("user_id".to_string(), "583231".to_string())]);
		let credentials = HashMap::from([("github_user_id".to_string(), "999999".to_string())]);

		let step = GithubProvider.authenticate(&stored, &credentials).unwrap();
		assert_eq!(step, AuthStep::Failed);
	}

	#[test]
	fn test_authenticate_without_verified_user_id_fails() {
		// github_user_id is injected by the auth service only after it verified the
		// oauth token with github; raw client credentials must never authenticate.
		let stored = HashMap::from([("user_id".to_string(), "583231".to_string())]);
		let credentials = HashMap::from([
			("code".to_string(), "some-oauth-code".to_string()),
			("state".to_string(), "some-state".to_string()),
		]);

		let step = GithubProvider.authenticate(&stored, &credentials).unwrap();
		assert_eq!(step, AuthStep::Failed);
	}
}
