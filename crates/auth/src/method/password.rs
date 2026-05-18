// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use argon2::{
	Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
	password_hash::Error as PasswordHashError,
};
use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_runtime::context::rng::Rng;
use reifydb_type::{Result, error::Error};

use crate::error::AuthError;

pub struct PasswordProvider;

fn argon2_instance() -> Argon2<'static> {
	let params = Params::new(19 * 1024, 2, 1, Some(32)).expect("valid Argon2 params");
	Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

impl AuthenticationProvider for PasswordProvider {
	fn method(&self) -> &str {
		"password"
	}

	fn create(&self, _rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		let password = config.get("password").ok_or_else(|| Error::from(AuthError::PasswordRequired))?;

		let argon2 = argon2_instance();

		let phc = argon2
			.hash_password(password.as_bytes())
			.map_err(|e| {
				Error::from(AuthError::HashingFailed {
					reason: e.to_string(),
				})
			})?
			.to_string();

		Ok(HashMap::from([("phc".into(), phc), ("algorithm_version".into(), "1".into())]))
	}

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		let credential = credentials.get("password").ok_or_else(|| Error::from(AuthError::PasswordRequired))?;

		let phc_str = stored.get("phc").ok_or_else(|| {
			Error::from(AuthError::InvalidHash {
				reason: "missing 'phc' field".to_string(),
			})
		})?;

		let parsed_hash = PasswordHash::new(phc_str).map_err(|e| {
			Error::from(AuthError::InvalidHash {
				reason: e.to_string(),
			})
		})?;

		let argon2 = argon2_instance();

		match argon2.verify_password(credential.as_bytes(), &parsed_hash) {
			Ok(()) => Ok(AuthStep::Authenticated),
			Err(PasswordHashError::PasswordInvalid) => Ok(AuthStep::Failed),
			Err(e) => Err(Error::from(AuthError::VerificationFailed {
				reason: e.to_string(),
			})),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_password_create_and_authenticate() {
		let provider = PasswordProvider;
		let config = HashMap::from([("password".to_string(), "secret123".to_string())]);

		let stored = provider.create(&Rng::default(), &config).unwrap();
		assert!(stored.contains_key("phc"));
		assert!(stored.get("phc").unwrap().starts_with("$argon2id$"));
		assert_eq!(stored.get("algorithm_version").unwrap(), "1");

		let correct = HashMap::from([("password".to_string(), "secret123".to_string())]);
		assert_eq!(provider.authenticate(&stored, &correct).unwrap(), AuthStep::Authenticated);

		let wrong = HashMap::from([("password".to_string(), "wrong_password".to_string())]);
		assert_eq!(provider.authenticate(&stored, &wrong).unwrap(), AuthStep::Failed);
	}

	#[test]
	fn test_password_requires_password_field() {
		let provider = PasswordProvider;
		let config = HashMap::new();
		assert!(provider.create(&Rng::default(), &config).is_err());
	}

	#[test]
	fn test_authenticate_corrupted_hash() {
		let provider = PasswordProvider;
		let stored = HashMap::from([
			("phc".into(), "not-a-valid-phc-string".to_string()),
			("algorithm_version".into(), "1".into()),
		]);
		let creds = HashMap::from([("password".to_string(), "anything".to_string())]);
		assert!(provider.authenticate(&stored, &creds).is_err());
	}

	#[test]
	fn test_authenticate_missing_phc() {
		let provider = PasswordProvider;
		let stored = HashMap::new();
		let creds = HashMap::from([("password".to_string(), "anything".to_string())]);
		assert!(provider.authenticate(&stored, &creds).is_err());
	}

	#[test]
	fn test_authenticate_missing_password_credential() {
		let provider = PasswordProvider;
		let config = HashMap::from([("password".to_string(), "secret123".to_string())]);
		let stored = provider.create(&Rng::default(), &config).unwrap();
		let empty_creds = HashMap::new();
		assert!(provider.authenticate(&stored, &empty_creds).is_err());
	}
}
