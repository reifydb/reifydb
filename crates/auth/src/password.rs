// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use argon2::{
	Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
	password_hash::Error as PasswordHashError,
};
use reifydb_core::interface::auth::AuthenticationProvider;
use reifydb_type::{Result, error::Error};

use crate::error::AuthError;

pub struct PasswordProvider;

/// OWASP-recommended Argon2id parameters:
/// 19 MiB memory, 2 iterations, parallelism 1, 32-byte output.
fn argon2_instance() -> Argon2<'static> {
	let params = Params::new(19 * 1024, 2, 1, Some(32)).expect("valid Argon2 params");
	Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

impl AuthenticationProvider for PasswordProvider {
	fn method(&self) -> &str {
		"password"
	}

	fn create(&self, config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
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

	fn validate(&self, stored: &HashMap<String, String>, credential: &str) -> Result<bool> {
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
			Ok(()) => Ok(true),
			Err(PasswordHashError::PasswordInvalid) => Ok(false),
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
	fn test_password_create_and_validate() {
		let provider = PasswordProvider;
		let config = HashMap::from([("password".to_string(), "secret123".to_string())]);

		let stored = provider.create(&config).unwrap();
		assert!(stored.contains_key("phc"));
		assert!(stored.get("phc").unwrap().starts_with("$argon2id$"));
		assert_eq!(stored.get("algorithm_version").unwrap(), "1");

		assert!(provider.validate(&stored, "secret123").unwrap());
		assert!(!provider.validate(&stored, "wrong_password").unwrap());
	}

	#[test]
	fn test_password_requires_password_field() {
		let provider = PasswordProvider;
		let config = HashMap::new();
		assert!(provider.create(&config).is_err());
	}

	#[test]
	fn test_validate_corrupted_hash() {
		let provider = PasswordProvider;
		let stored = HashMap::from([
			("phc".into(), "not-a-valid-phc-string".to_string()),
			("algorithm_version".into(), "1".into()),
		]);
		assert!(provider.validate(&stored, "anything").is_err());
	}

	#[test]
	fn test_validate_missing_phc() {
		let provider = PasswordProvider;
		let stored = HashMap::new();
		assert!(provider.validate(&stored, "anything").is_err());
	}
}
