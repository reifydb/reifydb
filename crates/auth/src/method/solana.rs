// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Solana wallet authentication provider.
//!
//! Implements a challenge-response flow compatible with Sign In With Solana (SIWS):
//! 1. Client initiates auth → provider returns a challenge with a SIWS message to sign
//! 2. Client signs the message with their wallet → provider verifies the ed25519 signature

use std::collections::HashMap;

use bs58::decode as bs58_decode;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_type::{Result, error::Error};

use crate::error::AuthError;

pub struct SolanaProvider {
	clock: Clock,
}

impl SolanaProvider {
	pub fn new(clock: Clock) -> Self {
		Self {
			clock,
		}
	}
}

impl AuthenticationProvider for SolanaProvider {
	fn method(&self) -> &str {
		"solana"
	}

	fn create(&self, _rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		let public_key = config.get("public_key").ok_or_else(|| Error::from(AuthError::MissingPublicKey))?;

		// Validate that the public key is valid base58 and decodes to 32 bytes
		let bytes = bs58_decode(public_key).into_vec().map_err(|e| {
			Error::from(AuthError::InvalidPublicKey {
				reason: e.to_string(),
			})
		})?;

		if bytes.len() != 32 {
			return Err(Error::from(AuthError::InvalidPublicKey {
				reason: format!("expected 32 bytes, got {}", bytes.len()),
			}));
		}

		Ok(HashMap::from([("public_key".into(), public_key.clone())]))
	}

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		let public_key_b58 =
			stored.get("public_key").ok_or_else(|| Error::from(AuthError::MissingPublicKey))?;

		// Step 2: Verify signature (credentials contain "signature" + "signed_message" merged from challenge)
		if let Some(signature_b58) = credentials.get("signature") {
			let signed_message = credentials.get("signed_message").ok_or_else(|| {
				Error::from(AuthError::InvalidSignature {
					reason: "missing signed_message".to_string(),
				})
			})?;

			// Decode public key
			let pk_bytes: [u8; 32] = bs58_decode(public_key_b58)
				.into_vec()
				.map_err(|e| {
					Error::from(AuthError::InvalidPublicKey {
						reason: e.to_string(),
					})
				})?
				.try_into()
				.map_err(|_| {
					Error::from(AuthError::InvalidPublicKey {
						reason: "expected 32 bytes".to_string(),
					})
				})?;

			let verifying_key = VerifyingKey::from_bytes(&pk_bytes).map_err(|e| {
				Error::from(AuthError::InvalidPublicKey {
					reason: e.to_string(),
				})
			})?;

			// Decode signature
			let sig_bytes: [u8; 64] = bs58_decode(signature_b58)
				.into_vec()
				.map_err(|e| {
					Error::from(AuthError::InvalidSignature {
						reason: e.to_string(),
					})
				})?
				.try_into()
				.map_err(|_| {
					Error::from(AuthError::InvalidSignature {
						reason: "expected 64 bytes".to_string(),
					})
				})?;

			let signature = Signature::from_bytes(&sig_bytes);

			// Verify
			match verifying_key.verify(signed_message.as_bytes(), &signature) {
				Ok(()) => return Ok(AuthStep::Authenticated),
				Err(_) => return Ok(AuthStep::Failed),
			}
		}

		// Step 1: Generate challenge - build SIWS message with nonce
		let nonce_bytes = Rng::Os.bytes_32();
		let nonce: String = nonce_bytes.iter().map(|b| format!("{:02x}", b)).collect();

		// Get optional domain and statement from credentials (caller can provide context)
		let domain = credentials.get("domain").cloned().unwrap_or_else(|| "reifydb".to_string());
		let statement =
			credentials.get("statement").cloned().unwrap_or_else(|| "Sign in to ReifyDB".to_string());

		let issued_at =
			credentials.get("issued_at").cloned().unwrap_or_else(|| self.clock.now_secs().to_string());

		// Build SIWS-standard message
		let message = format!(
			"{domain} wants you to sign in with your Solana account:\n\
			 {address}\n\
			 \n\
			 {statement}\n\
			 \n\
			 Nonce: {nonce}\n\
			 Issued At: {issued_at}",
			domain = domain,
			address = public_key_b58,
			statement = statement,
			nonce = nonce,
			issued_at = issued_at,
		);

		Ok(AuthStep::Challenge {
			payload: HashMap::from([("message".into(), message), ("nonce".into(), nonce)]),
		})
	}
}

#[cfg(test)]
mod tests {
	use bs58::encode as bs58_encode;
	use ed25519_dalek::{Signer, SigningKey};
	use reifydb_runtime::context::clock::MockClock;

	use super::*;

	fn test_provider() -> SolanaProvider {
		let mock = MockClock::from_millis(1_700_000_000_000); // fixed timestamp
		SolanaProvider::new(Clock::Mock(mock))
	}

	fn test_keypair() -> (SigningKey, String) {
		let secret: [u8; 32] = [
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
			27, 28, 29, 30, 31, 32,
		];
		let signing_key = SigningKey::from_bytes(&secret);
		let public_key = signing_key.verifying_key();
		let public_key_b58 = bs58_encode(public_key.as_bytes()).into_string();
		(signing_key, public_key_b58)
	}

	#[test]
	fn test_create_stores_public_key() {
		let provider = test_provider();
		let (_, public_key_b58) = test_keypair();
		let config = HashMap::from([("public_key".to_string(), public_key_b58.clone())]);

		let stored = provider.create(&Rng::default(), &config).unwrap();
		assert_eq!(stored.get("public_key").unwrap(), &public_key_b58);
	}

	#[test]
	fn test_create_requires_public_key() {
		let provider = test_provider();
		assert!(provider.create(&Rng::default(), &HashMap::new()).is_err());
	}

	#[test]
	fn test_create_rejects_invalid_public_key() {
		let provider = test_provider();
		let config = HashMap::from([("public_key".to_string(), "not-valid-base58!!!".to_string())]);
		assert!(provider.create(&Rng::default(), &config).is_err());
	}

	#[test]
	fn test_create_rejects_wrong_length_key() {
		let provider = test_provider();
		// Only 16 bytes
		let short_key = bs58_encode(&[0u8; 16]).into_string();
		let config = HashMap::from([("public_key".to_string(), short_key)]);
		assert!(provider.create(&Rng::default(), &config).is_err());
	}

	#[test]
	fn test_challenge_response_flow() {
		let provider = test_provider();
		let (signing_key, public_key_b58) = test_keypair();
		let stored = HashMap::from([("public_key".to_string(), public_key_b58)]);

		// Step 1: Get challenge
		let step1 = provider.authenticate(&stored, &HashMap::new()).unwrap();
		let challenge_data = match step1 {
			AuthStep::Challenge {
				payload,
			} => payload,
			other => panic!("expected Challenge, got {:?}", other),
		};

		assert!(challenge_data.contains_key("message"));
		assert!(challenge_data.contains_key("nonce"));

		let message = challenge_data.get("message").unwrap();
		assert!(message.contains("wants you to sign in with your Solana account"));
		assert!(message.contains("Nonce:"));
		assert!(message.contains("Issued At: 1700000000"));

		// Step 2: Sign the message and verify
		let signature = signing_key.sign(message.as_bytes());
		let signature_b58 = bs58_encode(signature.to_bytes()).into_string();

		let credentials = HashMap::from([
			("signature".to_string(), signature_b58),
			("signed_message".to_string(), message.clone()),
		]);

		let step2 = provider.authenticate(&stored, &credentials).unwrap();
		assert_eq!(step2, AuthStep::Authenticated);
	}

	#[test]
	fn test_invalid_signature_fails() {
		let provider = test_provider();
		let (_, public_key_b58) = test_keypair();
		let stored = HashMap::from([("public_key".to_string(), public_key_b58)]);

		// Use a different key to sign
		let wrong_key = SigningKey::from_bytes(&[99u8; 32]);
		let signature = wrong_key.sign(b"some message");
		let signature_b58 = bs58_encode(signature.to_bytes()).into_string();

		let credentials = HashMap::from([
			("signature".to_string(), signature_b58),
			("signed_message".to_string(), "some message".to_string()),
		]);

		let step = provider.authenticate(&stored, &credentials).unwrap();
		assert_eq!(step, AuthStep::Failed);
	}
}
