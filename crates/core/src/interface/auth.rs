// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_runtime::context::rng::Rng;
use reifydb_type::Result;

/// Result of a single authentication step.
///
/// Authentication may complete in one step (password, API token) or require
/// multiple round-trips (challenge-response flows like wallet signing, WebAuthn).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStep {
	/// Authentication succeeded.
	Authenticated,
	/// Credentials were invalid.
	Failed,
	/// The provider needs the client to respond to a challenge.
	/// The `data` map contains provider-specific challenge material
	/// (e.g., a nonce for the client to sign).
	Challenge {
		data: HashMap<String, String>,
	},
}

pub trait AuthenticationProvider: Send + Sync {
	/// The method name this provider handles (e.g., "password", "token", "solana").
	fn method(&self) -> &str;

	/// Create stored credentials from configuration.
	/// Called during `CREATE AUTHENTICATION ... FOR USER ...`.
	/// The `rng` parameter provides deterministic randomness in test mode.
	fn create(&self, rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>>;

	/// Authenticate a user given their stored credentials and the presented credentials.
	///
	/// For single-step methods (password, token), this returns `Authenticated` or `Failed`.
	/// For challenge-response methods, this may return `Challenge` with data the client
	/// must respond to, followed by a second call with the response.
	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep>;
}
