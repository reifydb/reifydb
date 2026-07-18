// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
	#[error("password is required for password authentication")]
	PasswordRequired,

	#[error("stored authentication is missing hash")]
	MissingHash,

	#[error("stored authentication is missing salt")]
	MissingSalt,

	#[error("stored authentication is missing token")]
	MissingToken,

	#[error("unknown authentication method: {method}")]
	UnknownMethod {
		method: String,
	},

	#[error("failed to serialize authentication properties: {reason}")]
	SerializeProperties {
		reason: String,
	},

	#[error("password hashing failed: {reason}")]
	HashingFailed {
		reason: String,
	},

	#[error("stored hash is invalid or corrupted: {reason}")]
	InvalidHash {
		reason: String,
	},

	#[error("password verification failed: {reason}")]
	VerificationFailed {
		reason: String,
	},
}

#[derive(Debug, thiserror::Error)]
pub enum SolanaError {
	#[error("public key is required for solana authentication")]
	MissingPublicKey,

	#[error("invalid public key: {reason}")]
	InvalidPublicKey {
		reason: String,
	},

	#[error("invalid signature: {reason}")]
	InvalidSignature {
		reason: String,
	},
}

#[derive(Debug, thiserror::Error)]
pub enum GithubError {
	#[error("user_id is required for github authentication")]
	MissingUserId,

	#[error("invalid github user_id: {reason}")]
	InvalidUserId {
		reason: String,
	},

	#[error("github oauth code exchange failed: {reason}")]
	ExchangeFailed {
		reason: String,
	},

	#[error("github api request failed: {reason}")]
	ApiFailed {
		reason: String,
	},
}

impl IntoDiagnostic for AuthError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			AuthError::PasswordRequired => Diagnostic {
				code: "AU_001".to_string(),
				rql: None,
				message: "password is required for password authentication".to_string(),
				fragment: Fragment::None,
				label: Some("missing password".to_string()),
				help: Some("provide a password in the authentication configuration".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::MissingHash => Diagnostic {
				code: "AU_002".to_string(),
				rql: None,
				message: "stored authentication is missing hash".to_string(),
				fragment: Fragment::None,
				label: Some("missing hash".to_string()),
				help: Some("the stored authentication record is corrupted or incomplete".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::MissingSalt => Diagnostic {
				code: "AU_003".to_string(),
				rql: None,
				message: "stored authentication is missing salt".to_string(),
				fragment: Fragment::None,
				label: Some("missing salt".to_string()),
				help: Some("the stored authentication record is corrupted or incomplete".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::MissingToken => Diagnostic {
				code: "AU_004".to_string(),
				rql: None,
				message: "stored authentication is missing token".to_string(),
				fragment: Fragment::None,
				label: Some("missing token".to_string()),
				help: Some("the stored authentication record is corrupted or incomplete".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::SerializeProperties {
				reason,
			} => Diagnostic {
				code: "AU_006".to_string(),
				rql: None,
				message: format!("failed to serialize authentication properties: {}", reason),
				fragment: Fragment::None,
				label: Some("serialization failed".to_string()),
				help: Some("ensure authentication properties are valid".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::UnknownMethod {
				method,
			} => Diagnostic {
				code: "AU_005".to_string(),
				rql: None,
				message: format!("unknown authentication method: {}", method),
				fragment: Fragment::None,
				label: Some("unknown method".to_string()),
				help: Some(
					"supported authentication methods are: password, token, solana, github"
						.to_string(),
				),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::HashingFailed {
				reason,
			} => Diagnostic {
				code: "AU_007".to_string(),
				rql: None,
				message: format!("password hashing failed: {}", reason),
				fragment: Fragment::None,
				label: Some("hashing failed".to_string()),
				help: Some("an internal error occurred during password hashing".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::InvalidHash {
				reason,
			} => Diagnostic {
				code: "AU_008".to_string(),
				rql: None,
				message: format!("stored hash is invalid or corrupted: {}", reason),
				fragment: Fragment::None,
				label: Some("invalid hash".to_string()),
				help: Some("the stored authentication record is corrupted or incomplete".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::VerificationFailed {
				reason,
			} => Diagnostic {
				code: "AU_009".to_string(),
				rql: None,
				message: format!("password verification failed: {}", reason),
				fragment: Fragment::None,
				label: Some("verification failed".to_string()),
				help: Some("an internal error occurred during password verification".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

		}
	}
}

impl From<AuthError> for Error {
	fn from(err: AuthError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for SolanaError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			SolanaError::MissingPublicKey => Diagnostic {
				code: "AU_010".to_string(),
				rql: None,
				message: "public key is required for solana authentication".to_string(),
				fragment: Fragment::None,
				label: Some("missing public key".to_string()),
				help: Some("provide a public_key in the authentication configuration".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			SolanaError::InvalidPublicKey {
				reason,
			} => Diagnostic {
				code: "AU_011".to_string(),
				rql: None,
				message: format!("invalid public key: {}", reason),
				fragment: Fragment::None,
				label: Some("invalid public key".to_string()),
				help: Some("provide a valid base58-encoded 32-byte Solana public key".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			SolanaError::InvalidSignature {
				reason,
			} => Diagnostic {
				code: "AU_012".to_string(),
				rql: None,
				message: format!("invalid signature: {}", reason),
				fragment: Fragment::None,
				label: Some("invalid signature".to_string()),
				help: Some("provide a valid base58-encoded 64-byte ed25519 signature".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<SolanaError> for Error {
	fn from(err: SolanaError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

impl IntoDiagnostic for GithubError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			GithubError::MissingUserId => Diagnostic {
				code: "AU_013".to_string(),
				rql: None,
				message: "user_id is required for github authentication".to_string(),
				fragment: Fragment::None,
				label: Some("missing user_id".to_string()),
				help: Some("provide a user_id in the authentication configuration".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			GithubError::InvalidUserId {
				reason,
			} => Diagnostic {
				code: "AU_014".to_string(),
				rql: None,
				message: format!("invalid github user_id: {}", reason),
				fragment: Fragment::None,
				label: Some("invalid user_id".to_string()),
				help: Some("provide the numeric github account id as user_id".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			GithubError::ExchangeFailed {
				reason,
			} => Diagnostic {
				code: "AU_015".to_string(),
				rql: None,
				message: format!("github oauth code exchange failed: {}", reason),
				fragment: Fragment::None,
				label: Some("code exchange failed".to_string()),
				help: Some(
					"verify the github oauth client id, client secret, and redirect uri"
						.to_string(),
				),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			GithubError::ApiFailed {
				reason,
			} => Diagnostic {
				code: "AU_016".to_string(),
				rql: None,
				message: format!("github api request failed: {}", reason),
				fragment: Fragment::None,
				label: Some("github api failed".to_string()),
				help: Some("github may be unreachable; retry the sign-in".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<GithubError> for Error {
	fn from(err: GithubError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
