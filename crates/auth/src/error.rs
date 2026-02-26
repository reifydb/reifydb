// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
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

impl IntoDiagnostic for AuthError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			AuthError::PasswordRequired => Diagnostic {
				code: "AU_001".to_string(),
				statement: None,
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
				statement: None,
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
				statement: None,
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
				statement: None,
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
				statement: None,
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
				statement: None,
				message: format!("unknown authentication method: {}", method),
				fragment: Fragment::None,
				label: Some("unknown method".to_string()),
				help: Some("supported authentication methods are: password, token".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			AuthError::HashingFailed {
				reason,
			} => Diagnostic {
				code: "AU_007".to_string(),
				statement: None,
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
				statement: None,
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
				statement: None,
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
		Error(err.into_diagnostic())
	}
}
