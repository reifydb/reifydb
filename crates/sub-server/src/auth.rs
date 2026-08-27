// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error as StdError, fmt};

use reifydb_auth::service::AuthService;
use reifydb_value::value::identity::IdentityId;
use tracing::error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
	InvalidHeader,

	MissingCredentials,

	InvalidToken,

	Expired,

	InsufficientPermissions,

	Internal,
}

impl fmt::Display for AuthError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AuthError::InvalidHeader => write!(f, "Invalid authorization header"),
			AuthError::MissingCredentials => write!(f, "Authentication required"),
			AuthError::InvalidToken => write!(f, "Invalid authentication token"),
			AuthError::Expired => write!(f, "Authentication token expired"),
			AuthError::InsufficientPermissions => write!(f, "Insufficient permissions"),
			AuthError::Internal => write!(f, "Authentication could not be completed"),
		}
	}
}

impl StdError for AuthError {}

pub type AuthResult<T> = Result<T, AuthError>;

pub fn extract_identity_from_auth_header(auth_service: &AuthService, auth_header: &str) -> AuthResult<IdentityId> {
	match auth_header.strip_prefix("Bearer ") {
		Some(token) => validate_bearer_token(auth_service, token.trim()),
		None => Err(AuthError::InvalidHeader),
	}
}

pub fn extract_identity_from_ws_auth(auth_service: &AuthService, token: Option<&str>) -> AuthResult<IdentityId> {
	match token {
		Some(t) if !t.is_empty() => validate_bearer_token(auth_service, t),
		_ => Ok(IdentityId::anonymous()),
	}
}

fn validate_bearer_token(auth_service: &AuthService, token: &str) -> AuthResult<IdentityId> {
	if token.is_empty() {
		return Err(AuthError::InvalidToken);
	}

	match auth_service.validate_token(token) {
		Ok(Some(session)) => Ok(session.identity),
		Ok(None) => Err(AuthError::InvalidToken),
		Err(e) => {
			error!("token validation could not reach storage, rejecting the request: {e}");
			Err(AuthError::Internal)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_auth_error_display() {
		assert_eq!(AuthError::InvalidHeader.to_string(), "Invalid authorization header");
		assert_eq!(AuthError::MissingCredentials.to_string(), "Authentication required");
		assert_eq!(AuthError::InvalidToken.to_string(), "Invalid authentication token");
		assert_eq!(AuthError::Expired.to_string(), "Authentication token expired");
	}

	#[test]
	fn test_anonymous_identity() {
		let identity = IdentityId::anonymous();
		assert!(identity.is_anonymous());
	}

	#[test]
	fn test_root_identity() {
		let identity = IdentityId::root();
		assert!(identity.is_root());
	}

	#[test]
	fn internal_does_not_read_as_a_client_mistake() {
		// The gRPC status carries this text verbatim, so it must not send the caller after its credentials.
		let message = AuthError::Internal.to_string();
		assert_eq!(message, "Authentication could not be completed");
		assert!(!message.contains("token"), "a storage failure must not point the client at its token");
	}
}
