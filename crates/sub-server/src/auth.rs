// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt};

use reifydb_auth::service::AuthService;
use reifydb_type::value::identity::IdentityId;

/// Authentication error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
	/// The authorization header is malformed or contains invalid UTF-8.
	InvalidHeader,
	/// No credentials were provided (no Authorization header or auth token).
	MissingCredentials,
	/// The provided token is invalid or cannot be verified.
	InvalidToken,
	/// The token has expired.
	Expired,
	/// The token is valid but the user lacks required permissions.
	InsufficientPermissions,
}

impl fmt::Display for AuthError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AuthError::InvalidHeader => write!(f, "Invalid authorization header"),
			AuthError::MissingCredentials => write!(f, "Authentication required"),
			AuthError::InvalidToken => write!(f, "Invalid authentication token"),
			AuthError::Expired => write!(f, "Authentication token expired"),
			AuthError::InsufficientPermissions => write!(f, "Insufficient permissions"),
		}
	}
}

impl StdError for AuthError {}

/// Result type for authentication operations.
pub type AuthResult<T> = Result<T, AuthError>;

/// Extract identity from HTTP Authorization header value.
///
/// Supports the following authentication schemes:
/// - `Bearer <token>` - Session token validated against AuthService
/// - `Basic <base64>` - Basic authentication (username:password)
pub fn extract_identity_from_auth_header(auth_service: &AuthService, auth_header: &str) -> AuthResult<IdentityId> {
	if let Some(token) = auth_header.strip_prefix("Bearer ") {
		validate_bearer_token(auth_service, token.trim())
	} else if let Some(credentials) = auth_header.strip_prefix("Basic ") {
		validate_basic_auth(auth_service, credentials.trim())
	} else {
		Err(AuthError::InvalidHeader)
	}
}

/// Extract identity from WebSocket authentication message.
///
/// Called when a WebSocket client sends an Auth message with a token.
pub fn extract_identity_from_ws_auth(auth_service: &AuthService, token: Option<&str>) -> AuthResult<IdentityId> {
	match token {
		Some(t) if !t.is_empty() => validate_bearer_token(auth_service, t),
		_ => Ok(IdentityId::anonymous()),
	}
}

/// Validate a bearer token and return the associated identity.
fn validate_bearer_token(auth_service: &AuthService, token: &str) -> AuthResult<IdentityId> {
	if token.is_empty() {
		return Err(AuthError::InvalidToken);
	}

	match auth_service.validate_token(token) {
		Some(session) => Ok(session.identity),
		None => Err(AuthError::InvalidToken),
	}
}

/// Validate basic authentication credentials (Base64-encoded username:password).
fn validate_basic_auth(_auth_service: &AuthService, _credentials: &str) -> AuthResult<IdentityId> {
	// TODO: Implement Basic auth (Base64 decode → username:password → auth_service.authenticate)
	Err(AuthError::InvalidToken)
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
}
