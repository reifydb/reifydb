// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	extract::{Request, State},
	http::header::AUTHORIZATION,
	middleware::Next,
	response::Response,
};
use reifydb::IdentityId;

use crate::{error::ApiError, state::AppState};

#[derive(Clone, Copy)]
pub struct CurrentUser(pub IdentityId);

fn bearer_token(req: &Request) -> Option<String> {
	let header = req.headers().get(AUTHORIZATION)?.to_str().ok()?;
	let token = header.strip_prefix("Bearer ")?;
	if token.is_empty() {
		return None;
	}
	Some(token.to_string())
}

pub async fn require_auth(State(st): State<AppState>, mut req: Request, next: Next) -> Result<Response, ApiError> {
	let token = bearer_token(&req).ok_or(ApiError::Unauthorized)?;
	let auth = st.auth.clone();
	let validated =
		st.tokio.spawn_blocking(move || auth.validate_token(&token))
			.await
			.map_err(|e| ApiError::internal("token validation task failed", e))?;
	let token = validated.ok_or(ApiError::Unauthorized)?;
	req.extensions_mut().insert(CurrentUser(token.identity));
	Ok(next.run(req).await)
}

pub fn valid_email(email: &str) -> bool {
	if email.is_empty() || email.len() > 254 {
		return false;
	}
	if !email.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '%' | '+' | '-' | '@')) {
		return false;
	}
	let mut parts = email.split('@');
	let (Some(local), Some(domain), None) = (parts.next(), parts.next(), parts.next()) else {
		return false;
	};
	!local.is_empty()
		&& !domain.is_empty()
		&& domain.contains('.')
		&& !domain.starts_with('.')
		&& !domain.ends_with('.')
}

#[cfg(test)]
mod tests {
	use super::valid_email;

	#[test]
	fn accepts_common_addresses() {
		assert!(valid_email("user@example.com"));
		assert!(valid_email("first.last+tag@sub.example.co"));
		assert!(valid_email("a_b-c%d@example.io"));
	}

	#[test]
	fn rejects_malformed_addresses() {
		assert!(!valid_email(""));
		assert!(!valid_email("no-at-sign.example.com"));
		assert!(!valid_email("two@@example.com"));
		assert!(!valid_email("a@b@c.com"));
		assert!(!valid_email("@example.com"));
		assert!(!valid_email("user@"));
		assert!(!valid_email("user@nodot"));
		assert!(!valid_email("user@.leading.dot"));
		assert!(!valid_email("user@trailing.dot."));
	}

	#[test]
	fn rejects_rql_hostile_characters() {
		// The email is embedded as a backtick identifier in CREATE USER, so
		// backticks, quotes, whitespace, and braces must never validate.
		assert!(!valid_email("user`x@example.com"));
		assert!(!valid_email("user\"x@example.com"));
		assert!(!valid_email("user'x@example.com"));
		assert!(!valid_email("user x@example.com"));
		assert!(!valid_email("user{x}@example.com"));
		assert!(!valid_email("user;x@example.com"));
	}
}
