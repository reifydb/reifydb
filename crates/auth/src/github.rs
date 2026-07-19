// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::disallowed_types)]
use std::time::Duration;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reifydb_value::{Result, error::Error};
#[cfg(not(target_arch = "wasm32"))]
use reqwest::{
	blocking::Client,
	header::{ACCEPT, USER_AGENT},
};
#[cfg(not(target_arch = "wasm32"))]
use serde_json::{Value as JsonValue, json};

use crate::error::GithubError;

#[derive(Clone)]
pub struct GithubConfig {
	pub client_id: String,
	pub client_secret: String,
	pub redirect_uri: String,
}

impl fmt::Debug for GithubConfig {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("GithubConfig")
			.field("client_id", &self.client_id)
			.field("client_secret", &"<redacted>")
			.field("redirect_uri", &self.redirect_uri)
			.finish()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubUser {
	pub id: u64,
	pub login: String,
}

pub trait GithubApi: Send + Sync {
	fn exchange_code(&self, config: &GithubConfig, code: &str) -> Result<String>;

	fn fetch_user(&self, access_token: &str) -> Result<GithubUser>;
}

pub fn build_authorize_url(config: &GithubConfig, state: &str) -> String {
	format!(
		"https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&state={}",
		utf8_percent_encode(&config.client_id, NON_ALPHANUMERIC),
		utf8_percent_encode(&config.redirect_uri, NON_ALPHANUMERIC),
		utf8_percent_encode(state, NON_ALPHANUMERIC),
	)
}

#[cfg(not(target_arch = "wasm32"))]
pub struct HttpGithubApi;

#[cfg(not(target_arch = "wasm32"))]
impl HttpGithubApi {
	#[allow(clippy::disallowed_types)]
	fn client(&self) -> Result<Client> {
		Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.timeout(Duration::from_secs(10))
			.build()
			.map_err(|e| api_failed(e.to_string()))
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl GithubApi for HttpGithubApi {
	fn exchange_code(&self, config: &GithubConfig, code: &str) -> Result<String> {
		let response = self
			.client()?
			.post("https://github.com/login/oauth/access_token")
			.header(ACCEPT, "application/json")
			.header(USER_AGENT, "reifydb")
			.json(&json!({
				"client_id": config.client_id,
				"client_secret": config.client_secret,
				"code": code,
				"redirect_uri": config.redirect_uri,
			}))
			.send()
			.map_err(|e| exchange_failed(e.to_string()))?;

		let body: JsonValue = response.json().map_err(|e| exchange_failed(e.to_string()))?;
		if let Some(token) = body.get("access_token").and_then(JsonValue::as_str) {
			return Ok(token.to_string());
		}

		let reason = body
			.get("error_description")
			.or_else(|| body.get("error"))
			.and_then(JsonValue::as_str)
			.unwrap_or("missing access_token in response")
			.to_string();
		Err(exchange_failed(reason))
	}

	fn fetch_user(&self, access_token: &str) -> Result<GithubUser> {
		let response = self
			.client()?
			.get("https://api.github.com/user")
			.bearer_auth(access_token)
			.header(ACCEPT, "application/vnd.github+json")
			.header(USER_AGENT, "reifydb")
			.send()
			.map_err(|e| api_failed(e.to_string()))?;

		let status = response.status();
		if !status.is_success() {
			return Err(api_failed(format!("unexpected status {}", status)));
		}

		let body: JsonValue = response.json().map_err(|e| api_failed(e.to_string()))?;
		let id = body
			.get("id")
			.and_then(JsonValue::as_u64)
			.ok_or_else(|| api_failed("missing numeric id in user response".to_string()))?;
		let login = body.get("login").and_then(JsonValue::as_str).unwrap_or_default().to_string();

		Ok(GithubUser {
			id,
			login,
		})
	}
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange_failed(reason: String) -> Error {
	Error::from(GithubError::ExchangeFailed {
		reason,
	})
}

#[cfg(not(target_arch = "wasm32"))]
fn api_failed(reason: String) -> Error {
	Error::from(GithubError::ApiFailed {
		reason,
	})
}

#[cfg(target_arch = "wasm32")]
pub struct UnsupportedGithubApi;

#[cfg(target_arch = "wasm32")]
impl GithubApi for UnsupportedGithubApi {
	fn exchange_code(&self, _config: &GithubConfig, _code: &str) -> Result<String> {
		Err(unsupported())
	}

	fn fetch_user(&self, _access_token: &str) -> Result<GithubUser> {
		Err(unsupported())
	}
}

#[cfg(target_arch = "wasm32")]
fn unsupported() -> Error {
	Error::from(GithubError::ApiFailed {
		reason: "github authentication is not available in this build".to_string(),
	})
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn default_api() -> std::sync::Arc<dyn GithubApi> {
	std::sync::Arc::new(HttpGithubApi)
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn default_api() -> std::sync::Arc<dyn GithubApi> {
	std::sync::Arc::new(UnsupportedGithubApi)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_config() -> GithubConfig {
		GithubConfig {
			client_id: "Iv1.abc123".to_string(),
			client_secret: "super-secret-value".to_string(),
			redirect_uri: "http://localhost:8080/auth/github/callback?next=/dashboard".to_string(),
		}
	}

	#[test]
	fn test_authorize_url_encodes_redirect_uri() {
		let url = build_authorize_url(&test_config(), "abc123");

		// The redirect uri must arrive as ONE query parameter: raw ':', '/', '?', '&'
		// would split it and let it inject extra parameters into the authorize URL.
		assert!(url.starts_with("https://github.com/login/oauth/authorize?"));
		assert!(url.contains(
			"redirect_uri=http%3A%2F%2Flocalhost%3A8080%2Fauth%2Fgithub%2Fcallback%3Fnext%3D%2Fdashboard"
		));
		assert!(!url.contains("callback?next"));
	}

	#[test]
	fn test_authorize_url_contains_client_id_and_state() {
		let url = build_authorize_url(&test_config(), "state-nonce-42");

		assert!(url.contains("client_id=Iv1%2Eabc123"));
		assert!(url.contains("state=state%2Dnonce%2D42"));
	}

	#[test]
	fn test_authorize_url_never_leaks_client_secret() {
		// The authorize URL is handed to the browser; the secret must stay server-side.
		let url = build_authorize_url(&test_config(), "abc123");
		assert!(!url.contains("secret"));
	}

	#[test]
	fn test_debug_redacts_client_secret() {
		// GithubConfig is embedded in AuthServiceConfig which derives Debug and may be
		// logged; the secret must never appear in that output.
		let rendered = format!("{:?}", test_config());
		assert!(rendered.contains("<redacted>"));
		assert!(!rendered.contains("super-secret-value"));
	}
}
