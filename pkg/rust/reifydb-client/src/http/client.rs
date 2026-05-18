// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
use std::{collections::HashMap, time::Duration};

use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
	value::frame::frame::Frame,
};
use reifydb_wire_format::{decode::decode_frames, json::types::ResponseFrame};
use reqwest::{Client as ReqwestClient, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json};

use crate::{
	AdminRequest, AdminResponse, AdminResult, CommandRequest, CommandResponse, CommandResult, ErrResponse,
	LoginResult, QueryRequest, QueryResponse, QueryResult, Response, ResponseMeta, ResponsePayload, WireFormat,
	params_to_wire,
	session::{parse_admin_response, parse_command_response, parse_query_response},
};

/// HTTP-specific response format (server returns `{ "frames": [...] }`)
#[derive(Debug, Deserialize)]
struct HttpFrameResponse {
	frames: Vec<ResponseFrame>,
}

impl HttpFrameResponse {
	fn into_admin(self, meta: Option<ResponseMeta>) -> AdminResponse {
		AdminResponse {
			content_type: "application/vnd.reifydb.frames".to_string(),
			body: json!({ "frames": self.frames }),
			meta,
		}
	}

	fn into_command(self, meta: Option<ResponseMeta>) -> CommandResponse {
		CommandResponse {
			content_type: "application/vnd.reifydb.frames".to_string(),
			body: json!({ "frames": self.frames }),
			meta,
		}
	}

	fn into_query(self, meta: Option<ResponseMeta>) -> QueryResponse {
		QueryResponse {
			content_type: "application/vnd.reifydb.frames".to_string(),
			body: json!({ "frames": self.frames }),
			meta,
		}
	}
}

fn extract_meta(headers: &HeaderMap) -> Option<ResponseMeta> {
	let fingerprint = headers.get("x-fingerprint").and_then(|v| v.to_str().ok())?;
	let duration = headers.get("x-duration").and_then(|v| v.to_str().ok())?;
	Some(ResponseMeta {
		fingerprint: fingerprint.to_string(),
		duration: duration.to_string(),
	})
}

/// HTTP-specific error response matching the server's format
#[derive(Debug, Deserialize)]
struct HttpErrorResponse {
	code: String,
	error: String,
	#[serde(default)]
	diagnostic: Option<Diagnostic>,
}

/// HTTP authentication response matching the server's `/v1/authenticate` format
#[derive(Debug, Deserialize)]
struct HttpAuthenticateResponse {
	status: String,
	token: Option<String>,
	identity: Option<String>,
	reason: Option<String>,
}

/// Async HTTP client for ReifyDB
#[derive(Clone)]
pub struct HttpClient {
	inner: ReqwestClient,
	base_url: String,
	token: Option<String>,
	format: WireFormat,
}

impl HttpClient {
	/// Create a new HTTP client connected to the given URL.
	///
	/// # Arguments
	/// * `url` - Base URL of the ReifyDB server (e.g., "http://localhost:8080")
	/// * `format` - Wire format for responses
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		if format == WireFormat::Proto {
			return Err(Error(Box::new(Diagnostic {
				code: "INVALID_FORMAT".to_string(),
				message: "WireFormat::Proto is not supported for HttpClient".to_string(),
				..Default::default()
			})));
		}

		let inner = ReqwestClient::builder().timeout(Duration::from_secs(30)).build().unwrap(); // FIXME better error handling

		// Normalize URL (remove trailing slash)
		let base_url = url.trim_end_matches('/').to_string();

		Ok(Self {
			inner,
			base_url,
			token: None,
			format,
		})
	}

	/// Create a new HTTP client using an existing reqwest Client for connection pooling.
	///
	/// # Arguments
	/// * `client` - Shared reqwest Client instance
	/// * `url` - Base URL of the ReifyDB server
	/// * `format` - Wire format for responses
	pub fn with_client(client: ReqwestClient, url: &str, format: WireFormat) -> Result<Self, Error> {
		if format == WireFormat::Proto {
			return Err(Error(Box::new(Diagnostic {
				code: "INVALID_FORMAT".to_string(),
				message: "WireFormat::Proto is not supported for HttpClient".to_string(),
				..Default::default()
			})));
		}

		let base_url = url.trim_end_matches('/').to_string();
		Ok(Self {
			inner: client,
			base_url,
			token: None,
			format,
		})
	}

	/// Set the authentication token for subsequent requests.
	///
	/// # Arguments
	/// * `token` - Bearer token for authentication
	pub fn authenticate(&mut self, token: &str) {
		self.token = Some(token.to_string());
	}

	pub async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("identifier".to_string(), identifier.to_string());
		credentials.insert("password".to_string(), password.to_string());
		self.login("password", credentials).await
	}

	pub async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("token".to_string(), token.to_string());
		self.login("token", credentials).await
	}

	pub async fn login(
		&mut self,
		method: &str,
		credentials: HashMap<String, String>,
	) -> Result<LoginResult, Error> {
		let body = json!({
			"method": method,
			"credentials": credentials
		});

		let url = format!("{}/v1/authenticate", self.base_url);
		let response = self.inner.post(&url).json(&body).send().await.unwrap(); // FIXME better error handling
		let response_body = response.text().await.unwrap(); // FIXME better error handling

		let auth: HttpAuthenticateResponse = from_str(&response_body).unwrap(); // FIXME better error handling

		if auth.status == "authenticated" {
			let token = auth.token.unwrap_or_default();
			let identity = auth.identity.unwrap_or_default();
			self.token = Some(token.clone());
			Ok(LoginResult {
				token,
				identity,
			})
		} else {
			let reason = auth.reason.unwrap_or_else(|| "Authentication failed".to_string());
			panic!("Authentication failed: {}", reason) // FIXME better error handling
		}
	}

	/// Logout from the server, revoking the current session token.
	pub async fn logout(&mut self) -> Result<(), Error> {
		let token = match self.token.as_ref() {
			Some(t) => t.clone(),
			None => return Ok(()),
		};

		let url = format!("{}/v1/logout", self.base_url);
		let response = self.inner.post(&url).bearer_auth(&token).send().await.unwrap(); // FIXME better error handling

		let status = response.status();
		if status.is_success() {
			self.token = None;
			Ok(())
		} else {
			let body = response.text().await.unwrap(); // FIXME better error handling
			Err(self.parse_error_response(&body))
		}
	}

	/// Execute an admin (DDL + DML + Query) statement.
	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.admin_with_meta(rql, params).await?.frames)
	}

	/// Execute an admin statement and return frames together with server-reported metadata.
	pub async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = AdminRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_wire),
			format: None,
		};

		if self.format == WireFormat::Rbcf {
			let (frames, meta) = self.send_rbcf("/v1/admin", &request).await?;
			return Ok(AdminResult {
				frames,
				meta,
			});
		}

		let response = self.send_admin(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Admin(response),
		};
		parse_admin_response(ws_response)
	}

	/// Execute a command (write) statement.
	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.command_with_meta(rql, params).await?.frames)
	}

	/// Execute a command statement and return frames together with server-reported metadata.
	pub async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = CommandRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_wire),
			format: None,
		};

		if self.format == WireFormat::Rbcf {
			let (frames, meta) = self.send_rbcf("/v1/command", &request).await?;
			return Ok(CommandResult {
				frames,
				meta,
			});
		}

		let response = self.send_command(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Command(response),
		};
		parse_command_response(ws_response)
	}

	/// Execute a query (read) statement.
	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.query_with_meta(rql, params).await?.frames)
	}

	/// Execute a query statement and return frames together with server-reported metadata.
	pub async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = QueryRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_wire),
			format: None,
		};

		if self.format == WireFormat::Rbcf {
			let (frames, meta) = self.send_rbcf("/v1/query", &request).await?;
			return Ok(QueryResult {
				frames,
				meta,
			});
		}

		let response = self.send_query(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Query(response),
		};
		parse_query_response(ws_response)
	}

	/// Send an admin request to the server.
	async fn send_admin(&self, request: &AdminRequest) -> Result<AdminResponse, Error> {
		let url = format!("{}/v1/admin?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_admin(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send a command request to the server.
	async fn send_command(&self, request: &CommandRequest) -> Result<CommandResponse, Error> {
		let url = format!("{}/v1/command?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_command(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send a query request to the server.
	async fn send_query(&self, request: &QueryRequest) -> Result<QueryResponse, Error> {
		let url = format!("{}/v1/query?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_query(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send an RBCF request: append ?format=rbcf, decode binary response.
	async fn send_rbcf<T: Serialize>(
		&self,
		path: &str,
		body: &T,
	) -> Result<(Vec<Frame>, Option<ResponseMeta>), Error> {
		let url = format!("{}{}?format=rbcf", self.base_url, path);
		let (bytes, meta) = self.send_request_bytes(&url, body).await?;
		let frames = decode_frames(&bytes).map_err(|e| {
			Error(Box::new(Diagnostic {
				code: "RBCF_DECODE".to_string(),
				message: format!("Failed to decode RBCF response: {}", e),
				..Default::default()
			}))
		})?;
		Ok((frames, meta))
	}

	/// Send an HTTP POST request and return the response body as text plus extracted meta.
	async fn send_request<T: Serialize>(
		&self,
		url: &str,
		body: &T,
	) -> Result<(String, Option<ResponseMeta>), Error> {
		let mut request = self.inner.post(url).json(body);

		if let Some(ref token) = self.token {
			request = request.bearer_auth(token);
		}

		let response = request.send().await.unwrap(); // FIXME better error handling
		let meta = extract_meta(response.headers());
		Ok((response.text().await.unwrap(), meta)) // FIXME better error handling
	}

	/// Send an HTTP POST request and return the response body as bytes plus extracted meta.
	async fn send_request_bytes<T: Serialize>(
		&self,
		url: &str,
		body: &T,
	) -> Result<(Vec<u8>, Option<ResponseMeta>), Error> {
		let mut request = self.inner.post(url).json(body);

		if let Some(ref token) = self.token {
			request = request.bearer_auth(token);
		}

		let response = request.send().await.unwrap(); // FIXME better error handling

		if !response.status().is_success() {
			let body = response.text().await.unwrap();
			return Err(self.parse_error_response(&body));
		}

		let meta = extract_meta(response.headers());
		Ok((response.bytes().await.unwrap().to_vec(), meta)) // FIXME better error handling
	}

	/// Parse an error response body into an Error.
	fn parse_error_response(&self, body: &str) -> Error {
		// Try parsing as HTTP error response format
		if let Ok(http_err) = from_str::<HttpErrorResponse>(body) {
			let diag = http_err.diagnostic.unwrap_or_else(|| Diagnostic {
				code: http_err.code,
				message: http_err.error,
				..Default::default()
			});
			return Error(Box::new(diag));
		}

		// Try parsing as diagnostic error response
		if let Ok(err_response) = from_str::<ErrResponse>(body) {
			return Error(Box::new(err_response.diagnostic));
		}

		// Fallback: return raw response as error
		panic!("Failed to parse response: {}", body) // FIXME better error handling
	}
}
