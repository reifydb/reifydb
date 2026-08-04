// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{collections::HashMap, time::Duration as StdDuration};

use reifydb_codec::{frame::decode::decode_frames, json::types::ResponseFrame};
use reifydb_value::{
	error::{Diagnostic, Error},
	fragment::Fragment,
	params::Params,
	value::{duration::Duration, frame::frame::Frame, temporal::parse::duration::parse_duration},
};
use reqwest::{Client as ReqwestClient, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json};

use crate::{
	AdminRequest, AdminResponse, AdminResult, CommandRequest, CommandResponse, CommandResult, ErrResponse,
	LoginResult, QueryRequest, QueryResponse, QueryResult, QueueClaimRequest, Response, ResponseMeta,
	ResponsePayload, WireFormat,
	error::ClientError,
	params_to_wire,
	session::{parse_admin_response, parse_command_response, parse_query_response},
};

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

#[derive(Clone)]
pub struct HttpClient {
	inner: ReqwestClient,
	base_url: String,
	token: Option<String>,
	format: WireFormat,
}

impl HttpClient {
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		let inner =
			ReqwestClient::builder().timeout(Duration::from_seconds(30).unwrap().to_std()).build().unwrap(); // FIXME better error handling

		let base_url = url.trim_end_matches('/').to_string();

		Ok(Self {
			inner,
			base_url,
			token: None,
			format,
		})
	}

	/// Shares an existing reqwest Client so connections are pooled across clients.
	pub fn with_client(client: ReqwestClient, url: &str, format: WireFormat) -> Result<Self, Error> {
		let base_url = url.trim_end_matches('/').to_string();
		Ok(Self {
			inner: client,
			base_url,
			token: None,
			format,
		})
	}

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
			Err(ClientError::NotAuthenticated(reason).into())
		}
	}

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

	async fn send_admin(&self, request: &AdminRequest) -> Result<AdminResponse, Error> {
		let url = format!("{}/v1/admin?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_admin(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	async fn send_command(&self, request: &CommandRequest) -> Result<CommandResponse, Error> {
		let url = format!("{}/v1/command?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_command(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	async fn send_query(&self, request: &QueryRequest) -> Result<QueryResponse, Error> {
		let url = format!("{}/v1/query?format=frames", self.base_url);
		let (response_body, meta) = self.send_request(&url, request).await?;

		match from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_query(meta)),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send an RBCF request: append ?format=rbcf, decode binary response.
	/// Claim items from a queue, optionally long-polling until work arrives or the budget expires.
	///
	/// `wait_for` and `lease_ttl` are RQL duration literals such as `"25s"`. An absent or zero
	/// `wait_for` is a plain non-blocking claim.
	pub async fn queue_claim(&self, request: QueueClaimRequest) -> Result<Vec<Frame>, Error> {
		let budget = request.wait_for.as_deref().and_then(|raw| parse_duration(Fragment::internal(raw)).ok());
		let timeout = budget.unwrap_or(Duration::zero()).to_std() + StdDuration::from_secs(30);

		let url = format!("{}/v1/queue/claim?format=rbcf", self.base_url);
		let (bytes, _) = self.send_request_bytes_with_timeout(&url, &request, timeout).await?;
		decode_frames(&bytes)
			.map_err(|e| ClientError::Decode(format!("Failed to decode RBCF response: {}", e)).into())
	}

	async fn send_request_bytes_with_timeout<T: Serialize>(
		&self,
		url: &str,
		body: &T,
		timeout: StdDuration,
	) -> Result<(Vec<u8>, Option<ResponseMeta>), Error> {
		let mut request = self.inner.post(url).timeout(timeout).json(body);

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

	async fn send_rbcf<T: Serialize>(
		&self,
		path: &str,
		body: &T,
	) -> Result<(Vec<Frame>, Option<ResponseMeta>), Error> {
		let url = format!("{}{}?format=rbcf", self.base_url, path);
		let (bytes, meta) = self.send_request_bytes(&url, body).await?;
		let frames = decode_frames(&bytes)
			.map_err(|e| ClientError::Decode(format!("Failed to decode RBCF response: {}", e)))?;
		Ok((frames, meta))
	}

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

	fn parse_error_response(&self, body: &str) -> Error {
		if let Ok(http_err) = from_str::<HttpErrorResponse>(body) {
			let diag = http_err.diagnostic.unwrap_or_else(|| Diagnostic {
				code: http_err.code,
				message: http_err.error,
				..Default::default()
			});
			return Error(Box::new(diag));
		}

		if let Ok(err_response) = from_str::<ErrResponse>(body) {
			return Error(Box::new(err_response.diagnostic));
		}

		panic!("Failed to parse response: {}", body) // FIXME better error handling
	}
}
