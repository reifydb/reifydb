// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
use std::{collections::HashMap, time::Duration};

use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
};
use reqwest::Client as ReqwestClient;
use serde::Deserialize;

use crate::{
	AdminRequest, AdminResponse, AdminResult, ClientFrame, CommandRequest, CommandResponse, CommandResult,
	ErrResponse, LoginResult, QueryRequest, QueryResponse, QueryResult, Response, ResponsePayload, params_to_wire,
	session::{parse_admin_response, parse_command_response, parse_query_response},
};

/// HTTP-specific response format (server returns `{ "frames": [...] }`)
#[derive(Debug, serde::Deserialize)]
struct HttpFrameResponse {
	frames: Vec<ClientFrame>,
}

impl HttpFrameResponse {
	fn into_admin(self) -> AdminResponse {
		AdminResponse {
			content_type: "application/vnd.reifydb.frames+json".to_string(),
			body: serde_json::json!({ "frames": self.frames }),
		}
	}

	fn into_command(self) -> CommandResponse {
		CommandResponse {
			content_type: "application/vnd.reifydb.frames+json".to_string(),
			body: serde_json::json!({ "frames": self.frames }),
		}
	}

	fn into_query(self) -> QueryResponse {
		QueryResponse {
			content_type: "application/vnd.reifydb.frames+json".to_string(),
			body: serde_json::json!({ "frames": self.frames }),
		}
	}
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
}

impl HttpClient {
	/// Create a new HTTP client connected to the given URL.
	///
	/// # Arguments
	/// * `url` - Base URL of the ReifyDB server (e.g., "http://localhost:8080")
	///
	/// # Example
	/// ```no_run
	/// use reifydb_client::HttpClient;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
	/// 	let client = HttpClient::connect("http://localhost:8080").await?;
	/// 	Ok(())
	/// }
	/// ```
	pub async fn connect(url: &str) -> Result<Self, Error> {
		// let inner = ReqwestClient::builder().timeout(Duration::from_secs(30)).build().map_err(|e| {
		// 	Error(diagnostic::internal::internal(format!("Failed to create HTTP client: {}", e)))
		// })?;

		let inner = ReqwestClient::builder().timeout(Duration::from_secs(30)).build().unwrap(); // FIXME better error handling

		// Normalize URL (remove trailing slash)
		let base_url = url.trim_end_matches('/').to_string();

		Ok(Self {
			inner,
			base_url,
			token: None,
		})
	}

	/// Create a new HTTP client using an existing reqwest Client for connection pooling.
	///
	/// # Arguments
	/// * `client` - Shared reqwest Client instance
	/// * `url` - Base URL of the ReifyDB server
	pub fn with_client(client: ReqwestClient, url: &str) -> Self {
		let base_url = url.trim_end_matches('/').to_string();
		Self {
			inner: client,
			base_url,
			token: None,
		}
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
		let body = serde_json::json!({
			"method": method,
			"credentials": credentials
		});

		let url = format!("{}/v1/authenticate", self.base_url);
		let response = self.inner.post(&url).json(&body).send().await.unwrap(); // FIXME better error handling
		let response_body = response.text().await.unwrap(); // FIXME better error handling

		let auth: HttpAuthenticateResponse = serde_json::from_str(&response_body).unwrap(); // FIXME better error handling

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
	///
	/// # Arguments
	/// * `rql` - RQL statement to execute
	/// * `params` - Optional parameters for the statement
	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = AdminRequest {
			statements: vec![rql.to_string()],
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_admin(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Admin(response),
		};
		parse_admin_response(ws_response)
	}

	/// Execute multiple admin statements in a batch.
	pub async fn admin_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = AdminRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_admin(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Admin(response),
		};
		parse_admin_response(ws_response)
	}

	/// Execute a command (write) statement.
	///
	/// # Arguments
	/// * `rql` - RQL statement to execute
	/// * `params` - Optional parameters for the statement
	///
	/// # Example
	/// ```no_run
	/// use reifydb_client::HttpClient;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
	/// 	let mut client = HttpClient::connect("http://localhost:8080").await?;
	/// 	client.authenticate("mytoken");
	///
	/// 	let result = client.command("INSERT INTO users VALUES (1, 'Alice')", None).await?;
	/// 	Ok(())
	/// }
	/// ```
	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = CommandRequest {
			statements: vec![rql.to_string()],
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_command(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Command(response),
		};
		parse_command_response(ws_response)
	}

	/// Execute a query (read) statement.
	///
	/// # Arguments
	/// * `rql` - RQL query to execute
	/// * `params` - Optional parameters for the query
	///
	/// # Example
	/// ```no_run
	/// use reifydb_client::HttpClient;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
	/// 	let mut client = HttpClient::connect("http://localhost:8080").await?;
	/// 	client.authenticate("mytoken");
	///
	/// 	let result = client.query("SELECT * FROM users", None).await?;
	/// 	for frame in result.frames {
	/// 		println!("{}", frame);
	/// 	}
	/// 	Ok(())
	/// }
	/// ```
	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = QueryRequest {
			statements: vec![rql.to_string()],
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_query(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Query(response),
		};
		parse_query_response(ws_response)
	}

	/// Execute multiple command statements in a batch.
	pub async fn command_batch(
		&self,
		statements: Vec<&str>,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		let request = CommandRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_command(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Command(response),
		};
		parse_command_response(ws_response)
	}

	/// Execute multiple query statements in a batch.
	pub async fn query_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = QueryRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.map(params_to_wire).flatten(),
		};

		let response = self.send_query(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Query(response),
		};
		parse_query_response(ws_response)
	}

	/// Send an admin request to the server.
	async fn send_admin(&self, request: &AdminRequest) -> Result<AdminResponse, Error> {
		let url = format!("{}/v1/admin", self.base_url);
		let response_body = self.send_request(&url, request).await?;

		match serde_json::from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_admin()),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send a command request to the server.
	async fn send_command(&self, request: &CommandRequest) -> Result<CommandResponse, Error> {
		let url = format!("{}/v1/command", self.base_url);
		let response_body = self.send_request(&url, request).await?;

		match serde_json::from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_command()),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send a query request to the server.
	async fn send_query(&self, request: &QueryRequest) -> Result<QueryResponse, Error> {
		let url = format!("{}/v1/query", self.base_url);
		let response_body = self.send_request(&url, request).await?;

		match serde_json::from_str::<HttpFrameResponse>(&response_body) {
			Ok(response) => Ok(response.into_query()),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send an HTTP POST request and return the response body.
	async fn send_request<T: serde::Serialize>(&self, url: &str, body: &T) -> Result<String, Error> {
		let mut request = self.inner.post(url).json(body);

		if let Some(ref token) = self.token {
			request = request.bearer_auth(token);
		}

		let response = request.send().await.unwrap(); // FIXME better error handling

		Ok(response.text().await.unwrap()) // FIXME better error handling
	}

	/// Parse an error response body into an Error.
	fn parse_error_response(&self, body: &str) -> Error {
		// Try parsing as HTTP error response format
		if let Ok(http_err) = serde_json::from_str::<HttpErrorResponse>(body) {
			let diag = http_err.diagnostic.unwrap_or_else(|| Diagnostic {
				code: http_err.code,
				message: http_err.error,
				..Default::default()
			});
			return Error(diag);
		}

		// Try parsing as diagnostic error response
		if let Ok(err_response) = serde_json::from_str::<ErrResponse>(body) {
			return Error(err_response.diagnostic);
		}

		// Fallback: return raw response as error
		// Error(diagnostic::internal::internal(format!("Failed to parse response: {}", body)))
		panic!("Failed to parse response: {}", body) // FIXME better error handling
	}
}
