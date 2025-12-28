// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::time::Duration;

use reifydb_type::{Error, Params, diagnostic};
use reqwest::Client as ReqwestClient;

use crate::{
	CommandRequest, CommandResponse, ErrResponse, QueryRequest, QueryResponse, Response, ResponsePayload,
	session::{CommandResult, QueryResult, parse_command_response, parse_query_response},
};

/// HTTP-specific error response matching the server's format
#[derive(Debug, serde::Deserialize)]
struct HttpErrorResponse {
	code: String,
	error: String,
	#[serde(default)]
	diagnostic: Option<diagnostic::Diagnostic>,
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
		let inner = ReqwestClient::builder()
			.timeout(Duration::from_secs(30))
			.build()
			.map_err(|e| Error(diagnostic::internal(format!("Failed to create HTTP client: {}", e))))?;

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
			params,
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
			params,
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
			params,
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
			params,
		};

		let response = self.send_query(&request).await?;
		let ws_response = Response {
			id: String::new(),
			payload: ResponsePayload::Query(response),
		};
		parse_query_response(ws_response)
	}

	/// Send a command request to the server.
	async fn send_command(&self, request: &CommandRequest) -> Result<CommandResponse, Error> {
		let url = format!("{}/v1/command", self.base_url);
		let response_body = self.send_request(&url, request).await?;

		// Try to parse as CommandResponse first, then as error
		match serde_json::from_str::<CommandResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send a query request to the server.
	async fn send_query(&self, request: &QueryRequest) -> Result<QueryResponse, Error> {
		let url = format!("{}/v1/query", self.base_url);
		let response_body = self.send_request(&url, request).await?;

		// Try to parse as QueryResponse first, then as error
		match serde_json::from_str::<QueryResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => Err(self.parse_error_response(&response_body)),
		}
	}

	/// Send an HTTP POST request and return the response body.
	async fn send_request<T: serde::Serialize>(&self, url: &str, body: &T) -> Result<String, Error> {
		let mut request = self.inner.post(url).json(body);

		if let Some(ref token) = self.token {
			request = request.bearer_auth(token);
		}

		let response = request
			.send()
			.await
			.map_err(|e| Error(diagnostic::internal(format!("Request failed: {}", e))))?;

		response.text()
			.await
			.map_err(|e| Error(diagnostic::internal(format!("Failed to read response: {}", e))))
	}

	/// Parse an error response body into an Error.
	fn parse_error_response(&self, body: &str) -> Error {
		// Try parsing as HTTP error response format
		if let Ok(http_err) = serde_json::from_str::<HttpErrorResponse>(body) {
			let diag = http_err.diagnostic.unwrap_or_else(|| diagnostic::Diagnostic {
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
		Error(diagnostic::internal(format!("Failed to parse response: {}", body)))
	}
}
