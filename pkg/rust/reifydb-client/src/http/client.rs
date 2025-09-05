// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	collections::HashMap,
	io::{Read, Write},
	net::{SocketAddr, TcpStream, ToSocketAddrs},
	sync::{Arc, Mutex, mpsc},
	thread::{self, JoinHandle},
	time::Duration,
};

use serde_json;

use crate::{
	CommandRequest, CommandResponse, ErrResponse, QueryRequest,
	QueryResponse,
	http::{message::HttpInternalMessage, worker::http_worker_thread},
};

/// HTTP client implementation with worker thread
#[derive(Clone)]
pub struct HttpClient {
	inner: Arc<HttpClientInner>,
}

pub(crate) struct HttpClientInner {
	pub(crate) command_tx: mpsc::Sender<HttpInternalMessage>,
	worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// HTTP client configuration for the worker thread
#[derive(Clone)]
pub(crate) struct HttpClientConfig {
	pub(crate) host: String,
	pub(crate) port: u16,
	pub(crate) timeout: Duration,
}

impl Drop for HttpClient {
	fn drop(&mut self) {
		// Only send close if this is the last reference
		if Arc::strong_count(&self.inner) == 1 {
			// Send close message to worker thread
			let _ = self
				.inner
				.command_tx
				.send(HttpInternalMessage::Close);
		}
	}
}

impl HttpClient {
	/// Create a new HTTP client from a socket address
	pub fn new<A: ToSocketAddrs>(
		addr: A,
	) -> Result<Self, Box<dyn std::error::Error>> {
		// Resolve the address to get the first valid SocketAddr
		let socket_addr = addr
			.to_socket_addrs()?
			.next()
			.ok_or("Failed to resolve address")?;

		let host = socket_addr.ip().to_string();
		let port = socket_addr.port();

		let config = HttpClientConfig {
			host,
			port,
			timeout: Duration::from_secs(30),
		};

		Self::with_config(config)
	}

	/// Create HTTP client from URL (e.g., "http://localhost:8080")
	pub fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
		let url = if url.starts_with("http://") {
			&url[7..] // Remove "http://"
		} else {
			url
		};

		let parts: Vec<&str> = url.split(':').collect();
		let host = parts[0].to_string();
		let port = if parts.len() > 1 {
			parts[1].parse()?
		} else {
			8080
		};

		Self::new((host.as_str(), port))
	}

	/// Create HTTP client with specific configuration
	fn with_config(
		config: HttpClientConfig,
	) -> Result<Self, Box<dyn std::error::Error>> {
		let (command_tx, command_rx) = mpsc::channel();

		// Test connection first
		let test_config = config.clone();
		test_config.test_connection()?;

		// Start the background worker thread
		let worker_config = config.clone();
		let worker_handle = thread::spawn(move || {
			http_worker_thread(worker_config, command_rx);
		});

		Ok(Self {
			inner: Arc::new(HttpClientInner {
				command_tx,
				worker_handle: Arc::new(Mutex::new(Some(
					worker_handle,
				))),
			}),
		})
	}

	/// Get the command sender for internal use
	pub(crate) fn command_tx(&self) -> &mpsc::Sender<HttpInternalMessage> {
		&self.inner.command_tx
	}

	/// Close the client connection
	pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
		// The Drop impl handles cleanup
		Ok(())
	}

	/// Test connection to the server
	pub fn test_connection(
		&self,
	) -> Result<(), Box<dyn std::error::Error>> {
		// The connection was already tested during creation
		Ok(())
	}

	/// Create a blocking session
	pub fn blocking_session(
		&self,
		token: Option<String>,
	) -> Result<crate::http::HttpBlockingSession, reifydb_type::Error> {
		crate::http::HttpBlockingSession::from_client(
			self.clone(),
			token,
		)
	}

	/// Create a callback session
	pub fn callback_session(
		&self,
		token: Option<String>,
	) -> Result<crate::http::HttpCallbackSession, reifydb_type::Error> {
		crate::http::HttpCallbackSession::from_client(
			self.clone(),
			token,
		)
	}

	/// Create a channel session
	pub fn channel_session(
		&self,
		token: Option<String>,
	) -> Result<
		(
			crate::http::HttpChannelSession,
			mpsc::Receiver<crate::http::HttpResponseMessage>,
		),
		reifydb_type::Error,
	> {
		crate::http::HttpChannelSession::from_client(
			self.clone(),
			token,
		)
	}
}

impl HttpClientConfig {
	/// Send a command request
	pub fn send_command(
		&self,
		request: &CommandRequest,
	) -> Result<CommandResponse, reifydb_type::Error> {
		let json_body =
			serde_json::to_string(request).map_err(|e| {
				reifydb_type::Error(
					reifydb_type::diagnostic::internal(
						format!(
							"Failed to serialize request: {}",
							e
						),
					),
				)
			})?;
		let response_body =
			self.send_request("/v1/command", &json_body).map_err(
				|e| {
					reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Request failed: {}", e)
			))
				},
			)?;

		// Try to parse as CommandResponse first, then as error
		match serde_json::from_str::<CommandResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as error response
				match serde_json::from_str::<ErrResponse>(
					&response_body,
				) {
					Ok(err_response) => Err(reifydb_type::Error(err_response.diagnostic)),
					Err(_) => Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						format!("Failed to parse response: {}", response_body)
					))),
				}
			}
		}
	}

	/// Send a query request
	pub fn send_query(
		&self,
		request: &QueryRequest,
	) -> Result<QueryResponse, reifydb_type::Error> {
		let json_body =
			serde_json::to_string(request).map_err(|e| {
				reifydb_type::Error(
					reifydb_type::diagnostic::internal(
						format!(
							"Failed to serialize request: {}",
							e
						),
					),
				)
			})?;
		let response_body =
			self.send_request("/v1/query", &json_body).map_err(
				|e| {
					reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Request failed: {}", e)
			))
				},
			)?;

		// Try to parse as QueryResponse first, then as error
		match serde_json::from_str::<QueryResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as error response
				match serde_json::from_str::<ErrResponse>(
					&response_body,
				) {
					Ok(err_response) => Err(reifydb_type::Error(err_response.diagnostic)),
					Err(_) => Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						format!("Failed to parse response: {}", response_body)
					))),
				}
			}
		}
	}

	/// Send HTTP request and return response body
	fn send_request(
		&self,
		path: &str,
		body: &str,
	) -> Result<String, Box<dyn std::error::Error>> {
		// Parse socket address
		// Check if host is an IPv6 address by looking for colons
		let addr_str = if self.host.contains(':') {
			format!("[{}]:{}", self.host, self.port)
		} else {
			format!("{}:{}", self.host, self.port)
		};
		let addr: SocketAddr = addr_str.parse()?;

		// Create TCP connection
		let mut stream = TcpStream::connect(addr)?;

		// Build HTTP request
		let request = format!(
			"POST {} HTTP/1.1\r\n\
			Host: {}\r\n\
			Content-Type: application/json\r\n\
			Content-Length: {}\r\n\
			Connection: close\r\n\
			\r\n\
			{}",
			path,
			self.host,
			body.len(),
			body
		);

		// Send request
		stream.write_all(request.as_bytes())?;

		// Read response
		let mut response = String::new();
		stream.read_to_string(&mut response)?;

		// Parse HTTP response
		self.parse_http_response(&response)
	}

	/// Parse HTTP response and extract body
	fn parse_http_response(
		&self,
		response: &str,
	) -> Result<String, Box<dyn std::error::Error>> {
		let lines: Vec<&str> = response.lines().collect();

		if lines.is_empty() {
			return Err("Empty HTTP response".into());
		}

		// Parse status line
		let status_line = lines[0];
		let status_parts: Vec<&str> =
			status_line.split_whitespace().collect();

		if status_parts.len() < 3 {
			return Err("Invalid HTTP status line".into());
		}

		let status_code: u16 = status_parts[1].parse()?;
		if status_code < 200 || status_code >= 300 {
			return Err(format!(
				"HTTP error {}: {}",
				status_code, status_parts[2]
			)
			.into());
		}

		// Find headers and body separator
		let mut headers_end = None;
		for (i, line) in lines.iter().enumerate() {
			if line.is_empty() {
				headers_end = Some(i);
				break;
			}
		}

		let headers_end =
			headers_end.ok_or("No headers/body separator found")?;

		// Parse headers
		let mut headers = HashMap::new();
		for line in &lines[1..headers_end] {
			if let Some(colon_pos) = line.find(':') {
				let key =
					line[..colon_pos].trim().to_lowercase();
				let value = line[colon_pos + 1..]
					.trim()
					.to_string();
				headers.insert(key, value);
			}
		}

		// Get body
		let body_lines = &lines[headers_end + 1..];
		let body = body_lines.join("\n");

		// Handle chunked encoding if present
		if headers.get("transfer-encoding").map(|s| s.as_str())
			== Some("chunked")
		{
			return self.parse_chunked_body(&body);
		}

		Ok(body)
	}

	/// Parse chunked HTTP response body
	fn parse_chunked_body(
		&self,
		body: &str,
	) -> Result<String, Box<dyn std::error::Error>> {
		let mut result = String::new();
		let mut lines = body.lines();

		while let Some(size_line) = lines.next() {
			let size_line = size_line.trim();
			if size_line.is_empty() {
				continue;
			}

			// Parse chunk size (hexadecimal)
			let chunk_size = usize::from_str_radix(size_line, 16)?;
			if chunk_size == 0 {
				break; // Last chunk
			}

			// Read chunk data
			let mut chunk_data = String::new();
			let mut bytes_read = 0;

			while bytes_read < chunk_size {
				if let Some(line) = lines.next() {
					if !chunk_data.is_empty() {
						chunk_data.push('\n');
					}
					chunk_data.push_str(line);
					bytes_read += line.len() + 1; // +1 for newline
				} else {
					break;
				}
			}

			result.push_str(&chunk_data);
		}

		Ok(result)
	}

	/// Test connection to the server
	pub fn test_connection(
		&self,
	) -> Result<(), Box<dyn std::error::Error>> {
		// Check if host is an IPv6 address by looking for colons
		let addr_str = if self.host.contains(':') {
			format!("[{}]:{}", self.host, self.port)
		} else {
			format!("{}:{}", self.host, self.port)
		};
		let addr: SocketAddr = addr_str.parse()?;
		let _stream = TcpStream::connect(addr)?;
		Ok(())
	}
}
