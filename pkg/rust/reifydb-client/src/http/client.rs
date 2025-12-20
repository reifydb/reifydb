// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	io::{BufRead, BufReader, Read, Write},
	net::{SocketAddr, TcpStream, ToSocketAddrs},
	sync::{Arc, Mutex, mpsc},
	thread::{self, JoinHandle},
	time::Duration,
};

use serde_json;

use crate::{
	CommandRequest, CommandResponse, ErrResponse, QueryRequest, QueryResponse,
	http::{message::HttpInternalMessage, worker::http_worker_thread},
};

/// HTTP-specific error response matching the server's format
#[derive(Debug, serde::Deserialize)]
struct HttpErrorResponse {
	code: String,
	error: String,
	/// Full diagnostic info for rich error display (when available)
	#[serde(default)]
	diagnostic: Option<reifydb_type::diagnostic::Diagnostic>,
}

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
	pub(crate) _timeout: Duration,
}

impl Drop for HttpClient {
	fn drop(&mut self) {
		let _ = self.inner.command_tx.send(HttpInternalMessage::Close);
	}
}

impl HttpClient {
	/// Create a new HTTP client from a socket address
	pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Box<dyn std::error::Error>> {
		// Resolve the address to get the first valid SocketAddr
		let socket_addr = addr.to_socket_addrs()?.next().ok_or("Failed to resolve address")?;

		let host = socket_addr.ip().to_string();
		let port = socket_addr.port();

		let config = HttpClientConfig {
			host,
			port,
			_timeout: Duration::from_secs(30),
		};

		Self::with_config(config)
	}

	/// Create HTTP client from URL (e.g., "http://localhost:8080")
	pub fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
		let url = if url.starts_with("http://") {
			&url[7..] // Remove "http://"
		} else if url.starts_with("https://") {
			return Err("HTTPS is not yet supported".into());
		} else {
			url
		};

		// Parse host and port, handling IPv6 addresses
		let (host, port) = if url.starts_with('[') {
			// IPv6 address with brackets: [::1]:8080
			if let Some(end_bracket) = url.find(']') {
				let host = &url[1..end_bracket];
				let port_str = &url[end_bracket + 1..];
				let port = if port_str.starts_with(':') {
					port_str[1..].parse()?
				} else {
					80
				};
				(host.to_string(), port)
			} else {
				return Err("Invalid IPv6 address format".into());
			}
		} else if url.starts_with("::") || url.contains("::") {
			// IPv6 address without brackets: ::1:8080
			// Find the last colon that's likely the port separator
			if let Some(port_idx) = url.rfind(':') {
				// Check if what follows the last colon is a
				// port number
				if url[port_idx + 1..].chars().all(|c| c.is_ascii_digit()) {
					let host = &url[..port_idx];
					let port: u16 = url[port_idx + 1..].parse()?;
					(host.to_string(), port)
				} else {
					// No port specified, use default
					(url.to_string(), 80)
				}
			} else {
				(url.to_string(), 80)
			}
		} else {
			// Regular hostname or IPv4 address
			if let Some(colon_idx) = url.find(':') {
				let host = &url[..colon_idx];
				let port: u16 = url[colon_idx + 1..].parse()?;
				(host.to_string(), port)
			} else {
				(url.to_string(), 80)
			}
		};

		Self::new((host.as_str(), port))
	}

	/// Create HTTP client with specific configuration
	fn with_config(config: HttpClientConfig) -> Result<Self, Box<dyn std::error::Error>> {
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
				worker_handle: Arc::new(Mutex::new(Some(worker_handle))),
			}),
		})
	}

	/// Get the command sender for internal use
	pub(crate) fn command_tx(&self) -> &mpsc::Sender<HttpInternalMessage> {
		&self.inner.command_tx
	}

	/// Close the client connection
	pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
		self.inner.command_tx.send(HttpInternalMessage::Close)?;

		// Wait for worker thread to finish
		if let Ok(mut handle_guard) = self.inner.worker_handle.lock() {
			if let Some(handle) = handle_guard.take() {
				let _ = handle.join();
			}
		}
		Ok(())
	}

	/// Test connection to the server
	pub fn test_connection(&self) -> Result<(), Box<dyn std::error::Error>> {
		// The connection was already tested during creation
		Ok(())
	}

	/// Create a blocking session
	pub fn blocking_session(
		&self,
		token: Option<String>,
	) -> Result<crate::http::HttpBlockingSession, reifydb_type::Error> {
		crate::http::HttpBlockingSession::from_client(self.clone(), token)
	}

	/// Create a callback session
	pub fn callback_session(
		&self,
		token: Option<String>,
	) -> Result<crate::http::HttpCallbackSession, reifydb_type::Error> {
		crate::http::HttpCallbackSession::from_client(self.clone(), token)
	}

	/// Create a channel session
	pub fn channel_session(
		&self,
		token: Option<String>,
	) -> Result<
		(crate::http::HttpChannelSession, mpsc::Receiver<crate::http::HttpResponseMessage>),
		reifydb_type::Error,
	> {
		crate::http::HttpChannelSession::from_client(self.clone(), token)
	}
}

impl HttpClientConfig {
	/// Send a command request
	pub fn send_command(&self, request: &CommandRequest) -> Result<CommandResponse, reifydb_type::Error> {
		let json_body = serde_json::to_string(request).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!(
				"Failed to serialize request: {}",
				e
			)))
		})?;
		let response_body = self.send_request("/v1/command", &json_body).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!("Request failed: {}", e)))
		})?;

		// Try to parse as CommandResponse first, then as error
		match serde_json::from_str::<CommandResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as HTTP error response format (with optional diagnostic)
				if let Ok(http_err) = serde_json::from_str::<HttpErrorResponse>(&response_body) {
					// Use full diagnostic if available, otherwise construct from code+message
					let diagnostic = http_err.diagnostic.unwrap_or_else(|| {
						reifydb_type::diagnostic::Diagnostic {
							code: http_err.code,
							message: http_err.error,
							..Default::default()
						}
					});
					return Err(reifydb_type::Error(diagnostic));
				}
				// Try parsing as diagnostic error response
				match serde_json::from_str::<ErrResponse>(&response_body) {
					Ok(err_response) => Err(reifydb_type::Error(err_response.diagnostic)),
					Err(_) => Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						format!("Failed to parse response: {}", response_body),
					))),
				}
			}
		}
	}

	/// Send a query request
	pub fn send_query(&self, request: &QueryRequest) -> Result<QueryResponse, reifydb_type::Error> {
		let json_body = serde_json::to_string(request).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!(
				"Failed to serialize request: {}",
				e
			)))
		})?;
		let response_body = self.send_request("/v1/query", &json_body).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!("Request failed: {}", e)))
		})?;

		// Try to parse as QueryResponse first, then as error
		match serde_json::from_str::<QueryResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as HTTP error response format (with optional diagnostic)
				if let Ok(http_err) = serde_json::from_str::<HttpErrorResponse>(&response_body) {
					// Use full diagnostic if available, otherwise construct from code+message
					let diagnostic = http_err.diagnostic.unwrap_or_else(|| {
						reifydb_type::diagnostic::Diagnostic {
							code: http_err.code,
							message: http_err.error,
							..Default::default()
						}
					});
					return Err(reifydb_type::Error(diagnostic));
				}
				// Try parsing as diagnostic error response
				match serde_json::from_str::<ErrResponse>(&response_body) {
					Ok(err_response) => Err(reifydb_type::Error(err_response.diagnostic)),
					Err(_) => Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						format!("Failed to parse response: {}", response_body),
					))),
				}
			}
		}
	}

	/// Send HTTP request and return response body
	fn send_request(&self, path: &str, body: &str) -> Result<String, Box<dyn std::error::Error>> {
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

		// Convert body to bytes first to get accurate Content-Length
		let body_bytes = body.as_bytes();

		// Build HTTP request header
		let header = format!(
			"POST {} HTTP/1.1\r\n\
			Host: {}\r\n\
			Content-Type: application/json\r\n\
			Content-Length: {}\r\n\
			Authorization: Bearer mysecrettoken\r\n\
			Connection: close\r\n\
			\r\n",
			path,
			self.host,
			body_bytes.len()
		);

		// Send request header and body
		stream.write_all(header.as_bytes())?;
		stream.write_all(body_bytes)?;
		stream.flush()?;

		// Parse HTTP response using buffered reader
		self.parse_http_response_buffered(stream)
	}

	/// Parse HTTP response using buffered reading for large responses
	fn parse_http_response_buffered(&self, stream: TcpStream) -> Result<String, Box<dyn std::error::Error>> {
		let mut reader = BufReader::new(stream);
		let mut line = String::new();

		// Read status line
		reader.read_line(&mut line)?;
		let status_line = line.trim_end();
		let status_parts: Vec<&str> = status_line.split_whitespace().collect();

		if status_parts.len() < 3 {
			return Err("Invalid HTTP status line".into());
		}

		// Read headers (body is always read for error response parsing)
		let mut content_length: Option<usize> = None;
		let mut is_chunked = false;

		loop {
			line.clear();
			reader.read_line(&mut line)?;

			if line == "\r\n" || line == "\n" {
				break; // End of headers
			}

			if let Some(colon_pos) = line.find(':') {
				let key = line[..colon_pos].trim().to_lowercase();
				let value = line[colon_pos + 1..].trim();

				if key == "content-length" {
					content_length = value.parse().ok();
				} else if key == "transfer-encoding" && value.contains("chunked") {
					is_chunked = true;
				}
			}
		}

		// Read body based on transfer method
		let body = if is_chunked {
			self.read_chunked_body(&mut reader)?
		} else if let Some(length) = content_length {
			// Read exact content length
			let mut body = vec![0u8; length];
			reader.read_exact(&mut body)?;
			String::from_utf8(body)?
		} else {
			// Read until EOF (Connection: close)
			let mut body = String::new();
			reader.read_to_string(&mut body)?;
			body
		};

		Ok(body)
	}

	/// Read chunked HTTP response body
	fn read_chunked_body(&self, reader: &mut BufReader<TcpStream>) -> Result<String, Box<dyn std::error::Error>> {
		let mut result = Vec::new();
		let mut line = String::new();

		loop {
			// Read chunk size line
			line.clear();
			reader.read_line(&mut line)?;

			// Parse chunk size (hexadecimal), ignoring any chunk
			// extensions after ';'
			let size_str = line.trim().split(';').next().unwrap_or("0");
			let chunk_size = usize::from_str_radix(size_str, 16)?;

			if chunk_size == 0 {
				// Last chunk - read trailing headers if any
				loop {
					line.clear();
					reader.read_line(&mut line)?;
					if line == "\r\n" || line == "\n" {
						break;
					}
				}
				break;
			}

			// Read exact chunk data
			let mut chunk = vec![0u8; chunk_size];
			reader.read_exact(&mut chunk)?;
			result.extend_from_slice(&chunk);

			// Read trailing CRLF after chunk data
			line.clear();
			reader.read_line(&mut line)?;
		}

		String::from_utf8(result).map_err(|e| e.into())
	}

	/// Test connection to the server
	pub fn test_connection(&self) -> Result<(), Box<dyn std::error::Error>> {
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
