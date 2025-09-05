// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	collections::HashMap,
	io::{Read, Write},
	net::{SocketAddr, TcpStream},
	time::Duration,
};

use serde_json;

use crate::{
	CommandRequest, CommandResponse, ErrResponse, QueryRequest,
	QueryResponse,
};

/// HTTP client implementation using mio for non-blocking I/O
#[derive(Clone)]
pub struct HttpClient {
	pub(crate) host: String,
	pub(crate) port: u16,
	pub(crate) timeout: Duration,
}

impl HttpClient {
	/// Create a new HTTP client for the given host and port
	pub fn new(host: &str, port: u16) -> Self {
		Self {
			host: host.to_string(),
			port,
			timeout: Duration::from_secs(30),
		}
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

		Ok(Self::new(&host, port))
	}

	/// Set timeout for requests
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.timeout = timeout;
		self
	}

	/// Send a command request
	pub fn send_command(
		&self,
		request: &CommandRequest,
	) -> Result<CommandResponse, Box<dyn std::error::Error>> {
		let json_body = serde_json::to_string(request)?;
		let response_body =
			self.send_request("/v1/command", &json_body)?;

		// Try to parse as CommandResponse first, then as error
		match serde_json::from_str::<CommandResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as error response
				match serde_json::from_str::<ErrResponse>(
					&response_body,
				) {
					Ok(err_response) => Err(format!(
						"Server error: {:?}",
						err_response.diagnostic
					)
					.into()),
					Err(_) => Err(format!(
						"Failed to parse response: {}",
						response_body
					)
					.into()),
				}
			}
		}
	}

	/// Send a query request
	pub fn send_query(
		&self,
		request: &QueryRequest,
	) -> Result<QueryResponse, Box<dyn std::error::Error>> {
		let json_body = serde_json::to_string(request)?;
		let response_body =
			self.send_request("/v1/query", &json_body)?;

		// Try to parse as QueryResponse first, then as error
		match serde_json::from_str::<QueryResponse>(&response_body) {
			Ok(response) => Ok(response),
			Err(_) => {
				// Try parsing as error response
				match serde_json::from_str::<ErrResponse>(
					&response_body,
				) {
					Ok(err_response) => Err(format!(
						"Server error: {:?}",
						err_response.diagnostic
					)
					.into()),
					Err(_) => Err(format!(
						"Failed to parse response: {}",
						response_body
					)
					.into()),
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
		let addr_str = format!("{}:{}", self.host, self.port);
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
		stream.flush()?;

		// Read response
		let mut response_buffer = Vec::new();
		stream.read_to_end(&mut response_buffer)?;

		let response_str = String::from_utf8_lossy(&response_buffer);

		// Parse HTTP response
		self.parse_http_response(&response_str)
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

			// Parse chunk size (hex)
			let chunk_size = usize::from_str_radix(size_line, 16)?;

			if chunk_size == 0 {
				break; // End of chunks
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
		let addr_str = format!("{}:{}", self.host, self.port);
		let addr: SocketAddr = addr_str.parse()?;
		let _stream = TcpStream::connect(addr)?;
		Ok(())
	}
}
