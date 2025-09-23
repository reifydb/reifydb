// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::io::{Read, Write};

use reifydb_core::interface::{Engine, Identity, Params, Transaction};
use reifydb_type::diagnostic::Diagnostic;

use super::{HttpConnectionData, HttpState, command::handle_v1_command, query::handle_v1_query};
use crate::{
	core::Connection,
	protocols::{
		ProtocolError, ProtocolHandler, ProtocolResult,
		ws::{CommandRequest, ErrResponse, QueryRequest},
	},
};

#[derive(Clone)]
pub struct HttpHandler;

impl HttpHandler {
	pub fn new() -> Self {
		Self
	}

	/// Parse HTTP request headers
	fn parse_request(
		&self,
		data: &[u8],
	) -> Result<(String, String, std::collections::HashMap<String, String>), String> {
		let request_str = String::from_utf8_lossy(data);
		let lines: Vec<&str> = request_str.lines().collect();

		if lines.is_empty() {
			return Err("Empty request".to_string());
		}

		// Parse request line (GET /path HTTP/1.1)
		let request_parts: Vec<&str> = lines[0].split_whitespace().collect();
		if request_parts.len() != 3 {
			return Err("Invalid request line".to_string());
		}

		let method = request_parts[0].to_string();
		let path = request_parts[1].to_string();

		// Parse headers
		let mut headers = std::collections::HashMap::new();
		for line in &lines[1..] {
			if line.is_empty() {
				break;
			}
			if let Some(colon_pos) = line.find(':') {
				let key = line[..colon_pos].trim().to_lowercase();
				let value = line[colon_pos + 1..].trim().to_string();
				headers.insert(key, value);
			}
		}

		Ok((method, path, headers))
	}

	/// Build HTTP response
	fn build_response(
		&self,
		status_code: u16,
		status_text: &str,
		body: &str,
		headers: Option<&std::collections::HashMap<String, String>>,
	) -> String {
		let mut response = format!("HTTP/1.1 {} {}\r\n", status_code, status_text);

		// Add default headers - use byte length for Content-Length
		response.push_str(&format!("Content-Length: {}\r\n", body.as_bytes().len()));
		response.push_str("Content-Type: application/json\r\n");
		response.push_str("Connection: close\r\n");

		// Add custom headers if provided
		if let Some(custom_headers) = headers {
			for (key, value) in custom_headers {
				response.push_str(&format!("{}: {}\r\n", key, value));
			}
		}

		response.push_str("\r\n");
		response.push_str(body);

		response
	}

	/// Handle query execution for HTTP requests
	fn handle_query<T: Transaction>(&self, conn: &Connection<T>, query: &str) -> Result<String, String> {
		match conn.engine().query_as(
			&Identity::System {
				id: 1,
				name: "root".to_string(),
			},
			query,
			Params::None,
		) {
			Ok(result) => {
				let response_body = serde_json::json!({
				    "success": true,
				    "data": format!("Query executed successfully, {} frames returned", result.len()),
				    "results": result.len()
				});
				Ok(response_body.to_string())
			}
			Err(e) => {
				let error_body = serde_json::json!({
				    "success": false,
				    "error": format!("Query error: {}", e)
				});
				Ok(error_body.to_string())
			}
		}
	}
}

impl<T: Transaction> ProtocolHandler<T> for HttpHandler {
	fn name(&self) -> &'static str {
		"http"
	}

	fn can_handle(&self, buffer: &[u8]) -> bool {
		// Check for HTTP request signature
		if buffer.len() < 16 {
			return false;
		}

		let request = String::from_utf8_lossy(buffer);
		request.starts_with("GET ")
			|| request.starts_with("POST ")
			|| request.starts_with("PSVT ")
			|| request.starts_with("DELETE ")
			|| request.starts_with("HEAD ")
			|| request.starts_with("OPTIONS ")
	}

	fn handle_connection(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		// Initialize HTTP state
		let http_state = HttpState::ReadingRequest(HttpConnectionData::new());
		conn.set_state(crate::core::ConnectionState::Http(http_state));
		Ok(())
	}

	fn handle_read(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::Http(http_state) = conn.state() {
			match http_state {
				HttpState::ReadingRequest(_) => self.handle_request_read(conn),
				HttpState::Processing(_) => {
					Ok(()) // No additional reading needed during processing
				}
				HttpState::WritingResponse(_) => {
					Ok(()) // No reading during response writing
				}
				HttpState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn handle_write(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::Http(http_state) = conn.state() {
			match http_state {
				HttpState::ReadingRequest(_) => {
					Ok(()) // No writing during request reading
				}
				HttpState::Processing(_) => {
					Ok(()) // No writing during processing
				}
				HttpState::WritingResponse(_) => self.handle_response_write(conn),
				HttpState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn should_close(&self, conn: &Connection<T>) -> bool {
		matches!(
			conn.state(),
			crate::core::ConnectionState::Http(HttpState::Closed) | crate::core::ConnectionState::Closed
		)
	}
}

impl HttpHandler {
	fn handle_request_read<T: Transaction>(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		println!("HttpHandler: handle_request_read called, buffer has {} bytes", conn.buffer().len());

		// Check if we already found headers and are waiting for body
		let header_end =
			if let crate::core::ConnectionState::Http(HttpState::ReadingRequest(data)) = conn.state() {
				data.header_end
			} else {
				None
			};

		// If we haven't found headers yet, look for them
		let header_end = if header_end.is_none() {
			// First, check any data already in the buffer
			if !conn.buffer().is_empty() {
				if let Some(end) = self.find_header_end(conn.buffer()) {
					// Store the header end position
					if let crate::core::ConnectionState::Http(HttpState::ReadingRequest(data)) =
						conn.state_mut()
					{
						data.header_end = Some(end);
					}
					Some(end)
				} else {
					None
				}
			} else {
				None
			}
		} else {
			header_end
		};

		// Read more data if needed
		let mut buf = [0u8; 4096];
		loop {
			match conn.stream().read(&mut buf) {
				Ok(0) => {
					// Connection closed
					if conn.buffer().is_empty() {
						return Err(ProtocolError::ConnectionClosed);
					}
					break; // Process what we have
				}
				Ok(n) => {
					// Add data to connection buffer
					conn.buffer_mut().extend_from_slice(&buf[..n]);

					// If we don't have headers yet, look for them
					if header_end.is_none() {
						if let Some(end) = self.find_header_end(conn.buffer()) {
							// Store the header end position
							if let crate::core::ConnectionState::Http(
								HttpState::ReadingRequest(data),
							) = conn.state_mut()
							{
								data.header_end = Some(end);
							}
							// Try to process the complete request
							return self.process_http_request(conn, end);
						}
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
					break; // No more data available now
				}
				Err(e) => return Err(ProtocolError::Io(e)),
			}
		}

		// If we have headers, try to process the request
		if let Some(end) = header_end {
			// Try to process - this will check if we have enough
			// body data
			self.process_http_request(conn, end)?;
		} else if let crate::core::ConnectionState::Http(HttpState::ReadingRequest(data)) = conn.state() {
			// Check again if we have headers after reading
			if let Some(end) = data.header_end {
				self.process_http_request(conn, end)?;
			}
		}

		Ok(())
	}

	fn handle_response_write<T: Transaction>(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		// Extract response data to avoid borrowing conflicts
		let (response_data, bytes_written, keep_alive) =
			if let crate::core::ConnectionState::Http(HttpState::WritingResponse(data)) = conn.state() {
				(data.response_buffer.clone(), data.bytes_written, data.keep_alive)
			} else {
				return Ok(());
			};

		let mut total_written = bytes_written;

		loop {
			if total_written >= response_data.len() {
				// Response completely written
				if keep_alive {
					// Reset to reading state for keep-alive
					let new_data = HttpConnectionData::new();
					conn.set_state(crate::core::ConnectionState::Http(HttpState::ReadingRequest(
						new_data,
					)));
				} else {
					// Close connection
					conn.set_state(crate::core::ConnectionState::Http(HttpState::Closed));
				}
				break;
			}

			match conn.stream().write(&response_data[total_written..]) {
				Ok(0) => return Err(ProtocolError::ConnectionClosed),
				Ok(n) => {
					total_written += n;
					// Update bytes written in state
					if let crate::core::ConnectionState::Http(HttpState::WritingResponse(data)) =
						conn.state_mut()
					{
						data.bytes_written = total_written;
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
				Err(e) => return Err(ProtocolError::Io(e)),
			}
		}
		Ok(())
	}

	/// Find the end of HTTP headers (\r\n\r\n)
	fn find_header_end(&self, buffer: &[u8]) -> Option<usize> {
		for i in 0..buffer.len().saturating_sub(3) {
			if buffer[i] == b'\r'
				&& buffer[i + 1] == b'\n' && buffer[i + 2] == b'\r'
				&& buffer[i + 3] == b'\n'
			{
				return Some(i);
			}
		}
		None
	}

	/// Process a complete HTTP request
	fn process_http_request<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
		header_end: usize,
	) -> ProtocolResult<()> {
		// Parse the request
		let (method, path, headers) = self
			.parse_request(&conn.buffer()[..header_end])
			.map_err(|e| ProtocolError::Custom(format!("Parse error: {}", e)))?;

		// Calculate content length for POST requests
		let content_length: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);

		let body_start = header_end + 4; // Skip \r\n\r\n
		let total_needed = body_start + content_length;

		// Check if we have the complete request (headers + body)
		if method == "POST" && conn.buffer().len() < total_needed {
			// We don't have the full body yet, keep waiting
			return Ok(());
		}

		// Process the request based on method and path
		let response_body = match (&method[..], &path[..]) {
			("GET", "/health") => serde_json::json!({"status": "ok", "service": "reifydb"}).to_string(),
			("POST", "/query") => {
				// Body is guaranteed to be complete at this point
				let body = &conn.buffer()[body_start..body_start + content_length];
				let body_str = String::from_utf8_lossy(body);

				// Try to parse JSON body for query
				if let Ok(query_json) = serde_json::from_str::<serde_json::Value>(&body_str) {
					if let Some(query) = query_json.get("query").and_then(|q| q.as_str()) {
						self.handle_query(conn, query).map_err(|e| ProtocolError::Custom(e))?
					} else {
						serde_json::json!({"error": "Missing 'query' field in request body"})
							.to_string()
					}
				} else {
					serde_json::json!({"error": "Invalid JSON in request body"}).to_string()
				}
			}
			("POST", "/v1/command") => {
				// Body is guaranteed to be complete at this point
				let body = &conn.buffer()[body_start..body_start + content_length];
				let body_str = String::from_utf8_lossy(body);

				match serde_json::from_str::<CommandRequest>(&body_str) {
					Ok(cmd_req) => match handle_v1_command(conn, &cmd_req) {
						Ok(response) => serde_json::to_string(&response).map_err(|e| {
							ProtocolError::Custom(format!("Serialization error: {}", e))
						})?,
						Err(error_response) => {
							serde_json::to_string(&error_response).map_err(|e| {
								ProtocolError::Custom(format!(
									"Serialization error: {}",
									e
								))
							})?
						}
					},
					Err(e) => {
						let error_response = ErrResponse {
							diagnostic: Diagnostic {
								code: "INVALID_JSON".to_string(),
								message: format!("Invalid CommandRequest JSON: {}", e),
								..Default::default()
							},
						};
						serde_json::to_string(&error_response).map_err(|e| {
							ProtocolError::Custom(format!("Serialization error: {}", e))
						})?
					}
				}
			}
			("POST", "/v1/query") => {
				// Body is guaranteed to be complete at this point
				let body = &conn.buffer()[body_start..body_start + content_length];
				let body_str = String::from_utf8_lossy(body);

				match serde_json::from_str::<QueryRequest>(&body_str) {
					Ok(query_req) => match handle_v1_query(conn, &query_req) {
						Ok(response) => serde_json::to_string(&response).map_err(|e| {
							ProtocolError::Custom(format!("Serialization error: {}", e))
						})?,
						Err(error_response) => {
							serde_json::to_string(&error_response).map_err(|e| {
								ProtocolError::Custom(format!(
									"Serialization error: {}",
									e
								))
							})?
						}
					},
					Err(e) => {
						let error_response = ErrResponse {
							diagnostic: Diagnostic {
								code: "INVALID_JSON".to_string(),
								message: format!("Invalid QueryRequest JSON: {}", e),
								..Default::default()
							},
						};
						serde_json::to_string(&error_response).map_err(|e| {
							ProtocolError::Custom(format!("Serialization error: {}", e))
						})?
					}
				}
			}
			("GET", path) if path.starts_with("/query?") => {
				// Handle query via GET parameters
				if let Some(query_start) = path.find("q=") {
					let query_param = &path[query_start + 2..];
					let query = urlencoding::decode(query_param)
						.map_err(|_| ProtocolError::Custom("Invalid URL encoding".to_string()))?
						.to_string();
					self.handle_query(conn, &query).map_err(|e| ProtocolError::Custom(e))?
				} else {
					serde_json::json!({"error": "Missing 'q' query parameter"}).to_string()
				}
			}
			_ => serde_json::json!({"error": "Not found", "path": path, "method": method}).to_string(),
		};

		// Build HTTP response
		let response = if path == "/health"
			|| (method == "POST" && path == "/query")
			|| (method == "POST" && path == "/v1/command")
			|| (method == "POST" && path == "/v1/query")
			|| path.starts_with("/query?")
		{
			self.build_response(200, "OK", &response_body, None)
		} else {
			self.build_response(404, "Not Found", &response_body, None)
		};

		// Clear the processed request from the buffer
		let bytes_consumed = if method == "POST" {
			body_start + content_length
		} else {
			header_end + 4 // Just headers + \r\n\r\n
		};

		// Remove processed data from buffer
		conn.buffer_mut().drain(0..bytes_consumed);

		// Update state to writing response
		let mut response_data = HttpConnectionData::new();
		response_data.response_buffer = response.into_bytes();
		response_data.keep_alive =
			headers.get("connection").map(|v| v.to_lowercase() == "keep-alive").unwrap_or(false);

		conn.set_state(crate::core::ConnectionState::Http(HttpState::WritingResponse(response_data)));

		Ok(())
	}
}
