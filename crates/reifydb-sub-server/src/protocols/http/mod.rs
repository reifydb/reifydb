// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod command;
pub mod handler;
pub mod query;

use std::collections::HashMap;

pub use handler::HttpHandler;
use mio::Interest;

/// HTTP connection state
#[derive(Debug, Clone, PartialEq)]
pub enum HttpState {
	/// Reading HTTP request
	ReadingRequest(HttpConnectionData),
	/// Processing request
	Processing(HttpConnectionData),
	/// Writing HTTP response
	WritingResponse(HttpConnectionData),
	/// Connection closed
	Closed,
}

/// HTTP-specific connection data
#[derive(Debug, Clone, PartialEq)]
pub struct HttpConnectionData {
	/// Request buffer
	pub request_buffer: Vec<u8>,
	/// Response buffer
	pub response_buffer: Vec<u8>,
	/// Bytes written in response
	pub bytes_written: usize,
	/// Request method (GET, POST, etc.)
	pub method: Option<String>,
	/// Request path
	pub path: Option<String>,
	/// HTTP headers
	pub headers: HashMap<String, String>,
	/// Request body (for POST requests)
	pub body: Vec<u8>,
	/// Keep-alive flag
	pub keep_alive: bool,
}

impl HttpConnectionData {
	pub fn new() -> Self {
		Self {
			request_buffer: Vec::with_capacity(8192),
			response_buffer: Vec::new(),
			bytes_written: 0,
			method: None,
			path: None,
			headers: HashMap::new(),
			body: Vec::new(),
			keep_alive: false,
		}
	}
}

impl HttpState {
	pub fn interests(&self) -> Interest {
		match self {
			HttpState::ReadingRequest(_) => Interest::READABLE,
			HttpState::Processing(_) => Interest::READABLE,
			HttpState::WritingResponse(_) => Interest::WRITABLE,
			HttpState::Closed => Interest::READABLE,
		}
	}
}
