// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::net::ToSocketAddrs;

use crate::{HttpClient, ws::client::WsClient};

/// Unified client that can be either WebSocket or HTTP
#[derive(Clone)]
pub enum Client {
	Ws(WsClient),
	Http(HttpClient),
}

impl Client {
	/// Create a WebSocket client
	pub fn ws<A: ToSocketAddrs>(
		addr: A,
	) -> Result<WsClient, Box<dyn std::error::Error>> {
		WsClient::new(addr)
	}

	/// Create a WebSocket client from URL
	pub fn ws_from_url(
		url: &str,
	) -> Result<WsClient, Box<dyn std::error::Error>> {
		WsClient::from_url(url)
	}

	/// Create an HTTP client
	pub fn http<A: ToSocketAddrs>(
		addr: A,
	) -> Result<HttpClient, Box<dyn std::error::Error>> {
		HttpClient::new(addr)
	}

	/// Create an HTTP client from URL
	pub fn http_from_url(
		url: &str,
	) -> Result<HttpClient, Box<dyn std::error::Error>> {
		HttpClient::from_url(url)
	}

	/// Close the client connection
	pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
		match self {
			Client::Ws(ws) => ws.close(),
			Client::Http(http) => http.close(),
		}
	}
}
