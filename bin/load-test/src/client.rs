// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_client::{HttpClient, WsClient};

use crate::config::Protocol;

/// Error type for client operations
pub type Error = reifydb_client::r#type::Error;

/// Unified client abstraction over HTTP and WebSocket protocols
pub enum Client {
	Http(HttpClient),
	Ws(WsClient),
}

/// Operation to execute on the server
pub enum Operation {
	/// Read-only query
	Query(String),
	/// Write command (INSERT, UPDATE, DELETE, CREATE, etc.)
	Command(String),
}

impl Client {
	/// Connect to the server using the specified protocol
	pub async fn connect(protocol: Protocol, url: &str, token: Option<&str>) -> Result<Self, Error> {
		Self::connect_with_http_client(protocol, url, token, None).await
	}

	/// Connect with a shared HTTP client for connection pooling
	pub async fn connect_with_http_client(
		protocol: Protocol,
		url: &str,
		token: Option<&str>,
		http_client: Option<reqwest::Client>,
	) -> Result<Self, Error> {
		match protocol {
			Protocol::Http => {
				let mut client = if let Some(inner) = http_client {
					HttpClient::with_client(inner, url)
				} else {
					HttpClient::connect(url).await?
				};
				if let Some(token) = token {
					client.authenticate(token);
				}
				Ok(Client::Http(client))
			}
			Protocol::Ws => {
				let mut client = WsClient::connect(url).await?;
				if let Some(token) = token {
					client.authenticate(token).await?;
				}
				Ok(Client::Ws(client))
			}
		}
	}

	/// Execute an operation on the server
	pub async fn execute(&self, operation: &Operation) -> Result<(), Error> {
		match (self, operation) {
			(Client::Http(client), Operation::Query(rql)) => {
				client.query(rql, None).await?;
			}
			(Client::Http(client), Operation::Command(rql)) => {
				client.command(rql, None).await?;
			}
			(Client::Ws(client), Operation::Query(rql)) => {
				client.query(rql, None).await?;
			}
			(Client::Ws(client), Operation::Command(rql)) => {
				client.command(rql, None).await?;
			}
		}
		Ok(())
	}

	/// Close the connection gracefully
	pub async fn close(self) -> Result<(), Error> {
		if let Client::Ws(client) = self {
			client.close().await?;
		}
		// HTTP client has no explicit close - uses connection pooling
		Ok(())
	}
}
