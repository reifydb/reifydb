// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{HttpClient, WireFormat, WsClient};
use reqwest::Client as ReqwestClient;

use crate::config::Protocol;

pub type Error = reifydb_client::value::error::Error;

pub enum Client {
	Http(HttpClient),
	Ws(WsClient),
}

pub enum Operation {
	Query(String),
	Command(String),
	Admin(String),
}

impl Operation {
	pub fn rql(&self) -> &str {
		match self {
			Operation::Query(rql) | Operation::Command(rql) | Operation::Admin(rql) => rql,
		}
	}
}

impl Client {
	pub async fn connect(protocol: Protocol, url: &str, token: Option<&str>) -> Result<Self, Error> {
		Self::connect_with_http_client(protocol, url, token, None).await
	}

	pub async fn connect_with_http_client(
		protocol: Protocol,
		url: &str,
		token: Option<&str>,
		http_client: Option<ReqwestClient>,
	) -> Result<Self, Error> {
		match protocol {
			Protocol::Http => {
				let mut client = if let Some(inner) = http_client {
					HttpClient::with_client(inner, url, WireFormat::Frames)?
				} else {
					HttpClient::connect(url, WireFormat::Frames).await?
				};
				if let Some(token) = token {
					client.authenticate(token);
				}
				Ok(Client::Http(client))
			}
			Protocol::Ws => {
				let mut client = WsClient::connect(url, WireFormat::Frames).await?;
				if let Some(token) = token {
					client.authenticate(token).await?;
				}
				Ok(Client::Ws(client))
			}
		}
	}

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
			(Client::Http(client), Operation::Admin(rql)) => {
				client.admin(rql, None).await?;
			}
			(Client::Ws(client), Operation::Admin(rql)) => {
				client.admin(rql, None).await?;
			}
		}
		Ok(())
	}

	pub async fn close(self) -> Result<(), Error> {
		if let Client::Ws(client) = self {
			client.close().await?;
		}
		Ok(())
	}
}
