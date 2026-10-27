// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::fmt;

use reifydb_client::{
	GrpcClient, GrpcClientOptions, GrpcSubscription, RawChangePayload, ReconnectOptions, SubscribeItem,
	SubscriptionConfig, WireFormat, value::error::Error,
};
use tokio::{select, sync::watch};

#[derive(Debug)]
pub enum RemoteSubscriptionError {
	Connect(String),
	Subscribe(String),
}

impl fmt::Display for RemoteSubscriptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Connect(e) => write!(f, "Failed to connect to remote: {}", e),
			Self::Subscribe(e) => write!(f, "Remote subscribe failed: {}", e),
		}
	}
}

pub struct RemoteSubscription {
	inner: GrpcSubscription,
	subscription_id: String,
}

impl RemoteSubscription {
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}
}

pub async fn connect_remote(
	address: &str,
	body: &str,
	config: SubscriptionConfig,
	token: Option<&str>,
	wire_format: WireFormat,
) -> Result<RemoteSubscription, RemoteSubscriptionError> {
	let options = GrpcClientOptions {
		format: wire_format,
		reconnect: ReconnectOptions {
			max_reconnect_attempts: 0,
			..Default::default()
		},
	};
	let mut client = GrpcClient::connect_with_options(address, options)
		.await
		.map_err(|e| RemoteSubscriptionError::Connect(e.to_string()))?;
	if let Some(t) = token {
		client.authenticate(t);
	}
	let sub = client
		.subscribe(&[SubscribeItem::new(body, config)])
		.await
		.map_err(|e| RemoteSubscriptionError::Subscribe(e.to_string()))?
		.into_iter()
		.next()
		.ok_or_else(|| RemoteSubscriptionError::Subscribe("remote acknowledged no subscription".to_string()))?;
	let subscription_id = sub.subscription_id().to_string();
	Ok(RemoteSubscription {
		inner: sub,
		subscription_id,
	})
}

pub async fn proxy_remote_to_sink<F>(
	mut remote_sub: RemoteSubscription,
	mut shutdown: watch::Receiver<bool>,
	mut sink: F,
) -> Result<(), Error>
where
	F: FnMut(RawChangePayload) -> bool + Send + 'static,
{
	loop {
		select! {
			payload = remote_sub.inner.recv_raw() => {
				match payload? {
					Some(payload) => {
						if !sink(payload) {
							break;
						}
					}
					None => break,
				}
			}
			_ = shutdown.changed() => break,
		}
	}
	Ok(())
}
