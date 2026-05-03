// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Bridge that lets a local engine subscribe to a remote ReifyDB instance and forward incoming change payloads into
//! a local channel or sink. Wraps the gRPC client, handles authentication, and proxies raw RBCF payloads through a
//! conversion callback so the caller controls how remote events are typed.
//!
//! This is the only place in the workspace where external wire-format payloads are turned into events the local
//! engine consumes; doing the conversion anywhere else would couple unrelated subsystems to the gRPC client and
//! `wire-format` decoders.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::fmt;

use reifydb_client::{GrpcClient, GrpcSubscription, RawChangePayload, WireFormat};
use tokio::{
	select,
	sync::{mpsc, watch},
};

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
	rql: &str,
	token: Option<&str>,
	wire_format: WireFormat,
) -> Result<RemoteSubscription, RemoteSubscriptionError> {
	let mut client = GrpcClient::connect(address, wire_format)
		.await
		.map_err(|e| RemoteSubscriptionError::Connect(e.to_string()))?;
	if let Some(t) = token {
		client.authenticate(t);
	}
	let sub = client.subscribe(rql).await.map_err(|e| RemoteSubscriptionError::Subscribe(e.to_string()))?;
	let subscription_id = sub.subscription_id().to_string();
	Ok(RemoteSubscription {
		inner: sub,
		subscription_id,
	})
}

pub async fn proxy_remote<T, F>(
	mut remote_sub: RemoteSubscription,
	sender: mpsc::UnboundedSender<T>,
	mut shutdown: watch::Receiver<bool>,
	convert: F,
) where
	T: Send + 'static,
	F: Fn(RawChangePayload) -> T,
{
	loop {
		select! {
			payload = remote_sub.inner.recv_raw() => {
				match payload {
					Some(payload) => {
						if sender.send(convert(payload)).is_err() {
							break;
						}
					}
					None => break,
				}
			}
			_ = sender.closed() => break,
			_ = shutdown.changed() => break,
		}
	}
}

pub async fn proxy_remote_to_sink<F>(
	mut remote_sub: RemoteSubscription,
	mut shutdown: watch::Receiver<bool>,
	mut sink: F,
) where
	F: FnMut(RawChangePayload) -> bool + Send + 'static,
{
	loop {
		select! {
			payload = remote_sub.inner.recv_raw() => {
				match payload {
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
}
