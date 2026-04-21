// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

//! Remote subscription proxy.
//!
//! Provides connection and proxy logic for remote subscriptions,
//! used by both gRPC and WebSocket server subsystems.

use std::fmt;

use reifydb_client::{GrpcClient, GrpcSubscription, RawChangePayload, WireFormat};
use tokio::{
	select,
	sync::{mpsc, watch},
};

/// Error returned when connecting to a remote subscription fails.
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

/// An active remote subscription, wrapping the underlying gRPC connection.
pub struct RemoteSubscription {
	inner: GrpcSubscription,
	subscription_id: String,
}

impl RemoteSubscription {
	/// The subscription ID assigned by the remote node.
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}
}

/// Connect to a remote node and create a subscription.
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

/// Proxy raw payloads from a remote subscription to a local channel.
///
/// Receives raw payloads from the remote subscription and converts them using the
/// provided closure before sending through the local channel. Exits when:
/// - The remote stream ends
/// - The local channel closes (receiver dropped)
/// - A shutdown signal is received
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

/// Proxy raw payloads from a remote subscription into a caller-supplied sink closure.
///
/// Each received `RawChangePayload` is passed to `sink`. The sink returns `true` to
/// continue, `false` to stop the proxy (e.g. downstream batch was torn down).
/// Exits when:
/// - The remote stream ends
/// - `sink` returns `false`
/// - A shutdown signal is received
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
