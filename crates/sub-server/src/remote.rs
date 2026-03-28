// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared remote subscription support.
//!
//! Provides connection and proxy logic for remote subscriptions,
//! used by both gRPC and WebSocket server subsystems.

use std::fmt;

use reifydb_client::{GrpcClient, GrpcSubscription};
use reifydb_type::value::frame::frame::Frame;
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
	query: &str,
	token: Option<&str>,
) -> Result<RemoteSubscription, RemoteSubscriptionError> {
	let mut client =
		GrpcClient::connect(address).await.map_err(|e| RemoteSubscriptionError::Connect(e.to_string()))?;
	if let Some(t) = token {
		client.authenticate(t);
	}
	let sub = client.subscribe(query).await.map_err(|e| RemoteSubscriptionError::Subscribe(e.to_string()))?;
	let subscription_id = sub.subscription_id().to_string();
	Ok(RemoteSubscription {
		inner: sub,
		subscription_id,
	})
}

/// Proxy frames from a remote subscription to a local channel.
///
/// Receives frames from the remote subscription and converts them using the
/// provided closure before sending through the local channel. Exits when:
/// - The remote stream ends
/// - The local channel closes (receiver dropped)
/// - A shutdown signal is received
pub async fn proxy_remote<T, F>(
	mut remote_sub: RemoteSubscription,
	sender: mpsc::Sender<T>,
	mut shutdown: watch::Receiver<bool>,
	convert: F,
) where
	T: Send + 'static,
	F: Fn(Vec<Frame>) -> T,
{
	loop {
		select! {
			frames = remote_sub.inner.recv() => {
				match frames {
					Some(frames) => {
						if sender.send(convert(frames)).await.is_err() {
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
