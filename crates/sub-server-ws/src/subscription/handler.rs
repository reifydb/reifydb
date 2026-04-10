// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Subscription request handler.
//!
//! Handles WebSocket subscription requests by creating database subscriptions
//! and registering them with the registry and poller for real-time updates.

use reifydb_core::{interface::catalog::id::SubscriptionId, value::frame::response::convert_frames};
use reifydb_remote_proxy::{connect_remote, proxy_remote};
use reifydb_sub_server::{
	interceptor::{Protocol, RequestMetadata},
	subscribe::{CreateSubscriptionError, CreateSubscriptionResult::*, create_subscription},
};
use reifydb_type::value::identity::IdentityId;
use serde_json::json;
use tokio::spawn;
use tracing::info;

use crate::{
	handler::{ConnectionContext, error_to_response},
	protocol::SubscribeRequest,
	response::{CONTENT_TYPE_FRAMES, Response},
	subscription::PushMessage,
};

/// Handle a subscription request.
///
/// # Arguments
///
/// * `request_id` - The WebSocket request ID for response correlation
/// * `sub` - The subscription request containing the query
/// * `conn` - The connection context with shared state
///
/// # Returns
///
/// `Option<String>` - JSON response string, or None if no response needed
pub(crate) async fn handle_subscribe(
	request_id: &str,
	sub: SubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	let id: IdentityId = conn.identity.unwrap_or_else(IdentityId::root);
	let user_query = sub.query.clone();
	// TODO: capture upgrade request headers via accept_hdr_async
	let metadata = RequestMetadata::new(Protocol::WebSocket);

	match create_subscription(conn.state, id, &user_query, metadata).await {
		Ok(Local(subscription_id)) => {
			conn.registry.subscribe(subscription_id, conn.connection_id, user_query, conn.push_tx.clone());

			info!("Connection {} subscribed: subscription_id={}", conn.connection_id, subscription_id);

			Some(Response::subscribed(request_id, subscription_id.to_string()).to_json())
		}
		Ok(Remote {
			address,
			query,
		}) => {
			let remote_sub = match connect_remote(&address, &query, conn.auth_token.as_deref()).await {
				Ok(s) => s,
				Err(e) => {
					return Some(Response::internal_error(
						request_id,
						"REMOTE_SUBSCRIBE_FAILED",
						e.to_string(),
					)
					.to_json());
				}
			};

			let remote_id = remote_sub.subscription_id().to_string();
			let subscription_id = match remote_id.parse::<u64>() {
				Ok(id) => SubscriptionId(id),
				Err(_) => {
					return Some(Response::internal_error(
						request_id,
						"REMOTE_SUBSCRIBE_FAILED",
						"Invalid remote subscription ID format",
					)
					.to_json());
				}
			};

			let push_tx = conn.push_tx.clone();
			let push_tx_close = push_tx.clone();
			let shutdown = conn.shutdown.clone();
			let handle = spawn(async move {
				proxy_remote(remote_sub, push_tx, shutdown, move |frames| {
					let ws_frames = convert_frames(&frames);
					PushMessage::Change {
						subscription_id,
						content_type: CONTENT_TYPE_FRAMES.to_string(),
						body: json!({ "frames": ws_frames }),
					}
				})
				.await;
				let _ = push_tx_close.send(PushMessage::Closed {
					subscription_id,
				});
			});
			conn.remote_tasks.insert(remote_id.clone(), handle);

			info!("Connection {} subscribed to remote: subscription_id={}", conn.connection_id, remote_id);

			Some(Response::subscribed(request_id, remote_id).to_json())
		}
		Err(CreateSubscriptionError::Execute(e)) => Some(error_to_response(request_id, e)),
		Err(CreateSubscriptionError::ExtractionFailed) => Some(Response::internal_error(
			request_id,
			"SUBSCRIPTION_FAILED",
			"Failed to extract subscription ID",
		)
		.to_json()),
	}
}
