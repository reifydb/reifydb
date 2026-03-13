// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Subscription request handler.
//!
//! Handles WebSocket subscription requests by creating database subscriptions
//! and registering them with the registry and poller for real-time updates.

use reifydb_sub_server::{
	state::AppState,
	subscribe::{CreateSubscriptionError, CreateSubscriptionResult::*, create_subscription},
};
use reifydb_subscription::poller::SubscriptionPoller;
use reifydb_type::value::{identity::IdentityId, uuid::Uuid7};
use tokio::sync::mpsc;
use tracing::info;

use crate::{
	handler::error_to_response,
	protocol::SubscribeRequest,
	response::Response,
	subscription::{PushMessage, SubscriptionRegistry},
};

type ConnectionId = Uuid7;

/// Handle a subscription request.
///
/// # Arguments
///
/// * `request_id` - The WebSocket request ID for response correlation
/// * `sub` - The subscription request containing the query
/// * `identity` - Optional authenticated identity (uses root if None)
/// * `connection_id` - The WebSocket connection ID
/// * `state` - Application state with database access
/// * `registry` - Subscription registry for tracking subscriptions
/// * `poller` - Subscription poller for consuming subscription data
/// * `push_tx` - Channel for sending push messages to the client
///
/// # Returns
///
/// `Option<String>` - JSON response string, or None if no response needed
pub(crate) async fn handle_subscribe(
	request_id: &str,
	sub: SubscribeRequest,
	identity: Option<IdentityId>,
	connection_id: ConnectionId,
	state: &AppState,
	registry: &SubscriptionRegistry,
	poller: &SubscriptionPoller,
	push_tx: mpsc::Sender<PushMessage>,
) -> Option<String> {
	let id: IdentityId = identity.unwrap_or_else(IdentityId::root);
	let user_query = sub.query.clone();

	match create_subscription(state, id, &user_query).await {
		Ok(Local(subscription_id)) => {
			registry.subscribe(subscription_id, connection_id, user_query, push_tx);
			poller.register(subscription_id);

			info!("Connection {} subscribed: subscription_id={}", connection_id, subscription_id);

			Some(Response::subscribed(request_id, subscription_id.to_string()).to_json())
		}
		Ok(Remote {
			..
		}) => Some(Response::internal_error(
			request_id,
			"REMOTE_SUBSCRIPTION_UNSUPPORTED",
			"Remote subscriptions are not yet supported over WebSocket",
		)
		.to_json()),
		Err(CreateSubscriptionError::Execute(e)) => Some(error_to_response(request_id, e)),
		Err(CreateSubscriptionError::ExtractionFailed) => Some(Response::internal_error(
			request_id,
			"SUBSCRIPTION_FAILED",
			"Failed to extract subscription ID",
		)
		.to_json()),
	}
}
