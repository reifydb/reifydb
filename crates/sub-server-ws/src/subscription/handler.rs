// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription request handler.
//!
//! Handles WebSocket subscription requests by creating database subscriptions
//! and registering them with the registry and poller for real-time updates.

use reifydb_core::interface::{auth::Identity, catalog::id::SubscriptionId as DbSubscriptionId};
use reifydb_sub_server::{execute::execute_command, state::AppState};
use reifydb_type::{
	params::Params,
	value::{Value, uuid::Uuid7},
};
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{
	handler::error_to_response,
	protocol::SubscribeRequest,
	subscription::{PushMessage, SubscriptionPoller, SubscriptionRegistry},
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
	identity: Option<Identity>,
	connection_id: ConnectionId,
	state: &AppState,
	registry: &SubscriptionRegistry,
	poller: &SubscriptionPoller,
	push_tx: mpsc::Sender<PushMessage>,
) -> Option<String> {
	// Authenticate if needed (subscriptions may require auth depending on your policy)
	let id = match identity.as_ref() {
		Some(id) => id.clone(),
		None => {
			// For now, allow unauthenticated subscriptions using root identity
			Identity::root()
		}
	};

	let params = Params::None;
	let timeout = state.query_timeout();
	let user_query = sub.query.clone();

	let create_sub_statement = format!("CREATE SUBSCRIPTION AS {{ {} }}", user_query);

	debug!("Generated subscription statement: {}", create_sub_statement);

	// Execute CREATE SUBSCRIPTION command
	match execute_command(state.pool(), state.engine_clone(), vec![create_sub_statement], id, params, timeout).await
	{
		Ok(cmd_frames) => {
			// Extract subscription ID
			let db_subscription_id = if let Some(cmd_frame) = cmd_frames.first() {
				// Look for "subscription_id" column (should be first)
				if let Some(sub_id_col) = cmd_frame.columns.iter().find(|c| c.name == "subscription_id")
				{
					if sub_id_col.data.len() > 0 {
						let value = sub_id_col.data.get_value(0);
						match value {
							Value::Uuid7(uuid) => Some(DbSubscriptionId(uuid.0)),
							_ => {
								error!(
									"subscription_id column has wrong type: {:?}",
									value
								);
								None
							}
						}
					} else {
						None
					}
				} else {
					error!("No subscription_id column in CREATE SUBSCRIPTION result");
					None
				}
			} else {
				None
			};

			if let Some(subscription_id) = db_subscription_id {
				// Register with registry using database subscription ID
				registry.subscribe(subscription_id, connection_id, user_query, push_tx);

				// Register with poller
				poller.register(subscription_id);

				tracing::info!(
					"Connection {} subscribed: subscription_id={}",
					connection_id,
					subscription_id
				);

				use crate::response::Response;
				Some(Response::subscribed(request_id, subscription_id.to_string()).to_json())
			} else {
				use crate::response::Response;
				Some(Response::internal_error(
					request_id,
					"SUBSCRIPTION_FAILED",
					"Failed to extract subscription ID",
				)
				.to_json())
			}
		}
		Err(e) => Some(error_to_response(request_id, e)),
	}
}
