// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Subscription request handler.
//!
//! Handles WebSocket subscription requests by creating database subscriptions
//! and registering them with the registry and poller for real-time updates.

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_remote_proxy::{RemoteSubscription, connect_remote, proxy_remote, proxy_remote_to_sink};
use reifydb_sub_server::{
	format::WireFormat,
	interceptor::{Protocol, RequestMetadata},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, resolve_response_json},
	subscribe::{
		CreateSubscriptionError, CreateSubscriptionResult, CreateSubscriptionResult::*, cleanup_subscription,
		create_subscription,
	},
};
use reifydb_subscription::batch::BatchId;
use reifydb_type::value::identity::IdentityId;
use reifydb_wire_format::{encode::encode_frames, json::to::convert_frames, options::EncodeOptions};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::{spawn, sync::watch::Receiver as WatchReceiver};
use tracing::{info, warn};

use crate::{
	handler::{BinaryKind, ConnectionContext, encode_rbcf_envelope, error_to_response},
	protocol::{BatchSubscribeRequest, BatchUnsubscribeRequest, SubscribeRequest},
	response::{BatchMemberInfo, Response},
	subscription::{PushMessage, registry::SubscriptionRegistry},
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
	let user_rql = sub.rql.clone();
	let format = sub.format;
	// TODO: capture upgrade request headers via accept_hdr_async
	let metadata = RequestMetadata::new(Protocol::WebSocket);

	match create_subscription(conn.state, id, &user_rql, metadata).await {
		Ok(Local(subscription_id)) => {
			conn.registry.subscribe(
				subscription_id,
				conn.connection_id,
				user_rql,
				conn.push_tx.clone(),
				format,
			);

			info!(
				"Connection {} subscribed: subscription_id={} format={:?}",
				conn.connection_id, subscription_id, format
			);

			Some(Response::subscribed(request_id, subscription_id.to_string()).to_json())
		}
		Ok(Remote {
			address,
			rql,
			token: ns_token,
		}) => {
			let remote_sub = match connect_remote(&address, &rql, ns_token.as_deref()).await {
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
				proxy_remote(remote_sub, push_tx, shutdown, move |frames| match format {
					WireFormat::Rbcf => {
						let rbcf_bytes = encode_frames(&frames, &EncodeOptions::fast())
							.unwrap_or_default();
						let envelope = encode_rbcf_envelope(
							BinaryKind::Change,
							&subscription_id.to_string(),
							&rbcf_bytes,
							None,
						);
						PushMessage::ChangeRbcf {
							subscription_id,
							envelope,
						}
					}
					WireFormat::Frames => {
						let ws_frames = convert_frames(&frames);
						PushMessage::ChangeJson {
							subscription_id,
							content_type: CONTENT_TYPE_FRAMES.to_string(),
							body: json!({ "frames": ws_frames }),
						}
					}
					WireFormat::Json => {
						let body = match resolve_response_json(frames, false) {
							Ok(r) => from_str::<JsonValue>(&r.body)
								.unwrap_or(JsonValue::String(r.body)),
							Err(_) => JsonValue::Array(vec![]),
						};
						PushMessage::ChangeJson {
							subscription_id,
							content_type: CONTENT_TYPE_JSON.to_string(),
							body,
						}
					}
				})
				.await;
				let _ = push_tx_close.send(PushMessage::Closed {
					subscription_id,
				});
			});
			conn.remote_tasks.insert(remote_id.clone(), handle);

			info!(
				"Connection {} subscribed to remote: subscription_id={} format={:?}",
				conn.connection_id, remote_id, format
			);

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

/// One resolved batch member, classified by source.
enum ResolvedBatchMember {
	Local {
		index: usize,
		subscription_id: SubscriptionId,
		query: String,
	},
	Remote {
		index: usize,
		subscription_id: SubscriptionId,
		remote_sub: Box<RemoteSubscription>,
	},
}

impl ResolvedBatchMember {
	fn index(&self) -> usize {
		match self {
			Self::Local {
				index,
				..
			}
			| Self::Remote {
				index,
				..
			} => *index,
		}
	}

	fn subscription_id(&self) -> SubscriptionId {
		match self {
			Self::Local {
				subscription_id,
				..
			}
			| Self::Remote {
				subscription_id,
				..
			} => *subscription_id,
		}
	}
}

/// Handle a batch-subscribe request: create N member subscriptions and group them under one BatchId.
///
/// The batch delivers coalesced deltas per poller tick as a single `BatchChange` push.
/// All-or-nothing: if any member fails to create, previously-created members are rolled back.
pub(crate) async fn handle_batch_subscribe(
	request_id: &str,
	req: BatchSubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	if req.queries.is_empty() {
		return Some(Response::internal_error(
			request_id,
			"INVALID_BATCH",
			"BatchSubscribe requires at least one query",
		)
		.to_json());
	}

	let id: IdentityId = conn.identity.unwrap_or_else(IdentityId::root);
	let format = req.format;

	let mut resolved: Vec<ResolvedBatchMember> = Vec::with_capacity(req.queries.len());

	for (index, user_rql) in req.queries.iter().enumerate() {
		let metadata = RequestMetadata::new(Protocol::WebSocket);
		match create_subscription(conn.state, id, user_rql, metadata).await {
			Ok(CreateSubscriptionResult::Local(subscription_id)) => {
				resolved.push(ResolvedBatchMember::Local {
					index,
					subscription_id,
					query: user_rql.clone(),
				});
			}
			Ok(CreateSubscriptionResult::Remote {
				address,
				rql,
				token: ns_token,
			}) => {
				let remote_sub = match connect_remote(&address, &rql, ns_token.as_deref()).await {
					Ok(s) => s,
					Err(e) => {
						rollback_batch_members(conn, &resolved).await;
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
					Ok(n) => SubscriptionId(n),
					Err(_) => {
						rollback_batch_members(conn, &resolved).await;
						return Some(Response::internal_error(
							request_id,
							"REMOTE_SUBSCRIBE_FAILED",
							"Invalid remote subscription ID format",
						)
						.to_json());
					}
				};
				resolved.push(ResolvedBatchMember::Remote {
					index,
					subscription_id,
					remote_sub: Box::new(remote_sub),
				});
			}
			Err(CreateSubscriptionError::Execute(e)) => {
				rollback_batch_members(conn, &resolved).await;
				return Some(error_to_response(request_id, e));
			}
			Err(CreateSubscriptionError::ExtractionFailed) => {
				rollback_batch_members(conn, &resolved).await;
				return Some(Response::internal_error(
					request_id,
					"SUBSCRIPTION_FAILED",
					"Failed to extract subscription ID",
				)
				.to_json());
			}
		}
	}

	// Register local members with the registry so the poller finds them.
	for member in &resolved {
		if let ResolvedBatchMember::Local {
			subscription_id,
			query,
			..
		} = member
		{
			conn.registry.subscribe(
				*subscription_id,
				conn.connection_id,
				query.clone(),
				conn.push_tx.clone(),
				format,
			);
		}
	}

	let member_ids: Vec<SubscriptionId> = resolved.iter().map(|m| m.subscription_id()).collect();
	let batch_id = conn.registry.register_batch(
		conn.connection_id,
		member_ids,
		conn.push_tx.clone(),
		format,
		conn.state.clock(),
		conn.state.rng(),
	);

	// Spawn proxy tasks for remote members, routing frames into the batch's pending envelope.
	let mut remote_members_taken: Vec<(SubscriptionId, RemoteSubscription)> = Vec::new();
	let mut members_for_ack: Vec<BatchMemberInfo> = Vec::with_capacity(resolved.len());
	for member in resolved {
		members_for_ack.push(BatchMemberInfo {
			index: member.index(),
			subscription_id: member.subscription_id().to_string(),
		});
		if let ResolvedBatchMember::Remote {
			subscription_id,
			remote_sub,
			..
		} = member
		{
			remote_members_taken.push((subscription_id, *remote_sub));
		}
	}

	let remote_handles = conn.batch_remote_tasks.entry(batch_id).or_default();
	for (subscription_id, remote_sub) in remote_members_taken {
		let registry = Arc::clone(conn.registry);
		let shutdown = conn.shutdown.clone();
		let handle = spawn(async move {
			run_batch_remote_proxy(registry, batch_id, subscription_id, remote_sub, shutdown).await;
		});
		remote_handles.push(handle);
	}

	info!(
		"Connection {} created batch {} with {} members (format={:?})",
		conn.connection_id,
		batch_id,
		members_for_ack.len(),
		format
	);

	Some(Response::batch_subscribed(request_id, batch_id.to_string(), members_for_ack).to_json())
}

/// Drive a remote subscription into a batch's pending envelope, emitting
/// `BatchMemberClosed` when the upstream stream ends.
async fn run_batch_remote_proxy(
	registry: Arc<SubscriptionRegistry>,
	batch_id: BatchId,
	subscription_id: SubscriptionId,
	remote_sub: RemoteSubscription,
	shutdown: WatchReceiver<bool>,
) {
	let registry_push = Arc::clone(&registry);
	proxy_remote_to_sink(remote_sub, shutdown, move |frames| {
		registry_push.push_batch_frames(batch_id, subscription_id, frames)
	})
	.await;
	let _ = registry.emit_batch_member_closed(batch_id, subscription_id);
}

/// Handle a batch-unsubscribe request: cascade-cancels every member.
pub(crate) async fn handle_batch_unsubscribe(
	request_id: &str,
	req: BatchUnsubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	let batch_id = match req.batch_id.parse::<BatchId>() {
		Ok(id) => id,
		Err(_) => {
			return Some(Response::internal_error(
				request_id,
				"INVALID_BATCH_ID",
				"Invalid batch ID format",
			)
			.to_json());
		}
	};

	if let Some(handles) = conn.batch_remote_tasks.remove(&batch_id) {
		for handle in handles {
			handle.abort();
		}
	}

	let Some(members) = conn.registry.unsubscribe_batch(batch_id) else {
		return Some(Response::batch_unsubscribed(request_id, batch_id.to_string()).to_json());
	};

	for subscription_id in members {
		if let Err(e) = cleanup_subscription(conn.state, subscription_id).await {
			warn!("Failed to cleanup batch member subscription {} from database: {:?}", subscription_id, e);
		}
	}

	info!("Connection {} unsubscribed batch {}", conn.connection_id, batch_id);
	Some(Response::batch_unsubscribed(request_id, batch_id.to_string()).to_json())
}

/// Drop any members already resolved during a batch build that subsequently errored.
async fn rollback_batch_members(conn: &ConnectionContext<'_>, resolved: &[ResolvedBatchMember]) {
	for member in resolved {
		if let ResolvedBatchMember::Local {
			subscription_id,
			..
		} = member && let Err(e) = cleanup_subscription(conn.state, *subscription_id).await
		{
			warn!("Failed to cleanup partial batch member {} during rollback: {:?}", subscription_id, e);
		}
		// Remote members: dropping the RemoteSubscription closes its gRPC stream,
		// which the remote server treats as an unsubscribe.
	}
}
