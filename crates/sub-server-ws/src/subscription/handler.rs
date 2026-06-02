// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_engine::subscription::HydrateError;
use reifydb_sub_server::{
	interceptor::{Protocol, RequestMetadata},
	subscription::{
		errors::CreateSubscriptionError,
		handler::{
			BatchSubscribeError, SubscribeError, handle_batch_subscribe as shared_batch_subscribe,
			handle_batch_unsubscribe as shared_batch_unsubscribe, handle_subscribe as shared_subscribe,
		},
	},
};
use reifydb_subscription::batch::BatchId;
use reifydb_value::value::identity::IdentityId;

use crate::{
	handler::{ConnectionContext, error_to_response},
	protocol::{BatchSubscribeRequest, BatchUnsubscribeRequest, SubscribeRequest},
	response::{BatchMemberInfo, Response},
	subscription::registry::WsWireSink,
};

pub(crate) async fn handle_subscribe(
	request_id: &str,
	sub: SubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	let identity: IdentityId = conn.identity.unwrap_or_else(IdentityId::root);
	let metadata = RequestMetadata::new(Protocol::WebSocket);
	let sink = WsWireSink::new(conn.push_tx.clone());

	match shared_subscribe(
		conn.state,
		conn.connection_id,
		identity,
		sub.rql.clone(),
		sink,
		conn.registry,
		sub.format,
		conn.shutdown.clone(),
		metadata,
	)
	.await
	{
		Ok(ack) => {
			if let Some(handle) = ack.remote_handle {
				conn.remote_tasks.insert(ack.subscription_id.to_string(), handle);
			}
			Some(Response::subscribed(request_id, ack.subscription_id.to_string()).to_json())
		}
		Err(err) => Some(subscribe_error_to_response(request_id, err)),
	}
}

pub(crate) async fn handle_batch_subscribe(
	request_id: &str,
	req: BatchSubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	let identity: IdentityId = conn.identity.unwrap_or_else(IdentityId::root);
	let metadata = RequestMetadata::new(Protocol::WebSocket);
	let sink = WsWireSink::new(conn.push_tx.clone());

	match shared_batch_subscribe(
		conn.state,
		conn.connection_id,
		identity,
		&req.queries,
		sink,
		conn.registry,
		req.format,
		conn.shutdown.clone(),
		metadata,
	)
	.await
	{
		Ok(ack) => {
			let handles = conn.batch_remote_tasks.entry(ack.batch_id).or_default();
			handles.extend(ack.remote_handles);
			let members_for_ack: Vec<BatchMemberInfo> = ack
				.members
				.into_iter()
				.map(|m| BatchMemberInfo {
					index: m.index,
					subscription_id: m.subscription_id.to_string(),
				})
				.collect();
			Some(Response::batch_subscribed(request_id, ack.batch_id.to_string(), members_for_ack)
				.to_json())
		}
		Err(err) => Some(batch_subscribe_error_to_response(request_id, err)),
	}
}

pub(crate) async fn handle_batch_unsubscribe(
	request_id: &str,
	req: BatchUnsubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Option<String> {
	let batch_id = match parse_batch_id(request_id, &req.batch_id) {
		Ok(id) => id,
		Err(response) => return Some(response),
	};

	abort_local_batch_handles(conn, &batch_id);

	let _ = shared_batch_unsubscribe(conn.state, conn.registry, batch_id).await;

	Some(Response::batch_unsubscribed(request_id, batch_id.to_string()).to_json())
}

#[inline]
fn parse_batch_id(request_id: &str, raw: &str) -> Result<BatchId, String> {
	raw.parse::<BatchId>().map_err(|_| {
		Response::internal_error(request_id, "INVALID_BATCH_ID", "Invalid batch ID format").to_json()
	})
}

#[inline]
fn abort_local_batch_handles(conn: &mut ConnectionContext<'_>, batch_id: &BatchId) {
	if let Some(handles) = conn.batch_remote_tasks.remove(batch_id) {
		for handle in handles {
			handle.abort();
		}
	}
}

fn subscribe_error_to_response(request_id: &str, err: SubscribeError) -> String {
	match err {
		SubscribeError::Create(CreateSubscriptionError::Execute(e)) => error_to_response(request_id, e),
		SubscribeError::Create(CreateSubscriptionError::ExtractionFailed) => {
			Response::internal_error(request_id, "SUBSCRIPTION_FAILED", "Failed to extract subscription ID")
				.to_json()
		}
		SubscribeError::RemoteConnect(msg) => {
			Response::internal_error(request_id, "REMOTE_SUBSCRIBE_FAILED", msg).to_json()
		}
		SubscribeError::InvalidRemoteId => Response::internal_error(
			request_id,
			"REMOTE_SUBSCRIBE_FAILED",
			"Invalid remote subscription ID format",
		)
		.to_json(),
		SubscribeError::LeaseFailed {
			code,
			message,
		} => Response::internal_error(request_id, code, message).to_json(),
		SubscribeError::HydrationBackpressure => Response::internal_error(
			request_id,
			"HYDRATION_BACKPRESSURE",
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		)
		.to_json(),
		SubscribeError::HydrationFailed {
			error,
			rql,
			max_rows,
		} => hydrate_error_to_response(request_id, error, &rql, max_rows),
		SubscribeError::HydrationServiceUnavailable(msg) => {
			Response::internal_error(request_id, "SUBSCRIPTION_SERVICE_UNAVAILABLE", msg).to_json()
		}
	}
}

fn batch_subscribe_error_to_response(request_id: &str, err: BatchSubscribeError) -> String {
	match err {
		BatchSubscribeError::Empty => Response::internal_error(
			request_id,
			"INVALID_BATCH",
			"BatchSubscribe requires at least one query",
		)
		.to_json(),
		BatchSubscribeError::Create(CreateSubscriptionError::Execute(e)) => error_to_response(request_id, e),
		BatchSubscribeError::Create(CreateSubscriptionError::ExtractionFailed) => {
			Response::internal_error(request_id, "SUBSCRIPTION_FAILED", "Failed to extract subscription ID")
				.to_json()
		}
		BatchSubscribeError::RemoteConnect(msg) => {
			Response::internal_error(request_id, "REMOTE_SUBSCRIBE_FAILED", msg).to_json()
		}
		BatchSubscribeError::InvalidRemoteId => Response::internal_error(
			request_id,
			"REMOTE_SUBSCRIBE_FAILED",
			"Invalid remote subscription ID format",
		)
		.to_json(),
		BatchSubscribeError::LeaseFailed {
			code,
			message,
		} => Response::internal_error(request_id, code, message).to_json(),
		BatchSubscribeError::HydrationBackpressure => Response::internal_error(
			request_id,
			"HYDRATION_BACKPRESSURE",
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		)
		.to_json(),
		BatchSubscribeError::HydrationFailed {
			error,
			rql,
			max_rows,
		} => hydrate_error_to_response(request_id, error, &rql, max_rows),
		BatchSubscribeError::HydrationServiceUnavailable(msg) => {
			Response::internal_error(request_id, "SUBSCRIPTION_SERVICE_UNAVAILABLE", msg).to_json()
		}
	}
}

fn hydrate_error_to_response(request_id: &str, err: HydrateError, rql: &str, cap: u64) -> String {
	Response::internal_error(request_id, err.wire_code(), err.wire_message(rql, cap)).to_json()
}
