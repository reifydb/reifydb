// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_engine::subscription::HydrateError;
use reifydb_sub_core::{
	errors::CreateSubscriptionError,
	handler::{SubscribeError, handle_unsubscribe as shared_unsubscribe, hydrate_subscribe, register_subscribe},
	host::SubscribeHost,
};
use reifydb_sub_server::{
	execute::ExecuteError,
	interceptor::{Protocol, RequestMetadata},
};
use reifydb_value::{error::Error, value::identity::IdentityId};
use tokio::spawn;

use crate::{
	handler::{ConnectionContext, build_error, error_to_response},
	protocol::{SubscribeDecodeError, SubscribeRequest, UnsubscribeRequest},
	response::{Response, SubscriptionInfo},
	subscription::registry::{PushMessage, WsWireSink},
};

fn subscribe_identity(connection: Option<IdentityId>) -> IdentityId {
	connection.unwrap_or_else(IdentityId::anonymous)
}

pub(crate) async fn handle_subscribe(
	request_id: &str,
	req: SubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Result<Option<String>, Error> {
	let identity: IdentityId = subscribe_identity(*conn.identity);
	let metadata = RequestMetadata::new(Protocol::WebSocket);
	let sink = WsWireSink::new(conn.push_tx.clone());

	let format = req.format;
	let queries = match req.into_queries() {
		Ok(queries) => queries,
		Err(e) => return Ok(Some(decode_error_to_response(request_id, e))),
	};

	let host = conn.state.subscribe_host(metadata);
	match register_subscribe(
		&host,
		conn.connection_id,
		identity,
		&queries,
		sink.clone(),
		conn.registry,
		format,
		conn.shutdown.clone(),
	)
	.await?
	{
		Ok((ack, hydration)) => {
			let ctx = host.context().clone();
			let registry = conn.registry.clone();
			let push_tx = conn.push_tx.clone();
			let subscription_ids: Vec<SubscriptionId> =
				ack.subscriptions.iter().map(|entry| entry.subscription_id).collect();
			let hydrating = spawn(async move {
				match hydrate_subscribe::<_, ExecuteError>(&ctx, &sink, &registry, format, hydration)
					.await
				{
					Ok(Ok(())) => {}
					Ok(Err(_)) => {
						for subscription_id in subscription_ids {
							let _ = push_tx.send(PushMessage::SubscriptionClosed {
								subscription_id,
							});
						}
					}
					Err(error) => {
						panic!(
							"hydrating websocket subscriptions {subscription_ids:?} failed: {error}"
						)
					}
				}
			});
			conn.remote_tasks.extend(ack.remote_handles);
			conn.hydration_tasks.push(hydrating);
			let subscriptions: Vec<SubscriptionInfo> = ack
				.subscriptions
				.into_iter()
				.map(|entry| SubscriptionInfo {
					index: entry.index,
					subscription_id: entry.subscription_id.to_string(),
				})
				.collect();
			Ok(Some(Response::subscribed(request_id, subscriptions).to_json()))
		}
		Err(err) => Ok(Some(subscribe_error_to_response(request_id, err))),
	}
}

pub(crate) async fn handle_unsubscribe(
	request_id: &str,
	req: UnsubscribeRequest,
	conn: &mut ConnectionContext<'_>,
) -> Result<Option<String>, Error> {
	let subscription_ids = match req
		.subscription_ids
		.iter()
		.map(|raw| raw.parse::<u64>().map(SubscriptionId))
		.collect::<Result<Vec<_>, _>>()
	{
		Ok(ids) => ids,
		Err(_) => {
			return Ok(Some(build_error(
				request_id,
				"INVALID_SUBSCRIPTION_ID",
				"Invalid subscription ID format",
			)));
		}
	};

	for subscription_id in &subscription_ids {
		if let Some(handle) = conn.remote_tasks.remove(subscription_id) {
			handle.abort();
		}
	}

	shared_unsubscribe(conn.state.engine(), conn.registry, conn.connection_id, &subscription_ids).await?;

	Ok(Some(Response::unsubscribed(request_id, req.subscription_ids).to_json()))
}

fn decode_error_to_response(request_id: &str, err: SubscribeDecodeError) -> String {
	match err {
		SubscribeDecodeError::InvalidParams(message) => build_error(request_id, "INVALID_PARAMS", &message),
		SubscribeDecodeError::InvalidOptions(message) => build_error(request_id, "INVALID_OPTIONS", &message),
	}
}

fn subscribe_error_to_response(request_id: &str, err: SubscribeError<ExecuteError>) -> String {
	match err {
		SubscribeError::Empty => Response::internal_error(
			request_id,
			"INVALID_SUBSCRIBE",
			"Subscribe requires at least one query",
		)
		.to_json(),
		SubscribeError::Create {
			error: CreateSubscriptionError::Execute(e),
			..
		} => error_to_response(request_id, e),
		SubscribeError::RemoteConnect {
			message,
			..
		} => Response::internal_error(request_id, "REMOTE_SUBSCRIBE_FAILED", message).to_json(),
		SubscribeError::InvalidRemoteId {
			..
		} => Response::internal_error(
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
			..
		} => hydrate_error_to_response(request_id, error, &rql, max_rows),
		SubscribeError::HydrationServiceUnavailable(msg) => {
			Response::internal_error(request_id, "SUBSCRIPTION_SERVICE_UNAVAILABLE", msg).to_json()
		}
	}
}

fn hydrate_error_to_response(request_id: &str, err: HydrateError, rql: &str, cap: u64) -> String {
	Response::internal_error(request_id, err.wire_code(), err.wire_message(rql, cap)).to_json()
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::identity::IdentityId;

	use super::subscribe_identity;

	#[test]
	fn missing_identity_must_not_resolve_to_root() {
		// A failed auth clears the connection identity to None; resolving that to root hands an unauthenticated
		// socket full read access.
		assert_eq!(subscribe_identity(None), IdentityId::anonymous());
	}

	#[test]
	fn an_explicitly_authenticated_identity_is_preserved() {
		// Only the absent case may be rewritten; a real identity must never be substituted.
		assert_eq!(subscribe_identity(Some(IdentityId::root())), IdentityId::root());
		assert_eq!(subscribe_identity(Some(IdentityId::anonymous())), IdentityId::anonymous());
	}
}
