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
	handler::{ConnectionContext, build_error, execute_error},
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
	let queries = match req.into_queries(conn.state.engine().catalog().cache()) {
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
	let (code, index, message) = match err {
		SubscribeDecodeError::InvalidParams {
			index,
			message,
		} => ("INVALID_PARAMS", index, message),
		SubscribeDecodeError::InvalidOptions {
			index,
			message,
		} => ("INVALID_OPTIONS", index, message),
	};
	Response::internal_error(request_id, code, message).with_index(index).to_json()
}

fn subscribe_error_to_response(request_id: &str, err: SubscribeError<ExecuteError>) -> String {
	let index = err.index();
	let response = match err {
		SubscribeError::Empty => Response::internal_error(
			request_id,
			"INVALID_SUBSCRIBE",
			"Subscribe requires at least one query",
		),
		SubscribeError::Create {
			error: CreateSubscriptionError::Execute(e),
			..
		} => execute_error(request_id, e),
		SubscribeError::RemoteConnect {
			message,
			..
		} => Response::internal_error(request_id, "REMOTE_SUBSCRIBE_FAILED", message),
		SubscribeError::InvalidRemoteId {
			..
		} => Response::internal_error(
			request_id,
			"REMOTE_SUBSCRIBE_FAILED",
			"Invalid remote subscription ID format",
		),
		SubscribeError::LeaseFailed {
			code,
			message,
		} => Response::internal_error(request_id, code, message),
		SubscribeError::HydrationBackpressure => Response::internal_error(
			request_id,
			"HYDRATION_BACKPRESSURE",
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		),
		SubscribeError::HydrationFailed {
			error,
			rql,
			max_rows,
			..
		} => hydrate_error_to_response(request_id, error, &rql, max_rows),
		SubscribeError::HydrationServiceUnavailable(msg) => {
			Response::internal_error(request_id, "SUBSCRIPTION_SERVICE_UNAVAILABLE", msg)
		}
	};
	match index {
		Some(index) => response.with_index(index),
		None => response,
	}
	.to_json()
}

fn hydrate_error_to_response(request_id: &str, err: HydrateError, rql: &str, cap: u64) -> Response {
	Response::internal_error(request_id, err.wire_code(), err.wire_message(rql, cap))
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::cache::CatalogCache;
	use reifydb_sub_core::{errors::CreateSubscriptionError, handler::SubscribeError};
	use reifydb_sub_server::execute::ExecuteError;
	use reifydb_value::value::identity::IdentityId;
	use serde_json::{Value as JsonValue, from_str};

	use super::{decode_error_to_response, subscribe_error_to_response, subscribe_identity};
	use crate::protocol::{Request, RequestPayload};

	fn payload(text: &str) -> JsonValue {
		from_str::<JsonValue>(text).expect("the frame must be valid json")["payload"].clone()
	}

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

	#[test]
	fn a_create_failure_names_its_item_in_the_frame() {
		// The engine knows which item failed; dropping it here leaves the client guessing.
		let frame = subscribe_error_to_response(
			"s",
			SubscribeError::Create {
				index: 2,
				error: CreateSubscriptionError::Execute(ExecuteError::Timeout),
			},
		);

		let payload = payload(&frame);
		assert_eq!(payload["index"], 2, "{payload}");
		assert_eq!(payload["diagnostic"]["code"], "QUERY_TIMEOUT", "{payload}");
	}

	#[test]
	fn a_lease_failure_names_no_item_in_the_frame() {
		// A whole-call failure must name no item, or the client blames an innocent subscription.
		let frame = subscribe_error_to_response(
			"s",
			SubscribeError::LeaseFailed {
				code: "TXN_012",
				message: "evicted".to_string(),
			},
		);

		let payload = payload(&frame);
		assert!(payload.get("index").is_none(), "{payload}");
	}

	#[test]
	fn a_bad_param_names_its_item_in_the_frame() {
		// The index must survive as a field, not only inside the message text.
		let request = from_str::<Request>(
			r#"{"id":"s","type":"Subscribe","payload":{"subscriptions":[{"rql":"from a"},{"rql":"from b","params":{"n":{"type":{"id":"Int4"},"value":"abc"}}}]}}"#,
		)
		.expect("a subscribe request should deserialise");
		let RequestPayload::Subscribe(subscribe) = request.payload else {
			panic!("expected a subscribe payload");
		};
		let err = subscribe.into_queries(&CatalogCache::new()).unwrap_err();

		let payload = payload(&decode_error_to_response("s", err));
		assert_eq!(payload["index"], 1, "{payload}");
		assert_eq!(payload["diagnostic"]["code"], "INVALID_PARAMS", "{payload}");
	}
}
