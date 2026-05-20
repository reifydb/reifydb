// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_client::{
	HydrationConfig as ClientHydrationConfig, RawChangePayload, SubscriptionConfig as ClientSubscriptionConfig,
	WireFormat as ClientWireFormat,
};
use reifydb_core::interface::catalog::{id::SubscriptionId, subscription::HydrationConfig};
use reifydb_engine::subscription::HydrateError;
use reifydb_remote_proxy::{RemoteSubscription, connect_remote, proxy_remote, proxy_remote_to_sink};
use reifydb_sub_server::{
	format::WireFormat,
	interceptor::{Protocol, RequestMetadata},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, resolve_response_json},
	subscription::{
		cleanup::cleanup_subscription,
		create::{CreateSubscriptionResult, CreateSubscriptionResult::*, create_subscription},
		errors::CreateSubscriptionError,
		hydrate::run_hydrate,
	},
};
use reifydb_subscription::batch::BatchId;
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_type::value::identity::IdentityId;
use reifydb_wire_format::{encode::encode_frames, json::to::convert_frames, options::EncodeOptions};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::{spawn, sync::watch::Receiver as WatchReceiver};
use tracing::{info, warn};

use crate::{
	handler::{BinaryKind, ConnectionContext, encode_rbcf_envelope, error_to_response},
	protocol::{BatchSubscribeRequest, BatchUnsubscribeRequest, SubscribeRequest},
	response::{BatchMemberInfo, Response},
	subscription::{
		PushMessage,
		registry::{PromoteResult, SubscriptionRegistry, encode_change_for_handler},
	},
};

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
		Ok(Local {
			id: subscription_id,
			hydration,
			throttle,
		}) => {
			let server_cap = conn.state.subscribe_max_hydration_rows();
			let throttle = conn.state.clamp_throttle(throttle);
			let max_rows = match hydration.max_rows {
				Some(n) if n > server_cap => {
					warn!("clamping hydration.max_rows from {} to server cap {}", n, server_cap);
					server_cap
				}
				Some(n) => n,
				None => server_cap,
			};
			let warming_cap = if hydration.enabled {
				Some(max_rows as usize)
			} else {
				None
			};
			conn.registry.subscribe(
				subscription_id,
				conn.connection_id,
				user_rql.clone(),
				conn.push_tx.clone(),
				format,
				warming_cap,
				throttle,
			);

			if hydration.enabled {
				let lease = match conn.state.engine().acquire_current_snapshot_lease() {
					Ok((_, lease)) => lease,
					Err(e) => {
						let code = if e.0.code == "TXN_012" {
							"HYDRATION_VERSION_EVICTED"
						} else {
							"PIN_VERSION_FAILED"
						};
						abort_warming(conn, subscription_id).await;
						return Some(Response::internal_error(request_id, code, e.to_string())
							.to_json());
					}
				};
				if let Some(err_response) = run_ws_hydrate(
					request_id,
					conn,
					subscription_id,
					&user_rql,
					id,
					lease,
					max_rows,
					format,
				)
				.await
				{
					return Some(err_response);
				}
			} else {
				let _ = conn.registry.promote_to_live(subscription_id);
			}

			info!(
				"Connection {} subscribed: subscription_id={} format={:?} hydration_enabled={}",
				conn.connection_id, subscription_id, format, hydration.enabled
			);

			Some(Response::subscribed(request_id, subscription_id.to_string()).to_json())
		}
		Ok(Remote {
			address,
			body,
			token: ns_token,
			hydration,
			throttle,
		}) => {
			let client_fmt = match format {
				WireFormat::Rbcf => ClientWireFormat::Rbcf,
				WireFormat::Json | WireFormat::Frames => ClientWireFormat::Rbcf,
			};
			let config = ClientSubscriptionConfig {
				hydration: ClientHydrationConfig {
					enabled: hydration.enabled,
					max_rows: hydration.max_rows,
				},
				throttle,
			};
			let remote_sub =
				match connect_remote(&address, &body, config, ns_token.as_deref(), client_fmt).await {
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
				proxy_remote(remote_sub, push_tx, shutdown, move |payload| match format {
					WireFormat::Rbcf => match payload {
						RawChangePayload::Rbcf(bytes) => {
							let envelope = encode_rbcf_envelope(
								BinaryKind::Change,
								&subscription_id.to_string(),
								&bytes,
								None,
							);
							PushMessage::ChangeRbcf {
								subscription_id,
								envelope,
							}
						}
						payload => {
							let frames = payload.into_frames();
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
					},
					WireFormat::Frames => {
						let frames = payload.into_frames();
						let ws_frames = convert_frames(&frames);
						PushMessage::ChangeJson {
							subscription_id,
							content_type: CONTENT_TYPE_FRAMES.to_string(),
							body: json!({ "frames": ws_frames }),
						}
					}
					WireFormat::Json => {
						let frames = payload.into_frames();
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
	let mut local_hydrations: Vec<(SubscriptionId, String, HydrationConfig, Option<Duration>)> = Vec::new();

	for (index, user_rql) in req.queries.iter().enumerate() {
		let metadata = RequestMetadata::new(Protocol::WebSocket);
		match create_subscription(conn.state, id, user_rql, metadata).await {
			Ok(CreateSubscriptionResult::Local {
				id: subscription_id,
				hydration,
				throttle,
			}) => {
				local_hydrations.push((subscription_id, user_rql.clone(), hydration, throttle));
				resolved.push(ResolvedBatchMember::Local {
					index,
					subscription_id,
					query: user_rql.clone(),
				});
			}
			Ok(CreateSubscriptionResult::Remote {
				address,
				body,
				token: ns_token,
				hydration,
				throttle,
			}) => {
				let client_format = match format {
					WireFormat::Rbcf => ClientWireFormat::Rbcf,
					WireFormat::Json | WireFormat::Frames => ClientWireFormat::Rbcf,
				};
				let config = ClientSubscriptionConfig {
					hydration: ClientHydrationConfig {
						enabled: hydration.enabled,
						max_rows: hydration.max_rows,
					},
					throttle,
				};
				let remote_sub = match connect_remote(
					&address,
					&body,
					config,
					ns_token.as_deref(),
					client_format,
				)
				.await
				{
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

	let server_cap = conn.state.subscribe_max_hydration_rows();
	let mut effective_max_rows: HashMap<SubscriptionId, u64> = HashMap::new();
	for (sub_id, _, hydration, _) in &local_hydrations {
		let max_rows = match hydration.max_rows {
			Some(n) if n > server_cap => {
				warn!("clamping hydration.max_rows from {} to server cap {}", n, server_cap);
				server_cap
			}
			Some(n) => n,
			None => server_cap,
		};
		effective_max_rows.insert(*sub_id, max_rows);
	}

	for member in &resolved {
		if let ResolvedBatchMember::Local {
			subscription_id,
			query,
			..
		} = member
		{
			let warming_cap = local_hydrations
				.iter()
				.find(|(sid, _, _, _)| sid == subscription_id)
				.and_then(|(_, _, h, _)| {
					if h.enabled {
						Some(*effective_max_rows.get(subscription_id).unwrap_or(&server_cap)
							as usize)
					} else {
						None
					}
				});
			let throttle = conn.state.clamp_throttle(
				local_hydrations
					.iter()
					.find(|(sid, _, _, _)| sid == subscription_id)
					.and_then(|(_, _, _, t)| *t),
			);
			conn.registry.subscribe(
				*subscription_id,
				conn.connection_id,
				query.clone(),
				conn.push_tx.clone(),
				format,
				warming_cap,
				throttle,
			);
		}
	}

	let any_hydration = local_hydrations.iter().any(|(_, _, h, _)| h.enabled);
	if any_hydration {
		let lease = match conn.state.engine().acquire_current_snapshot_lease() {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				rollback_batch_members(conn, &resolved).await;
				return Some(Response::internal_error(request_id, code, e.to_string()).to_json());
			}
		};
		for (sub_id, rql, hydration, _) in &local_hydrations {
			if !hydration.enabled {
				continue;
			}
			let max_rows = *effective_max_rows.get(sub_id).unwrap_or(&server_cap);
			if let Some(err_response) =
				run_ws_hydrate(request_id, conn, *sub_id, rql, id, lease.clone(), max_rows, format)
					.await
			{
				for (other_sub, _, _, _) in &local_hydrations {
					abort_warming(conn, *other_sub).await;
				}
				return Some(err_response);
			}
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
		format,
	);

	Some(Response::batch_subscribed(request_id, batch_id.to_string(), members_for_ack).to_json())
}

async fn run_batch_remote_proxy(
	registry: Arc<SubscriptionRegistry>,
	batch_id: BatchId,
	subscription_id: SubscriptionId,
	remote_sub: RemoteSubscription,
	shutdown: WatchReceiver<bool>,
) {
	let registry_push = Arc::clone(&registry);
	proxy_remote_to_sink(remote_sub, shutdown, move |payload| {
		let frames = payload.into_frames();
		registry_push.push_batch_frames(batch_id, subscription_id, frames)
	})
	.await;
	let _ = registry.emit_batch_member_closed(batch_id, subscription_id);
}

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

async fn rollback_batch_members(conn: &ConnectionContext<'_>, resolved: &[ResolvedBatchMember]) {
	for member in resolved {
		if let ResolvedBatchMember::Local {
			subscription_id,
			..
		} = member && let Err(e) = cleanup_subscription(conn.state, *subscription_id).await
		{
			warn!("Failed to cleanup partial batch member {} during rollback: {:?}", subscription_id, e);
		}
	}
}

#[allow(clippy::too_many_arguments)]
async fn run_ws_hydrate(
	request_id: &str,
	conn: &mut ConnectionContext<'_>,
	subscription_id: SubscriptionId,
	rql: &str,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
	format: WireFormat,
) -> Option<String> {
	let service = match conn.state.engine().services().ioc.resolve() {
		Ok(s) => s,
		Err(e) => {
			abort_warming(conn, subscription_id).await;
			return Some(Response::internal_error(
				request_id,
				"SUBSCRIPTION_SERVICE_UNAVAILABLE",
				e.to_string(),
			)
			.to_json());
		}
	};

	let engine = conn.state.engine_clone();

	let (version, outcome) = match run_hydrate(service, engine, subscription_id, identity, lease, max_rows).await {
		Ok(t) => t,
		Err(err) => {
			abort_warming(conn, subscription_id).await;
			return Some(hydrate_error_to_response(request_id, err, rql, max_rows));
		}
	};

	if let Some((cols, metrics)) = outcome {
		let row_count = cols.row_count();
		info!(
			subscription_id = subscription_id.0,
			version = version.0,
			total_us = metrics.total.microseconds(),
			compute_us = metrics.compute.microseconds(),
			statement_count = metrics.statements.len(),
			row_count = row_count,
			fingerprint = %metrics.fingerprint.to_hex(),
			"ws hydrate completed"
		);
		if let Some(msg) = encode_change_for_handler(subscription_id, cols, format) {
			let _ = conn.push_tx.send(msg);
		}
	}

	match conn.registry.promote_to_live(subscription_id) {
		PromoteResult::Promoted(_) | PromoteResult::NotWarming | PromoteResult::NotFound => None,
		PromoteResult::Overflowed => Some(Response::internal_error(
			request_id,
			"HYDRATION_BACKPRESSURE",
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		)
		.to_json()),
		PromoteResult::Disconnected => None,
	}
}

fn hydrate_error_to_response(request_id: &str, err: HydrateError, rql: &str, cap: u64) -> String {
	Response::internal_error(request_id, err.wire_code(), err.wire_message(rql, cap)).to_json()
}

async fn abort_warming(conn: &mut ConnectionContext<'_>, subscription_id: SubscriptionId) {
	conn.registry.unsubscribe(subscription_id);
	if let Err(e) = cleanup_subscription(conn.state, subscription_id).await {
		warn!("Failed to cleanup subscription {} after hydrate failure: {:?}", subscription_id, e);
	}
}
