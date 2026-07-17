// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_client::{HydrationConfig as ClientHydrationConfig, SubscriptionConfig as ClientSubscriptionConfig};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::SubscriptionId, subscription::HydrationConfig},
	metric::ExecutionMetrics,
	value::column::columns::Columns,
};
use reifydb_engine::subscription::{HydrateError, SubscriptionServiceRef};
use reifydb_remote_proxy::{RemoteSubscription, connect_remote, proxy_remote_to_sink};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::{
	params::Params,
	reifydb_assertions,
	value::{duration::Duration, frame::frame::Frame, identity::IdentityId},
};
use tokio::{spawn, sync::watch::Receiver as WatchReceiver, task::JoinHandle};
use tracing::{debug, warn};

use crate::{
	interceptor::RequestMetadata,
	state::AppState,
	subscription::{
		cleanup::cleanup_subscription,
		create::{CreateSubscriptionResult, create_subscription},
		errors::CreateSubscriptionError,
		hydrate::run_hydrate,
		registry::{ConnectionId, PromoteResult, SubscriptionRegistry},
		wire_sink::{BatchSubscribedMember, WireSink},
	},
};

#[derive(Debug, Clone)]
pub struct BatchMemberInfo {
	pub index: usize,
	pub subscription_id: SubscriptionId,
}

pub struct BatchAck {
	pub batch_id: BatchId,
	pub members: Vec<BatchMemberInfo>,
	pub remote_handles: Vec<JoinHandle<()>>,
}

pub struct SubscribeAck {
	pub subscription_id: SubscriptionId,
	pub remote_handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum SubscribeError {
	Create(CreateSubscriptionError),
	RemoteConnect(String),
	InvalidRemoteId,
	LeaseFailed {
		code: &'static str,
		message: String,
	},
	HydrationBackpressure,
	HydrationFailed {
		error: HydrateError,
		rql: String,
		max_rows: u64,
	},
	HydrationServiceUnavailable(String),
}

#[derive(Debug)]
pub enum BatchSubscribeError {
	Empty,
	Create(CreateSubscriptionError),
	RemoteConnect(String),
	InvalidRemoteId,
	LeaseFailed {
		code: &'static str,
		message: String,
	},
	HydrationBackpressure,
	HydrationFailed {
		error: HydrateError,
		rql: String,
		max_rows: u64,
	},
	HydrationServiceUnavailable(String),
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

#[allow(clippy::too_many_arguments)]
pub async fn handle_subscribe<S: WireSink>(
	state: &AppState,
	connection_id: ConnectionId,
	identity: IdentityId,
	rql: String,
	params: Params,
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
	metadata: RequestMetadata,
) -> Result<SubscribeAck, SubscribeError> {
	match create_subscription(state, identity, &rql, params, metadata).await {
		Ok(CreateSubscriptionResult::Local {
			id: subscription_id,
			hydration,
			throttle,
			linger,
		}) => {
			handle_subscribe_local(
				state,
				connection_id,
				identity,
				rql,
				sink,
				registry,
				format,
				subscription_id,
				hydration,
				throttle,
				linger,
			)
			.await
		}
		Ok(CreateSubscriptionResult::Remote {
			address,
			body,
			token: ns_token,
			hydration,
			throttle,
			linger,
		}) => {
			handle_subscribe_remote(
				connection_id,
				sink,
				format,
				shutdown,
				address,
				body,
				ns_token,
				hydration,
				throttle,
				linger,
			)
			.await
		}
		Err(e) => Err(SubscribeError::Create(e)),
	}
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn handle_subscribe_local<S: WireSink>(
	state: &AppState,
	connection_id: ConnectionId,
	identity: IdentityId,
	rql: String,
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	subscription_id: SubscriptionId,
	hydration: HydrationConfig,
	throttle: Option<Duration>,
	linger: Option<Duration>,
) -> Result<SubscribeAck, SubscribeError> {
	let server_cap = state.subscribe_max_hydration_rows();
	let throttle = state.clamp_throttle(throttle);
	let linger = state.clamp_linger(linger);
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

	reifydb_assertions! {
		let warming = warming_cap.is_some();
		assert!(
			warming == hydration.enabled,
			"warming cap must be Some iff hydration is enabled (warming={warming}, enabled={}); \
			 a mismatch leaves a warming subscription that the hydrate-vs-promote branch never \
			 promotes to live, stranding the client",
			hydration.enabled
		);
	}

	registry.subscribe(
		subscription_id,
		connection_id,
		rql.clone(),
		sink.clone(),
		format,
		warming_cap,
		throttle,
		linger,
	);

	if !matches!(sink.send_subscribed(subscription_id), DeliveryResult::Delivered) {
		abort_warming(state, registry, subscription_id).await;
		return Err(SubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before Subscribed could be delivered".to_string(),
		});
	}

	if hydration.enabled {
		let lease = match state.engine().acquire_current_snapshot_lease() {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				abort_warming(state, registry, subscription_id).await;
				return Err(SubscribeError::LeaseFailed {
					code,
					message: e.to_string(),
				});
			}
		};

		if let Err(err) = run_member_hydrate(
			state,
			registry,
			&sink,
			subscription_id,
			&rql,
			identity,
			lease,
			max_rows,
			format,
		)
		.await
		{
			return Err(err.into());
		}
	} else {
		let _ = registry.promote_to_live(subscription_id);
	}

	debug!(
		"Connection {} subscribed: subscription_id={} hydration_enabled={}",
		connection_id, subscription_id, hydration.enabled
	);

	Ok(SubscribeAck {
		subscription_id,
		remote_handle: None,
	})
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn handle_subscribe_remote<S: WireSink>(
	connection_id: ConnectionId,
	sink: S,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
	address: String,
	body: String,
	ns_token: Option<String>,
	hydration: HydrationConfig,
	throttle: Option<Duration>,
	linger: Option<Duration>,
) -> Result<SubscribeAck, SubscribeError> {
	let client_format = S::client_wire_format(format);
	let config = ClientSubscriptionConfig {
		hydration: ClientHydrationConfig {
			enabled: hydration.enabled,
			max_rows: hydration.max_rows,
		},
		throttle,
		linger,
	};
	let remote_sub = connect_remote(&address, &body, config, ns_token.as_deref(), client_format)
		.await
		.map_err(|e| SubscribeError::RemoteConnect(e.to_string()))?;
	let remote_id = remote_sub.subscription_id().to_string();
	let subscription_id = SubscriptionId(remote_id.parse::<u64>().map_err(|_| SubscribeError::InvalidRemoteId)?);

	if !matches!(sink.send_subscribed(subscription_id), DeliveryResult::Delivered) {
		return Err(SubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before Subscribed could be delivered".to_string(),
		});
	}

	let sink_for_proxy = sink.clone();
	let sink_for_close = sink.clone();
	let handle = spawn(async move {
		proxy_remote_to_sink(remote_sub, shutdown, move |payload| {
			matches!(
				sink_for_proxy.send_remote_change(subscription_id, payload, format),
				DeliveryResult::Delivered
			)
		})
		.await;
		let _ = sink_for_close.send_closed(subscription_id);
	});

	debug!("Connection {} subscribed to remote: subscription_id={}", connection_id, subscription_id);

	Ok(SubscribeAck {
		subscription_id,
		remote_handle: Some(handle),
	})
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_batch_subscribe<S: WireSink>(
	state: &AppState,
	connection_id: ConnectionId,
	identity: IdentityId,
	queries: &[String],
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
	metadata: RequestMetadata,
) -> Result<BatchAck, BatchSubscribeError> {
	if queries.is_empty() {
		return Err(BatchSubscribeError::Empty);
	}

	let (resolved, local_hydrations, member_lingers) =
		resolve_batch_members::<S>(state, identity, queries, format, &metadata).await?;

	let server_cap = state.subscribe_max_hydration_rows();
	let effective_max_rows = compute_effective_max_rows(&local_hydrations, server_cap);
	register_local_members(
		state,
		connection_id,
		&sink,
		registry,
		format,
		&resolved,
		&local_hydrations,
		&effective_max_rows,
		server_cap,
	);

	let (batch_id, members_for_ack, remote_members_taken) =
		register_batch_and_ack(state, connection_id, &sink, registry, format, resolved, &member_lingers)
			.await?;

	hydrate_batch_locals(
		state,
		identity,
		&sink,
		registry,
		format,
		batch_id,
		&local_hydrations,
		&effective_max_rows,
		server_cap,
	)
	.await?;

	let remote_handles = spawn_batch_remote_proxies(registry, batch_id, remote_members_taken, &shutdown);

	debug!("Connection {} created batch {} with {} members", connection_id, batch_id, members_for_ack.len());

	Ok(BatchAck {
		batch_id,
		members: members_for_ack,
		remote_handles,
	})
}

type LocalHydration = (SubscriptionId, String, HydrationConfig, Option<Duration>);
type ResolvedBatch = (Vec<ResolvedBatchMember>, Vec<LocalHydration>, HashMap<SubscriptionId, Duration>);
type BatchAckParts = (BatchId, Vec<BatchMemberInfo>, Vec<(SubscriptionId, RemoteSubscription)>);

#[inline]
async fn resolve_batch_members<S: WireSink>(
	state: &AppState,
	identity: IdentityId,
	queries: &[String],
	format: S::Format,
	metadata: &RequestMetadata,
) -> Result<ResolvedBatch, BatchSubscribeError> {
	let mut resolved: Vec<ResolvedBatchMember> = Vec::with_capacity(queries.len());
	let mut local_hydrations: Vec<LocalHydration> = Vec::new();
	let mut member_lingers: HashMap<SubscriptionId, Duration> = HashMap::new();

	for (index, user_rql) in queries.iter().enumerate() {
		match create_subscription(state, identity, user_rql, Params::None, metadata.clone()).await {
			Ok(CreateSubscriptionResult::Local {
				id: subscription_id,
				hydration,
				throttle,
				linger,
			}) => {
				member_lingers.insert(subscription_id, state.clamp_linger(linger));
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
				linger,
			}) => {
				let client_format = S::client_wire_format(format);
				let config = ClientSubscriptionConfig {
					hydration: ClientHydrationConfig {
						enabled: hydration.enabled,
						max_rows: hydration.max_rows,
					},
					throttle,
					linger,
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
						rollback_batch_members(state, &resolved).await;
						return Err(BatchSubscribeError::RemoteConnect(e.to_string()));
					}
				};
				let remote_id = remote_sub.subscription_id().to_string();
				let subscription_id = match remote_id.parse::<u64>() {
					Ok(n) => SubscriptionId(n),
					Err(_) => {
						rollback_batch_members(state, &resolved).await;
						return Err(BatchSubscribeError::InvalidRemoteId);
					}
				};
				member_lingers.insert(subscription_id, state.clamp_linger(linger));
				resolved.push(ResolvedBatchMember::Remote {
					index,
					subscription_id,
					remote_sub: Box::new(remote_sub),
				});
			}
			Err(e) => {
				rollback_batch_members(state, &resolved).await;
				return Err(BatchSubscribeError::Create(e));
			}
		}
	}

	reifydb_assertions! {
		let resolved_len = resolved.len();
		let query_len = queries.len();
		assert!(
			resolved_len == query_len,
			"every query must resolve to exactly one batch member (resolved={resolved_len}, \
			 queries={query_len}); a count mismatch desyncs member indices from the \
			 BatchSubscribed ack and misroutes change frames to the wrong subscription"
		);
	}

	Ok((resolved, local_hydrations, member_lingers))
}

#[inline]
fn compute_effective_max_rows(local_hydrations: &[LocalHydration], server_cap: u64) -> HashMap<SubscriptionId, u64> {
	let mut effective_max_rows: HashMap<SubscriptionId, u64> = HashMap::new();
	for (sub_id, _, hydration, _) in local_hydrations {
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
	effective_max_rows
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn register_local_members<S: WireSink>(
	state: &AppState,
	connection_id: ConnectionId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	resolved: &[ResolvedBatchMember],
	local_hydrations: &[LocalHydration],
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) {
	for member in resolved {
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
			let throttle = state.clamp_throttle(
				local_hydrations
					.iter()
					.find(|(sid, _, _, _)| sid == subscription_id)
					.and_then(|(_, _, _, t)| *t),
			);
			registry.subscribe(
				*subscription_id,
				connection_id,
				query.clone(),
				sink.clone(),
				format,
				warming_cap,
				throttle,
				Duration::zero(),
			);
		}
	}
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn register_batch_and_ack<S: WireSink>(
	state: &AppState,
	connection_id: ConnectionId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	resolved: Vec<ResolvedBatchMember>,
	member_lingers: &HashMap<SubscriptionId, Duration>,
) -> Result<BatchAckParts, BatchSubscribeError> {
	let members: Vec<(SubscriptionId, Duration)> = resolved
		.iter()
		.map(|m| {
			let id = m.subscription_id();
			(id, member_lingers.get(&id).copied().unwrap_or(Duration::zero()))
		})
		.collect();
	let batch_id =
		registry.register_batch(connection_id, members, sink.clone(), format, state.clock(), state.rng());

	let mut remote_members_taken: Vec<(SubscriptionId, RemoteSubscription)> = Vec::new();
	let mut members_for_ack: Vec<BatchMemberInfo> = Vec::with_capacity(resolved.len());
	for member in resolved {
		members_for_ack.push(BatchMemberInfo {
			index: member.index(),
			subscription_id: member.subscription_id(),
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

	let subscribed_members: Vec<BatchSubscribedMember> = members_for_ack
		.iter()
		.map(|m| BatchSubscribedMember {
			index: m.index,
			subscription_id: m.subscription_id,
		})
		.collect();
	if !matches!(sink.send_batch_subscribed(batch_id, &subscribed_members), DeliveryResult::Delivered) {
		registry.unsubscribe_batch(batch_id);
		rollback_batch_members(state, &[]).await;
		return Err(BatchSubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before BatchSubscribed could be delivered".to_string(),
		});
	}

	Ok((batch_id, members_for_ack, remote_members_taken))
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn hydrate_batch_locals<S: WireSink>(
	state: &AppState,
	identity: IdentityId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	batch_id: BatchId,
	local_hydrations: &[LocalHydration],
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) -> Result<(), BatchSubscribeError> {
	let any_hydration = local_hydrations.iter().any(|(_, _, h, _)| h.enabled);
	if any_hydration {
		let lease = match state.engine().acquire_current_snapshot_lease() {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				registry.unsubscribe_batch(batch_id);
				return Err(BatchSubscribeError::LeaseFailed {
					code,
					message: e.to_string(),
				});
			}
		};
		for (sub_id, rql, hydration, _) in local_hydrations {
			if !hydration.enabled {
				continue;
			}
			let max_rows = *effective_max_rows.get(sub_id).unwrap_or(&server_cap);
			if let Err(err) = run_member_hydrate(
				state,
				registry,
				sink,
				*sub_id,
				rql,
				identity,
				lease.clone(),
				max_rows,
				format,
			)
			.await
			{
				registry.unsubscribe_batch(batch_id);
				return Err(err.into_batch());
			}
		}
	} else {
		for (sub_id, _, _, _) in local_hydrations {
			let _ = registry.promote_to_live(*sub_id);
		}
	}
	Ok(())
}

#[inline]
fn spawn_batch_remote_proxies<S: WireSink>(
	registry: &Arc<SubscriptionRegistry<S>>,
	batch_id: BatchId,
	remote_members_taken: Vec<(SubscriptionId, RemoteSubscription)>,
	shutdown: &WatchReceiver<bool>,
) -> Vec<JoinHandle<()>> {
	let mut remote_handles: Vec<JoinHandle<()>> = Vec::with_capacity(remote_members_taken.len());
	for (subscription_id, remote_sub) in remote_members_taken {
		let registry_clone = Arc::clone(registry);
		let proxy_shutdown = shutdown.clone();
		let handle = spawn(async move {
			run_batch_remote_proxy(registry_clone, batch_id, subscription_id, remote_sub, proxy_shutdown)
				.await;
		});
		remote_handles.push(handle);
	}
	remote_handles
}

pub async fn handle_batch_unsubscribe<S: WireSink>(
	state: &AppState,
	registry: &Arc<SubscriptionRegistry<S>>,
	batch_id: BatchId,
) -> Option<Vec<SubscriptionId>> {
	let members = registry.unsubscribe_batch(batch_id)?;
	for subscription_id in &members {
		if let Err(e) = cleanup_subscription(state, *subscription_id).await {
			warn!("Failed to cleanup batch member subscription {} from database: {:?}", subscription_id, e);
		}
	}
	Some(members)
}

async fn run_batch_remote_proxy<S: WireSink>(
	registry: Arc<SubscriptionRegistry<S>>,
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

#[allow(clippy::too_many_arguments)]
async fn run_member_hydrate<S: WireSink>(
	state: &AppState,
	registry: &Arc<SubscriptionRegistry<S>>,
	sink: &S,
	subscription_id: SubscriptionId,
	rql: &str,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
	format: S::Format,
) -> Result<(), MemberHydrateError> {
	let service: SubscriptionServiceRef = match state.engine().services().ioc.resolve() {
		Ok(s) => s,
		Err(e) => {
			abort_warming(state, registry, subscription_id).await;
			return Err(MemberHydrateError {
				rql: rql.to_string(),
				max_rows,
				kind: MemberHydrateErrorKind::ServiceUnavailable(e.to_string()),
			});
		}
	};

	let engine = state.engine_clone();

	let (version, outcome): (CommitVersion, Option<(Columns, ExecutionMetrics)>) =
		match run_hydrate(service, engine, subscription_id, identity, lease, max_rows).await {
			Ok(t) => t,
			Err(err) => {
				abort_warming(state, registry, subscription_id).await;
				return Err(MemberHydrateError {
					rql: rql.to_string(),
					max_rows,
					kind: MemberHydrateErrorKind::Failed(err),
				});
			}
		};

	if let Some((cols, metrics)) = outcome {
		let row_count = cols.row_count();
		debug!(
			subscription_id = subscription_id.0,
			version = version.0,
			total_us = metrics.total.microseconds().unwrap_or(0),
			compute_us = metrics.compute.microseconds().unwrap_or(0),
			statement_count = metrics.statements.len(),
			row_count = row_count,
			fingerprint = %metrics.fingerprint.to_hex(),
			"hydrate completed"
		);
		if let Some(batch_id) = registry.batch_for(&subscription_id) {
			let _ = sink.send_batch_envelope(
				batch_id,
				format,
				vec![(subscription_id, vec![Frame::from(cols)])],
			);
		} else {
			let _ = sink.send_change(subscription_id, cols, format);
		}
	}

	match registry.promote_to_live(subscription_id) {
		PromoteResult::Promoted(_)
		| PromoteResult::NotWarming
		| PromoteResult::NotFound
		| PromoteResult::Disconnected => Ok(()),
		PromoteResult::Overflowed => Err(MemberHydrateError {
			rql: rql.to_string(),
			max_rows,
			kind: MemberHydrateErrorKind::Backpressure,
		}),
	}
}

struct MemberHydrateError {
	rql: String,
	max_rows: u64,
	kind: MemberHydrateErrorKind,
}

enum MemberHydrateErrorKind {
	ServiceUnavailable(String),
	Failed(HydrateError),
	Backpressure,
}

impl MemberHydrateError {
	fn into_batch(self) -> BatchSubscribeError {
		match self.kind {
			MemberHydrateErrorKind::ServiceUnavailable(msg) => {
				BatchSubscribeError::HydrationServiceUnavailable(msg)
			}
			MemberHydrateErrorKind::Failed(err) => BatchSubscribeError::HydrationFailed {
				error: err,
				rql: self.rql,
				max_rows: self.max_rows,
			},
			MemberHydrateErrorKind::Backpressure => BatchSubscribeError::HydrationBackpressure,
		}
	}
}

impl From<MemberHydrateError> for SubscribeError {
	fn from(e: MemberHydrateError) -> Self {
		match e.kind {
			MemberHydrateErrorKind::ServiceUnavailable(msg) => {
				SubscribeError::HydrationServiceUnavailable(msg)
			}
			MemberHydrateErrorKind::Failed(err) => SubscribeError::HydrationFailed {
				error: err,
				rql: e.rql,
				max_rows: e.max_rows,
			},
			MemberHydrateErrorKind::Backpressure => SubscribeError::HydrationBackpressure,
		}
	}
}

async fn rollback_batch_members(state: &AppState, resolved: &[ResolvedBatchMember]) {
	for member in resolved {
		if let ResolvedBatchMember::Local {
			subscription_id,
			..
		} = member && let Err(e) = cleanup_subscription(state, *subscription_id).await
		{
			warn!("Failed to cleanup partial batch member {} during rollback: {:?}", subscription_id, e);
		}
	}
}

async fn abort_warming<S: WireSink>(
	state: &AppState,
	registry: &Arc<SubscriptionRegistry<S>>,
	subscription_id: SubscriptionId,
) {
	registry.unsubscribe(subscription_id);
	if let Err(e) = cleanup_subscription(state, subscription_id).await {
		warn!("Failed to cleanup subscription {} after hydrate failure: {:?}", subscription_id, e);
	}
}
