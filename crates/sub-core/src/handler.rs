// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

#[cfg(not(reifydb_single_threaded))]
use reifydb_client::{
	HydrationConfig as ClientHydrationConfig, Linger as ClientLinger,
	SubscriptionConfig as ClientSubscriptionConfig, Throttle as ClientThrottle,
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			id::SubscriptionId,
			subscription::{HydrationConfig, SubscribeOptions},
		},
		change::StagedBatch,
	},
	metrics::execution::ExecutionMetrics,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, SubscriptionServiceRef},
};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::{
	error::Error,
	params::Params,
	reifydb_assertions,
	value::{duration::Duration, frame::frame::Frame, identity::IdentityId},
};
#[cfg(not(reifydb_single_threaded))]
use tokio::spawn;
use tokio::{sync::watch::Receiver as WatchReceiver, task::JoinHandle};
use tracing::{debug, warn};

#[cfg(not(reifydb_single_threaded))]
use crate::{
	cleanup::cleanup_subscription,
	hydrate::run_hydrate,
	remote::{connect_remote, proxy_remote_to_sink},
};
#[cfg(reifydb_single_threaded)]
use crate::{cleanup::cleanup_subscription_sync, hydrate::run_hydrate_sync};
use crate::{
	create::{CreateSubscriptionResult, create_subscription},
	errors::CreateSubscriptionError,
	host::{SubscribeContext, SubscribeHost},
	registry::{ConnectionId, PromoteResult, SubscriptionRegistry},
	remote::RemoteSubscription,
	wire_sink::{BatchSubscribedEntry, WireSink},
};

#[derive(Debug, Clone)]
pub struct BatchSubscriptionInfo {
	pub index: usize,
	pub subscription_id: SubscriptionId,
}

pub struct BatchAck {
	pub batch_id: BatchId,
	pub subscriptions: Vec<BatchSubscriptionInfo>,
	pub remote_handles: Vec<JoinHandle<()>>,
}

pub struct SubscribeAck {
	pub subscription_id: SubscriptionId,
	pub remote_handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum SubscribeError<E> {
	Create(CreateSubscriptionError<E>),
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
pub enum BatchSubscribeError<E> {
	Empty,
	Create(CreateSubscriptionError<E>),
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

enum ResolvedBatchSubscription {
	Local {
		index: usize,
		subscription_id: SubscriptionId,
	},
	Remote {
		index: usize,
		subscription_id: SubscriptionId,
		remote_sub: Box<RemoteSubscription>,
	},
}

impl ResolvedBatchSubscription {
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
pub async fn handle_subscribe<S: WireSink, H: SubscribeHost>(
	host: &H,
	connection_id: ConnectionId,
	identity: IdentityId,
	query: String,
	params: Params,
	options: SubscribeOptions,
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
) -> Result<Result<SubscribeAck, SubscribeError<H::Error>>, Error> {
	match create_subscription(host, identity, &query, params, options).await {
		Ok(CreateSubscriptionResult::Local {
			id: subscription_id,
			hydration,
			throttle,
			linger,
		}) => {
			handle_subscribe_local(
				host,
				connection_id,
				identity,
				query,
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
		}) => Ok(handle_subscribe_remote(
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
		.await),
		Err(e) => Ok(Err(SubscribeError::Create(e))),
	}
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn handle_subscribe_local<S: WireSink, H: SubscribeHost>(
	host: &H,
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
) -> Result<Result<SubscribeAck, SubscribeError<H::Error>>, Error> {
	let ctx = host.context();
	let server_cap = ctx.max_hydration_rows();
	let throttle = ctx.clamp_throttle(throttle);
	let linger = ctx.clamp_linger(linger);
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

	registry.subscribe(subscription_id, connection_id, sink.clone(), format, warming_cap, throttle, linger);

	if !matches!(sink.send_subscribed(subscription_id), DeliveryResult::Delivered) {
		abort_warming(ctx.engine(), registry, subscription_id).await?;
		return Ok(Err(SubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before Subscribed could be delivered".to_string(),
		}));
	}

	if hydration.enabled {
		let lease = match ctx.engine().acquire_current_snapshot_lease() {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				abort_warming(ctx.engine(), registry, subscription_id).await?;
				return Ok(Err(SubscribeError::LeaseFailed {
					code,
					message: e.to_string(),
				}));
			}
		};

		if let Err(err) = run_subscription_hydrate(
			ctx,
			registry,
			&sink,
			subscription_id,
			&rql,
			identity,
			lease,
			max_rows,
			format,
		)
		.await?
		{
			return Ok(Err(err.into()));
		}
	} else {
		let _ = registry.promote_to_live(subscription_id);
	}

	debug!(
		"Connection {} subscribed: subscription_id={} hydration_enabled={}",
		connection_id, subscription_id, hydration.enabled
	);

	Ok(Ok(SubscribeAck {
		subscription_id,
		remote_handle: None,
	}))
}

#[cfg(reifydb_single_threaded)]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn handle_subscribe_remote<S: WireSink, E>(
	_connection_id: ConnectionId,
	_sink: S,
	_format: S::Format,
	_shutdown: WatchReceiver<bool>,
	_address: String,
	_body: String,
	_ns_token: Option<String>,
	_hydration: HydrationConfig,
	_throttle: Option<Duration>,
	_linger: Option<Duration>,
) -> Result<SubscribeAck, SubscribeError<E>> {
	Err(SubscribeError::RemoteConnect("Remote subscriptions require a threaded runtime".to_string()))
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn handle_subscribe_remote<S: WireSink, E>(
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
) -> Result<SubscribeAck, SubscribeError<E>> {
	let client_format = S::client_wire_format(format);
	let config = ClientSubscriptionConfig {
		hydration: ClientHydrationConfig {
			enabled: hydration.enabled,
			max_rows: hydration.max_rows,
		},
		throttle: throttle.map(ClientThrottle::new),
		linger: linger.map(ClientLinger::new),
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
		let proxied = proxy_remote_to_sink(remote_sub, shutdown, move |payload| {
			matches!(
				sink_for_proxy.send_remote_change(subscription_id, payload, format),
				DeliveryResult::Delivered
			)
		})
		.await;
		end_remote_proxy(proxied);
		let _ = sink_for_close.send_closed(subscription_id);
	});

	debug!("Connection {} subscribed to remote: subscription_id={}", connection_id, subscription_id);

	Ok(SubscribeAck {
		subscription_id,
		remote_handle: Some(handle),
	})
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_batch_subscribe<S: WireSink, H: SubscribeHost>(
	host: &H,
	connection_id: ConnectionId,
	identity: IdentityId,
	queries: &[(String, Params, SubscribeOptions)],
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
) -> Result<Result<BatchAck, BatchSubscribeError<H::Error>>, Error> {
	if queries.is_empty() {
		return Ok(Err(BatchSubscribeError::Empty));
	}

	let ctx = host.context();
	let (resolved, local_hydrations, subscription_lingers) =
		match resolve_batch_subscriptions::<S, H>(host, identity, queries, format).await? {
			Ok(resolved) => resolved,
			Err(e) => return Ok(Err(e)),
		};

	let server_cap = ctx.max_hydration_rows();
	let effective_max_rows = compute_effective_max_rows(&local_hydrations, server_cap);
	register_local_subscriptions(
		ctx,
		connection_id,
		&sink,
		registry,
		format,
		&resolved,
		&local_hydrations,
		&effective_max_rows,
		server_cap,
	);

	let (batch_id, subscriptions_for_ack, remote_subscriptions_taken) = match register_batch_and_ack(
		ctx,
		connection_id,
		&sink,
		registry,
		format,
		resolved,
		&subscription_lingers,
	)
	.await?
	{
		Ok(parts) => parts,
		Err(e) => return Ok(Err(e)),
	};

	if let Err(e) = hydrate_batch_locals(
		ctx,
		identity,
		&sink,
		registry,
		format,
		batch_id,
		&local_hydrations,
		&effective_max_rows,
		server_cap,
	)
	.await?
	{
		return Ok(Err(e));
	}

	let remote_handles = spawn_batch_remote_proxies(registry, batch_id, remote_subscriptions_taken, &shutdown);

	debug!(
		"Connection {} created batch {} with {} subscriptions",
		connection_id,
		batch_id,
		subscriptions_for_ack.len()
	);

	Ok(Ok(BatchAck {
		batch_id,
		subscriptions: subscriptions_for_ack,
		remote_handles,
	}))
}

type LocalHydration = (SubscriptionId, String, HydrationConfig, Option<Duration>);
type ResolvedBatch = (Vec<ResolvedBatchSubscription>, Vec<LocalHydration>, HashMap<SubscriptionId, Duration>);
type BatchAckParts = (BatchId, Vec<BatchSubscriptionInfo>, Vec<(SubscriptionId, RemoteSubscription)>);

#[inline]
async fn resolve_batch_subscriptions<S: WireSink, H: SubscribeHost>(
	host: &H,
	identity: IdentityId,
	queries: &[(String, Params, SubscribeOptions)],
	format: S::Format,
) -> Result<Result<ResolvedBatch, BatchSubscribeError<H::Error>>, Error> {
	let ctx = host.context();
	let mut resolved: Vec<ResolvedBatchSubscription> = Vec::with_capacity(queries.len());
	let mut local_hydrations: Vec<LocalHydration> = Vec::new();
	let mut subscription_lingers: HashMap<SubscriptionId, Duration> = HashMap::new();

	for (index, (query, params, options)) in queries.iter().enumerate() {
		match create_subscription(host, identity, query, params.clone(), options.clone()).await {
			Ok(CreateSubscriptionResult::Local {
				id: subscription_id,
				hydration,
				throttle,
				linger,
			}) => {
				subscription_lingers.insert(subscription_id, ctx.clamp_linger(linger));
				local_hydrations.push((subscription_id, query.clone(), hydration, throttle));
				resolved.push(ResolvedBatchSubscription::Local {
					index,
					subscription_id,
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
				let (subscription_id, remote_sub) =
					match connect_batch_remote_subscription::<S, H::Error>(
						address, body, ns_token, hydration, throttle, linger, format,
					)
					.await
					{
						Ok(connected) => connected,
						Err(e) => {
							rollback_batch_subscriptions(ctx.engine(), &resolved).await?;
							return Ok(Err(e));
						}
					};
				subscription_lingers.insert(subscription_id, ctx.clamp_linger(linger));
				resolved.push(ResolvedBatchSubscription::Remote {
					index,
					subscription_id,
					remote_sub: Box::new(remote_sub),
				});
			}
			Err(e) => {
				rollback_batch_subscriptions(ctx.engine(), &resolved).await?;
				return Ok(Err(BatchSubscribeError::Create(e)));
			}
		}
	}

	reifydb_assertions! {
		let resolved_len = resolved.len();
		let query_len = queries.len();
		assert!(
			resolved_len == query_len,
			"every query must resolve to exactly one batch subscription (resolved={resolved_len}, \
			 queries={query_len}); a count mismatch desyncs subscription indices from the \
			 BatchSubscribed ack and misroutes change frames to the wrong subscription"
		);
	}

	Ok(Ok((resolved, local_hydrations, subscription_lingers)))
}

#[cfg(reifydb_single_threaded)]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn connect_batch_remote_subscription<S: WireSink, E>(
	_address: String,
	_body: String,
	_ns_token: Option<String>,
	_hydration: HydrationConfig,
	_throttle: Option<Duration>,
	_linger: Option<Duration>,
	_format: S::Format,
) -> Result<(SubscriptionId, RemoteSubscription), BatchSubscribeError<E>> {
	Err(BatchSubscribeError::RemoteConnect("Remote subscriptions require a threaded runtime".to_string()))
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn connect_batch_remote_subscription<S: WireSink, E>(
	address: String,
	body: String,
	ns_token: Option<String>,
	hydration: HydrationConfig,
	throttle: Option<Duration>,
	linger: Option<Duration>,
	format: S::Format,
) -> Result<(SubscriptionId, RemoteSubscription), BatchSubscribeError<E>> {
	let client_format = S::client_wire_format(format);
	let config = ClientSubscriptionConfig {
		hydration: ClientHydrationConfig {
			enabled: hydration.enabled,
			max_rows: hydration.max_rows,
		},
		throttle: throttle.map(ClientThrottle::new),
		linger: linger.map(ClientLinger::new),
	};
	let remote_sub = connect_remote(&address, &body, config, ns_token.as_deref(), client_format)
		.await
		.map_err(|e| BatchSubscribeError::RemoteConnect(e.to_string()))?;
	let remote_id = remote_sub.subscription_id().to_string();
	let subscription_id =
		SubscriptionId(remote_id.parse::<u64>().map_err(|_| BatchSubscribeError::InvalidRemoteId)?);
	Ok((subscription_id, remote_sub))
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
fn register_local_subscriptions<S: WireSink>(
	ctx: &SubscribeContext,
	connection_id: ConnectionId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	resolved: &[ResolvedBatchSubscription],
	local_hydrations: &[LocalHydration],
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) {
	for subscription in resolved {
		if let ResolvedBatchSubscription::Local {
			subscription_id,
			..
		} = subscription
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
			let throttle = ctx.clamp_throttle(
				local_hydrations
					.iter()
					.find(|(sid, _, _, _)| sid == subscription_id)
					.and_then(|(_, _, _, t)| *t),
			);
			registry.subscribe(
				*subscription_id,
				connection_id,
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
async fn register_batch_and_ack<S: WireSink, E>(
	ctx: &SubscribeContext,
	connection_id: ConnectionId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	resolved: Vec<ResolvedBatchSubscription>,
	subscription_lingers: &HashMap<SubscriptionId, Duration>,
) -> Result<Result<BatchAckParts, BatchSubscribeError<E>>, Error> {
	let subscriptions: Vec<(SubscriptionId, Duration)> = resolved
		.iter()
		.map(|m| {
			let id = m.subscription_id();
			(id, subscription_lingers.get(&id).copied().unwrap_or(Duration::zero()))
		})
		.collect();
	let batch_id =
		registry.register_batch(connection_id, subscriptions, sink.clone(), format, ctx.clock(), ctx.rng());

	let mut remote_subscriptions_taken: Vec<(SubscriptionId, RemoteSubscription)> = Vec::new();
	let mut subscriptions_for_ack: Vec<BatchSubscriptionInfo> = Vec::with_capacity(resolved.len());
	for subscription in resolved {
		subscriptions_for_ack.push(BatchSubscriptionInfo {
			index: subscription.index(),
			subscription_id: subscription.subscription_id(),
		});
		if let ResolvedBatchSubscription::Remote {
			subscription_id,
			remote_sub,
			..
		} = subscription
		{
			remote_subscriptions_taken.push((subscription_id, *remote_sub));
		}
	}

	let subscribed_entries: Vec<BatchSubscribedEntry> = subscriptions_for_ack
		.iter()
		.map(|m| BatchSubscribedEntry {
			index: m.index,
			subscription_id: m.subscription_id,
		})
		.collect();
	if !matches!(sink.send_batch_subscribed(batch_id, &subscribed_entries), DeliveryResult::Delivered) {
		registry.unsubscribe_batch(batch_id);
		rollback_batch_subscriptions(ctx.engine(), &[]).await?;
		return Ok(Err(BatchSubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before BatchSubscribed could be delivered".to_string(),
		}));
	}

	Ok(Ok((batch_id, subscriptions_for_ack, remote_subscriptions_taken)))
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn hydrate_batch_locals<S: WireSink, E>(
	ctx: &SubscribeContext,
	identity: IdentityId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	batch_id: BatchId,
	local_hydrations: &[LocalHydration],
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) -> Result<Result<(), BatchSubscribeError<E>>, Error> {
	let any_hydration = local_hydrations.iter().any(|(_, _, h, _)| h.enabled);
	if any_hydration {
		let lease = match ctx.engine().acquire_current_snapshot_lease() {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				registry.unsubscribe_batch(batch_id);
				return Ok(Err(BatchSubscribeError::LeaseFailed {
					code,
					message: e.to_string(),
				}));
			}
		};
		for (sub_id, rql, hydration, _) in local_hydrations {
			if !hydration.enabled {
				continue;
			}
			let max_rows = *effective_max_rows.get(sub_id).unwrap_or(&server_cap);
			if let Err(err) = run_subscription_hydrate(
				ctx,
				registry,
				sink,
				*sub_id,
				rql,
				identity,
				lease.clone(),
				max_rows,
				format,
			)
			.await?
			{
				registry.unsubscribe_batch(batch_id);
				return Ok(Err(err.into_batch()));
			}
		}
	} else {
		for (sub_id, _, _, _) in local_hydrations {
			let _ = registry.promote_to_live(*sub_id);
		}
	}
	Ok(Ok(()))
}

#[cfg(reifydb_single_threaded)]
#[inline]
fn spawn_batch_remote_proxies<S: WireSink>(
	_registry: &Arc<SubscriptionRegistry<S>>,
	_batch_id: BatchId,
	remote_subscriptions_taken: Vec<(SubscriptionId, RemoteSubscription)>,
	_shutdown: &WatchReceiver<bool>,
) -> Vec<JoinHandle<()>> {
	reifydb_assertions! {
		let taken = remote_subscriptions_taken.len();
		assert!(
			taken == 0,
			"a single-threaded batch must never carry a remote subscription (taken={taken}); resolution \
			 rejects remote subscriptions before registration, so a non-empty list means a remote stream \
			 was accepted with no task to drain it"
		);
	}
	let _ = remote_subscriptions_taken;
	Vec::new()
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
fn spawn_batch_remote_proxies<S: WireSink>(
	registry: &Arc<SubscriptionRegistry<S>>,
	batch_id: BatchId,
	remote_subscriptions_taken: Vec<(SubscriptionId, RemoteSubscription)>,
	shutdown: &WatchReceiver<bool>,
) -> Vec<JoinHandle<()>> {
	let mut remote_handles: Vec<JoinHandle<()>> = Vec::with_capacity(remote_subscriptions_taken.len());
	for (subscription_id, remote_sub) in remote_subscriptions_taken {
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
	engine: &StandardEngine,
	registry: &Arc<SubscriptionRegistry<S>>,
	connection_id: ConnectionId,
	batch_id: BatchId,
) -> Result<Option<Vec<SubscriptionId>>, Error> {
	let Some(subscriptions) = registry.unsubscribe_batch_owned(connection_id, batch_id) else {
		return Ok(None);
	};
	for subscription_id in &subscriptions {
		#[cfg(not(reifydb_single_threaded))]
		cleanup_subscription(engine, *subscription_id).await?;
		#[cfg(reifydb_single_threaded)]
		cleanup_subscription_sync(engine, *subscription_id)?;
	}
	Ok(Some(subscriptions))
}

#[cfg(not(reifydb_single_threaded))]
async fn run_batch_remote_proxy<S: WireSink>(
	registry: Arc<SubscriptionRegistry<S>>,
	batch_id: BatchId,
	subscription_id: SubscriptionId,
	remote_sub: RemoteSubscription,
	shutdown: WatchReceiver<bool>,
) {
	let registry_push = Arc::clone(&registry);
	let proxied = proxy_remote_to_sink(remote_sub, shutdown, move |payload| {
		let frames = payload.into_frames();
		registry_push.push_batch_frames(batch_id, subscription_id, frames)
	})
	.await;
	end_remote_proxy(proxied);
	let _ = registry.emit_batch_subscription_closed(batch_id, subscription_id);
}

#[cfg(not(reifydb_single_threaded))]
fn end_remote_proxy(result: Result<(), Error>) {
	match result {
		Ok(()) => {}
		Err(error) if error.0.code == "CONNECTION_LOST" => {}
		Err(error) => panic!("remote subscription proxy failed: {error}"),
	}
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription_hydrate<S: WireSink>(
	ctx: &SubscribeContext,
	registry: &Arc<SubscriptionRegistry<S>>,
	sink: &S,
	subscription_id: SubscriptionId,
	rql: &str,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
	format: S::Format,
) -> Result<Result<(), SubscriptionHydrateError>, Error> {
	let service: SubscriptionServiceRef = match ctx.engine().services().ioc.resolve() {
		Ok(s) => s,
		Err(e) => {
			abort_warming(ctx.engine(), registry, subscription_id).await?;
			return Ok(Err(SubscriptionHydrateError {
				rql: rql.to_string(),
				max_rows,
				kind: SubscriptionHydrateErrorKind::ServiceUnavailable(e.to_string()),
			}));
		}
	};

	let engine = ctx.engine_clone();

	#[cfg(not(reifydb_single_threaded))]
	let hydrated = run_hydrate(service, engine, subscription_id, identity, lease, max_rows).await;
	#[cfg(reifydb_single_threaded)]
	let hydrated = run_hydrate_sync(service, engine, subscription_id, identity, lease, max_rows);

	let (version, batches, metrics): (CommitVersion, Vec<StagedBatch>, ExecutionMetrics) = match hydrated {
		Ok(t) => t,
		Err(err) => {
			abort_warming(ctx.engine(), registry, subscription_id).await?;
			return Ok(Err(SubscriptionHydrateError {
				rql: rql.to_string(),
				max_rows,
				kind: SubscriptionHydrateErrorKind::Failed(err),
			}));
		}
	};

	if !batches.is_empty() {
		let row_count: usize = batches.iter().map(|(_, cols)| cols.row_count()).sum();
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
			let frames: Vec<Frame> =
				batches.into_iter().map(|(op, cols)| Frame::from(cols).with_op(op)).collect();
			let _ = sink.send_batch_envelope(batch_id, format, vec![(subscription_id, frames)]);
		} else {
			for (op, cols) in batches {
				let _ = sink.send_change(subscription_id, op, cols, format);
			}
		}
	}

	Ok(match registry.promote_to_live(subscription_id) {
		PromoteResult::Promoted(_)
		| PromoteResult::NotWarming
		| PromoteResult::NotFound
		| PromoteResult::Disconnected => Ok(()),
		PromoteResult::Overflowed => Err(SubscriptionHydrateError {
			rql: rql.to_string(),
			max_rows,
			kind: SubscriptionHydrateErrorKind::Backpressure,
		}),
	})
}

struct SubscriptionHydrateError {
	rql: String,
	max_rows: u64,
	kind: SubscriptionHydrateErrorKind,
}

enum SubscriptionHydrateErrorKind {
	ServiceUnavailable(String),
	Failed(HydrateError),
	Backpressure,
}

impl SubscriptionHydrateError {
	fn into_batch<E>(self) -> BatchSubscribeError<E> {
		match self.kind {
			SubscriptionHydrateErrorKind::ServiceUnavailable(msg) => {
				BatchSubscribeError::HydrationServiceUnavailable(msg)
			}
			SubscriptionHydrateErrorKind::Failed(err) => BatchSubscribeError::HydrationFailed {
				error: err,
				rql: self.rql,
				max_rows: self.max_rows,
			},
			SubscriptionHydrateErrorKind::Backpressure => BatchSubscribeError::HydrationBackpressure,
		}
	}
}

impl<E> From<SubscriptionHydrateError> for SubscribeError<E> {
	fn from(e: SubscriptionHydrateError) -> Self {
		match e.kind {
			SubscriptionHydrateErrorKind::ServiceUnavailable(msg) => {
				SubscribeError::HydrationServiceUnavailable(msg)
			}
			SubscriptionHydrateErrorKind::Failed(err) => SubscribeError::HydrationFailed {
				error: err,
				rql: e.rql,
				max_rows: e.max_rows,
			},
			SubscriptionHydrateErrorKind::Backpressure => SubscribeError::HydrationBackpressure,
		}
	}
}

async fn rollback_batch_subscriptions(
	engine: &StandardEngine,
	resolved: &[ResolvedBatchSubscription],
) -> Result<(), Error> {
	for subscription in resolved {
		if let ResolvedBatchSubscription::Local {
			subscription_id,
			..
		} = subscription
		{
			#[cfg(not(reifydb_single_threaded))]
			cleanup_subscription(engine, *subscription_id).await?;
			#[cfg(reifydb_single_threaded)]
			cleanup_subscription_sync(engine, *subscription_id)?;
		}
	}
	Ok(())
}

async fn abort_warming<S: WireSink>(
	engine: &StandardEngine,
	registry: &Arc<SubscriptionRegistry<S>>,
	subscription_id: SubscriptionId,
) -> Result<(), Error> {
	registry.unsubscribe(subscription_id);

	#[cfg(not(reifydb_single_threaded))]
	cleanup_subscription(engine, subscription_id).await?;
	#[cfg(reifydb_single_threaded)]
	cleanup_subscription_sync(engine, subscription_id)?;
	Ok(())
}

#[cfg(all(test, not(reifydb_single_threaded)))]
mod tests {
	use reifydb_value::error::{Diagnostic, Error};

	use super::end_remote_proxy;

	fn error_with_code(code: &str) -> Error {
		Error(Box::new(Diagnostic {
			code: code.to_string(),
			message: "remote stream ended".to_string(),
			..Default::default()
		}))
	}

	#[test]
	fn a_lost_connection_ends_the_remote_proxy_quietly() {
		end_remote_proxy(Err(error_with_code("CONNECTION_LOST")));
	}

	#[test]
	#[should_panic(expected = "remote subscription proxy failed")]
	fn any_other_remote_error_panics_naming_the_proxy() {
		end_remote_proxy(Err(error_with_code("TRANSPORT")));
	}
}
