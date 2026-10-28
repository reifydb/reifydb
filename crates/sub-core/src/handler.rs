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
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, SubscriptionServiceRef, acquire_hand_off_lease},
};
use reifydb_subscription::delivery::DeliveryResult;
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
	wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
};

pub struct SubscribeAck {
	pub subscriptions: Vec<SubscribedEntry>,
	pub remote_handles: Vec<(SubscriptionId, JoinHandle<()>)>,
}

pub struct SubscribeHydration {
	identity: IdentityId,
	subscription_ids: Vec<SubscriptionId>,
	local_hydrations: Vec<LocalHydration>,
	effective_max_rows: HashMap<SubscriptionId, u64>,
	server_cap: u64,
}

#[derive(Debug)]
pub enum SubscribeError<E> {
	Empty,
	Create {
		index: usize,
		error: CreateSubscriptionError<E>,
	},
	RemoteConnect {
		index: usize,
		message: String,
	},
	InvalidRemoteId {
		index: usize,
	},
	LeaseFailed {
		code: &'static str,
		message: String,
	},
	HydrationBackpressure,
	HydrationFailed {
		index: usize,
		error: HydrateError,
		rql: String,
		max_rows: u64,
	},
	HydrationServiceUnavailable(String),
}

impl<E> SubscribeError<E> {
	pub fn index(&self) -> Option<usize> {
		match self {
			Self::Create {
				index,
				..
			}
			| Self::RemoteConnect {
				index,
				..
			}
			| Self::InvalidRemoteId {
				index,
			}
			| Self::HydrationFailed {
				index,
				..
			} => Some(*index),
			Self::Empty
			| Self::LeaseFailed {
				..
			}
			| Self::HydrationBackpressure
			| Self::HydrationServiceUnavailable(_) => None,
		}
	}
}

enum ResolvedSubscription {
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

impl ResolvedSubscription {
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
	queries: &[(String, Params, SubscribeOptions)],
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
) -> Result<Result<SubscribeAck, SubscribeError<H::Error>>, Error> {
	let (ack, hydration) = match register_subscribe(
		host,
		connection_id,
		identity,
		queries,
		sink.clone(),
		registry,
		format,
		shutdown,
	)
	.await?
	{
		Ok(parts) => parts,
		Err(e) => return Ok(Err(e)),
	};
	if let Err(e) = hydrate_subscribe(host.context(), &sink, registry, format, hydration).await? {
		for (_, handle) in ack.remote_handles {
			handle.abort();
		}
		return Ok(Err(e));
	}
	Ok(Ok(ack))
}

#[allow(clippy::too_many_arguments)]
pub async fn register_subscribe<S: WireSink, H: SubscribeHost>(
	host: &H,
	connection_id: ConnectionId,
	identity: IdentityId,
	queries: &[(String, Params, SubscribeOptions)],
	sink: S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	shutdown: WatchReceiver<bool>,
) -> Result<Result<(SubscribeAck, SubscribeHydration), SubscribeError<H::Error>>, Error> {
	if queries.is_empty() {
		return Ok(Err(SubscribeError::Empty));
	}

	let ctx = host.context();
	let (resolved, local_hydrations, subscription_lingers) =
		match resolve_subscriptions::<S, H>(host, identity, queries, format).await? {
			Ok(resolved) => resolved,
			Err(e) => return Ok(Err(e)),
		};
	let subscription_ids: Vec<SubscriptionId> =
		resolved.iter().map(ResolvedSubscription::subscription_id).collect();

	let server_cap = ctx.max_hydration_rows();
	let effective_max_rows = compute_effective_max_rows(&local_hydrations, server_cap);
	register_local_subscriptions(
		ctx,
		connection_id,
		&sink,
		registry,
		format,
		&local_hydrations,
		&subscription_lingers,
		&effective_max_rows,
		server_cap,
	);

	let (subscriptions, remote_subscriptions) =
		match register_remotes_and_ack(connection_id, &sink, registry, format, resolved) {
			Ok(parts) => parts,
			Err(e) => {
				rollback_local_subscriptions(ctx.engine(), &local_hydrations).await?;
				return Ok(Err(e));
			}
		};

	let remote_handles = spawn_remote_proxies(registry, remote_subscriptions, &shutdown);

	debug!("Connection {} subscribed {} queries in one call", connection_id, subscriptions.len());

	Ok(Ok((
		SubscribeAck {
			subscriptions,
			remote_handles,
		},
		SubscribeHydration {
			identity,
			subscription_ids,
			local_hydrations,
			effective_max_rows,
			server_cap,
		},
	)))
}

pub async fn hydrate_subscribe<S: WireSink, E>(
	ctx: &SubscribeContext,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	hydration: SubscribeHydration,
) -> Result<Result<(), SubscribeError<E>>, Error> {
	hydrate_locals(
		ctx,
		hydration.identity,
		sink,
		registry,
		format,
		&hydration.subscription_ids,
		&hydration.local_hydrations,
		&hydration.effective_max_rows,
		hydration.server_cap,
	)
	.await
}

type LocalHydration = (SubscriptionId, String, HydrationConfig, Option<Duration>);
type Resolved = (Vec<ResolvedSubscription>, Vec<LocalHydration>, HashMap<SubscriptionId, Duration>);
type AckParts = (Vec<SubscribedEntry>, Vec<(SubscriptionId, RemoteSubscription)>);

#[inline]
async fn resolve_subscriptions<S: WireSink, H: SubscribeHost>(
	host: &H,
	identity: IdentityId,
	queries: &[(String, Params, SubscribeOptions)],
	format: S::Format,
) -> Result<Result<Resolved, SubscribeError<H::Error>>, Error> {
	let ctx = host.context();
	let mut resolved: Vec<ResolvedSubscription> = Vec::with_capacity(queries.len());
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
				resolved.push(ResolvedSubscription::Local {
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
				let (subscription_id, remote_sub) = match connect_remote_subscription::<S, H::Error>(
					index, address, body, ns_token, hydration, throttle, linger, format,
				)
				.await
				{
					Ok(connected) => connected,
					Err(e) => {
						rollback_resolved(ctx.engine(), &resolved).await?;
						return Ok(Err(e));
					}
				};
				resolved.push(ResolvedSubscription::Remote {
					index,
					subscription_id,
					remote_sub: Box::new(remote_sub),
				});
			}
			Err(e) => {
				rollback_resolved(ctx.engine(), &resolved).await?;
				return Ok(Err(SubscribeError::Create {
					index,
					error: e,
				}));
			}
		}
	}

	reifydb_assertions! {
		let resolved_len = resolved.len();
		let query_len = queries.len();
		assert!(
			resolved_len == query_len,
			"every query must resolve to exactly one subscription (resolved={resolved_len}, \
			 queries={query_len}); a count mismatch desyncs subscription indices from the \
			 Subscribed ack and misroutes change frames to the wrong subscription"
		);
	}

	Ok(Ok((resolved, local_hydrations, subscription_lingers)))
}

#[cfg(reifydb_single_threaded)]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn connect_remote_subscription<S: WireSink, E>(
	index: usize,
	_address: String,
	_body: String,
	_ns_token: Option<String>,
	_hydration: HydrationConfig,
	_throttle: Option<Duration>,
	_linger: Option<Duration>,
	_format: S::Format,
) -> Result<(SubscriptionId, RemoteSubscription), SubscribeError<E>> {
	Err(SubscribeError::RemoteConnect {
		index,
		message: "Remote subscriptions require a threaded runtime".to_string(),
	})
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
#[allow(clippy::too_many_arguments)]
async fn connect_remote_subscription<S: WireSink, E>(
	index: usize,
	address: String,
	body: String,
	ns_token: Option<String>,
	hydration: HydrationConfig,
	throttle: Option<Duration>,
	linger: Option<Duration>,
	format: S::Format,
) -> Result<(SubscriptionId, RemoteSubscription), SubscribeError<E>> {
	let client_format = S::client_wire_format(format);
	let config = ClientSubscriptionConfig {
		hydration: ClientHydrationConfig {
			enabled: hydration.enabled,
			max_rows: hydration.max_rows,
		},
		throttle: throttle.map(ClientThrottle::new),
		linger: linger.map(ClientLinger::new),
	};
	let remote_sub =
		connect_remote(&address, &body, config, ns_token.as_deref(), client_format).await.map_err(|e| {
			SubscribeError::RemoteConnect {
				index,
				message: e.to_string(),
			}
		})?;
	let remote_id = remote_sub.subscription_id().to_string();
	let subscription_id =
		SubscriptionId(remote_id.parse::<u64>().map_err(|_| SubscribeError::InvalidRemoteId {
			index,
		})?);
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
	local_hydrations: &[LocalHydration],
	subscription_lingers: &HashMap<SubscriptionId, Duration>,
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) {
	for (subscription_id, _, hydration, throttle) in local_hydrations {
		let warming_cap = hydration
			.enabled
			.then(|| *effective_max_rows.get(subscription_id).unwrap_or(&server_cap) as usize);
		registry.subscribe(
			*subscription_id,
			connection_id,
			sink.clone(),
			format,
			warming_cap,
			ctx.clamp_throttle(*throttle),
			subscription_lingers.get(subscription_id).copied().unwrap_or(Duration::zero()),
		);
	}
}

#[inline]
fn register_remotes_and_ack<S: WireSink, E>(
	connection_id: ConnectionId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	resolved: Vec<ResolvedSubscription>,
) -> Result<AckParts, SubscribeError<E>> {
	let mut remote_subscriptions: Vec<(SubscriptionId, RemoteSubscription)> = Vec::new();
	let mut subscriptions: Vec<SubscribedEntry> = Vec::with_capacity(resolved.len());
	for subscription in resolved {
		subscriptions.push(SubscribedEntry {
			index: subscription.index(),
			subscription_id: subscription.subscription_id(),
		});
		if let ResolvedSubscription::Remote {
			subscription_id,
			remote_sub,
			..
		} = subscription
		{
			registry.subscribe_remote(subscription_id, connection_id, sink.clone(), format);
			remote_subscriptions.push((subscription_id, *remote_sub));
		}
	}

	if !matches!(sink.send_subscribed(&subscriptions), DeliveryResult::Delivered) {
		for entry in &subscriptions {
			registry.unsubscribe(entry.subscription_id);
		}
		return Err(SubscribeError::LeaseFailed {
			code: "STREAM_CLOSED",
			message: "Client stream closed before Subscribed could be delivered".to_string(),
		});
	}

	Ok((subscriptions, remote_subscriptions))
}

#[inline]
#[allow(clippy::too_many_arguments)]
async fn hydrate_locals<S: WireSink, E>(
	ctx: &SubscribeContext,
	identity: IdentityId,
	sink: &S,
	registry: &Arc<SubscriptionRegistry<S>>,
	format: S::Format,
	subscription_ids: &[SubscriptionId],
	local_hydrations: &[LocalHydration],
	effective_max_rows: &HashMap<SubscriptionId, u64>,
	server_cap: u64,
) -> Result<Result<(), SubscribeError<E>>, Error> {
	let hydrating: Vec<SubscriptionId> =
		local_hydrations.iter().filter(|(_, _, h, _)| h.enabled).map(|(sub_id, _, _, _)| *sub_id).collect();
	if !hydrating.is_empty() {
		let lease = match acquire_hand_off_lease(ctx.engine(), &hydrating) {
			Ok((_, lease)) => lease,
			Err(e) => {
				let code = if e.0.code == "TXN_012" {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"PIN_VERSION_FAILED"
				};
				unregister_all(registry, subscription_ids);
				rollback_local_subscriptions(ctx.engine(), local_hydrations).await?;
				return Ok(Err(SubscribeError::LeaseFailed {
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
				unregister_all(registry, subscription_ids);
				rollback_local_subscriptions(ctx.engine(), local_hydrations).await?;
				let index = subscription_ids
					.iter()
					.position(|id| id == sub_id)
					.expect("every local hydration belongs to a subscription of this call");
				return Ok(Err(err.into_subscribe_error(index)));
			}
		}
	} else {
		for (sub_id, _, _, _) in local_hydrations {
			let _ = registry.promote_to_live(*sub_id);
		}
	}
	Ok(Ok(()))
}

#[inline]
fn unregister_all<S: WireSink>(registry: &Arc<SubscriptionRegistry<S>>, subscription_ids: &[SubscriptionId]) {
	for subscription_id in subscription_ids {
		registry.unsubscribe(*subscription_id);
	}
}

#[cfg(reifydb_single_threaded)]
#[inline]
fn spawn_remote_proxies<S: WireSink>(
	_registry: &Arc<SubscriptionRegistry<S>>,
	remote_subscriptions: Vec<(SubscriptionId, RemoteSubscription)>,
	_shutdown: &WatchReceiver<bool>,
) -> Vec<(SubscriptionId, JoinHandle<()>)> {
	reifydb_assertions! {
		let taken = remote_subscriptions.len();
		assert!(
			taken == 0,
			"a single-threaded subscribe must never carry a remote subscription (taken={taken}); resolution \
			 rejects remote subscriptions before registration, so a non-empty list means a remote stream \
			 was accepted with no task to drain it"
		);
	}
	let _ = remote_subscriptions;
	Vec::new()
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
fn spawn_remote_proxies<S: WireSink>(
	registry: &Arc<SubscriptionRegistry<S>>,
	remote_subscriptions: Vec<(SubscriptionId, RemoteSubscription)>,
	shutdown: &WatchReceiver<bool>,
) -> Vec<(SubscriptionId, JoinHandle<()>)> {
	let mut remote_handles = Vec::with_capacity(remote_subscriptions.len());
	for (subscription_id, remote_sub) in remote_subscriptions {
		let registry = Arc::clone(registry);
		let proxy_shutdown = shutdown.clone();
		let handle = spawn(async move {
			run_remote_proxy(registry, subscription_id, remote_sub, proxy_shutdown).await;
		});
		remote_handles.push((subscription_id, handle));
	}
	remote_handles
}

pub async fn handle_unsubscribe<S: WireSink>(
	engine: &StandardEngine,
	registry: &Arc<SubscriptionRegistry<S>>,
	connection_id: ConnectionId,
	subscription_ids: &[SubscriptionId],
) -> Result<Vec<SubscriptionId>, Error> {
	let removed = registry.unsubscribe_owned(connection_id, subscription_ids);
	for subscription_id in &removed {
		#[cfg(not(reifydb_single_threaded))]
		cleanup_subscription(engine, *subscription_id).await?;
		#[cfg(reifydb_single_threaded)]
		cleanup_subscription_sync(engine, *subscription_id)?;
	}
	Ok(removed)
}

#[cfg(not(reifydb_single_threaded))]
async fn run_remote_proxy<S: WireSink>(
	registry: Arc<SubscriptionRegistry<S>>,
	subscription_id: SubscriptionId,
	remote_sub: RemoteSubscription,
	shutdown: WatchReceiver<bool>,
) {
	let registry_push = Arc::clone(&registry);
	let proxied = proxy_remote_to_sink(remote_sub, shutdown, move |payload| {
		registry_push.push_payload(subscription_id, payload)
	})
	.await;
	end_remote_proxy(proxied);
	registry.close(subscription_id);
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

	#[cfg(not(reifydb_single_threaded))]
	let hydrated = run_hydrate(service, subscription_id, identity, lease, max_rows).await;
	#[cfg(reifydb_single_threaded)]
	let hydrated = run_hydrate_sync(service, subscription_id, identity, lease, max_rows);

	let (version, batches, total): (CommitVersion, Vec<StagedBatch>, Duration) = match hydrated {
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
		let row_count: usize = batches.iter().map(|(_, cols)| cols.num_rows()).sum();
		debug!(
			subscription_id = subscription_id.0,
			version = version.0,
			total_us = total.microseconds().unwrap_or(0),
			row_count = row_count,
			"hydrate completed"
		);
		let frames: Vec<Frame> = batches.into_iter().map(|(op, cols)| Frame::from(cols).with_op(op)).collect();
		match sink.send_change(format, vec![(subscription_id, frames)]) {
			ChangeDelivery::Delivered {
				failed,
			} if failed.contains(&subscription_id) => {
				registry.close(subscription_id);
				#[cfg(not(reifydb_single_threaded))]
				cleanup_subscription(ctx.engine(), subscription_id).await?;
				#[cfg(reifydb_single_threaded)]
				cleanup_subscription_sync(ctx.engine(), subscription_id)?;
				return Ok(Ok(()));
			}
			ChangeDelivery::Delivered {
				..
			}
			| ChangeDelivery::Disconnected => {}
		}
	}

	Ok(match registry.promote_to_live(subscription_id) {
		PromoteResult::Promoted(_) | PromoteResult::NotWarming | PromoteResult::NotFound => Ok(()),
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
	fn into_subscribe_error<E>(self, index: usize) -> SubscribeError<E> {
		match self.kind {
			SubscriptionHydrateErrorKind::ServiceUnavailable(msg) => {
				SubscribeError::HydrationServiceUnavailable(msg)
			}
			SubscriptionHydrateErrorKind::Failed(err) => SubscribeError::HydrationFailed {
				index,
				error: err,
				rql: self.rql,
				max_rows: self.max_rows,
			},
			SubscriptionHydrateErrorKind::Backpressure => SubscribeError::HydrationBackpressure,
		}
	}
}

async fn rollback_resolved(engine: &StandardEngine, resolved: &[ResolvedSubscription]) -> Result<(), Error> {
	for subscription in resolved {
		if let ResolvedSubscription::Local {
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

async fn rollback_local_subscriptions(
	engine: &StandardEngine,
	local_hydrations: &[LocalHydration],
) -> Result<(), Error> {
	for (subscription_id, _, _, _) in local_hydrations {
		#[cfg(not(reifydb_single_threaded))]
		cleanup_subscription(engine, *subscription_id).await?;
		#[cfg(reifydb_single_threaded)]
		cleanup_subscription_sync(engine, *subscription_id)?;
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
	use reifydb_engine::subscription::HydrateError;
	use reifydb_value::error::{Diagnostic, Error};

	use super::{SubscribeError, end_remote_proxy};
	use crate::errors::CreateSubscriptionError;

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

	#[test]
	fn only_item_errors_name_an_index() {
		// A whole-call error must name no item, or a client blames an innocent subscription.
		let cases: Vec<(SubscribeError<Error>, Option<usize>)> = vec![
			(SubscribeError::Empty, None),
			(
				SubscribeError::Create {
					index: 3,
					error: CreateSubscriptionError::Execute(error_with_code("X")),
				},
				Some(3),
			),
			(
				SubscribeError::RemoteConnect {
					index: 4,
					message: "down".to_string(),
				},
				Some(4),
			),
			(
				SubscribeError::InvalidRemoteId {
					index: 5,
				},
				Some(5),
			),
			(
				SubscribeError::LeaseFailed {
					code: "TXN_012",
					message: "evicted".to_string(),
				},
				None,
			),
			(SubscribeError::HydrationBackpressure, None),
			(
				SubscribeError::HydrationFailed {
					index: 6,
					error: HydrateError::SubscriptionNotFound,
					rql: "from a".to_string(),
					max_rows: 10,
				},
				Some(6),
			),
			(SubscribeError::HydrationServiceUnavailable("gone".to_string()), None),
		];
		for (error, expected) in cases {
			assert_eq!(error.index(), expected, "{error:?}");
		}
	}
}
