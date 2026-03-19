// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_api_key, extract_identity_from_auth_header},
	execute::{execute_admin, execute_command, execute_query},
	remote::{connect_remote, proxy_remote},
	state::AppState,
	subscribe::{CreateSubscriptionResult, cleanup_subscription_sync, create_subscription},
};
use reifydb_subscription::poller::SubscriptionPoller;
use reifydb_type::{params::Params, value::identity::IdentityId};
use tokio::{
	select, spawn,
	sync::{mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::{
	convert::{frames_to_proto, proto_params_to_params},
	error::GrpcError,
	generated::{
		AdminRequest, AdminResponse, ChangeEvent, CommandRequest, CommandResponse, Params as ProtoParams,
		QueryRequest, QueryResponse, SubscribeRequest, SubscribedEvent, SubscriptionEvent, UnsubscribeRequest,
		UnsubscribeResponse, reify_db_server::ReifyDb, subscription_event,
	},
	subscription::GrpcSubscriptionRegistry,
};

pub struct ReifyDbService {
	state: AppState,
	admin_enabled: bool,
	registry: Arc<GrpcSubscriptionRegistry>,
	poller: Arc<SubscriptionPoller>,
	shutdown_rx: watch::Receiver<bool>,
}

impl ReifyDbService {
	pub fn new(
		state: AppState,
		admin_enabled: bool,
		registry: Arc<GrpcSubscriptionRegistry>,
		poller: Arc<SubscriptionPoller>,
		shutdown_rx: watch::Receiver<bool>,
	) -> Self {
		Self {
			state,
			admin_enabled,
			registry,
			poller,
			shutdown_rx,
		}
	}

	fn extract_identity<T>(&self, request: &Request<T>) -> Result<IdentityId, GrpcError> {
		let metadata = request.metadata();

		if let Some(auth) = metadata.get("authorization") {
			let header = auth.to_str().map_err(|_| GrpcError::Unauthenticated(AuthError::InvalidHeader))?;
			return Ok(extract_identity_from_auth_header(header)?);
		}

		if let Some(api_key) = metadata.get("x-api-key") {
			let key = api_key.to_str().map_err(|_| GrpcError::Unauthenticated(AuthError::InvalidHeader))?;
			return Ok(extract_identity_from_api_key(key)?);
		}

		Err(GrpcError::Unauthenticated(AuthError::MissingCredentials))
	}

	fn extract_params(params: Option<ProtoParams>) -> Result<Params, GrpcError> {
		match params {
			None => Ok(Params::None),
			Some(p) => proto_params_to_params(p),
		}
	}

	async fn subscribe_local(
		&self,
		subscription_id: SubscriptionId,
	) -> Result<Response<ReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let (tx, rx) = mpsc::channel(256);

		// Send initial subscribed event
		let subscribed_event = SubscriptionEvent {
			event: Some(subscription_event::Event::Subscribed(SubscribedEvent {
				subscription_id: subscription_id.0.to_string(),
			})),
		};
		if tx.send(Ok(subscribed_event)).await.is_err() {
			return Err(Status::internal("Failed to send subscribed event"));
		}

		// Register with registry and poller
		self.registry.register(subscription_id, tx.clone());
		self.poller.register(subscription_id);

		info!("gRPC subscription created: {}", subscription_id);

		// Spawn cleanup task that monitors when the receiver is dropped or shutdown is signaled
		let registry = self.registry.clone();
		let poller = self.poller.clone();
		let engine = self.state.engine_clone();
		let system = self.state.actor_system();
		let mut shutdown_rx = self.shutdown_rx.clone();
		spawn(async move {
			let client_disconnected = select! {
				_ = tx.closed() => true,
				_ = shutdown_rx.changed() => { drop(tx); false }
			};

			debug!(
				"gRPC subscription {} stream closed, cleaning up (client_disconnected={})",
				subscription_id, client_disconnected
			);

			poller.unregister(&subscription_id);
			registry.unregister(&subscription_id);

			// Only run database cleanup on client disconnect, not server shutdown
			if client_disconnected {
				let engine_clone = engine.clone();
				let result = system
					.compute(move || cleanup_subscription_sync(&engine_clone, subscription_id))
					.await;
				match result {
					Ok(Ok(())) => debug!("Cleaned up gRPC subscription {}", subscription_id),
					Ok(Err(e)) => warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					),
					Err(e) => warn!(
						"Compute pool error cleaning up subscription {}: {:?}",
						subscription_id, e
					),
				}
			}
		});

		Ok(Response::new(ReceiverStream::new(rx)))
	}

	async fn subscribe_remote(
		&self,
		address: String,
		query: &str,
	) -> Result<Response<ReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let remote_sub =
			connect_remote(&address, query).await.map_err(|e| Status::unavailable(e.to_string()))?;

		let (tx, rx) = mpsc::channel(256);

		// Forward initial SubscribedEvent
		let subscribed_event = SubscriptionEvent {
			event: Some(subscription_event::Event::Subscribed(SubscribedEvent {
				subscription_id: remote_sub.subscription_id().to_string(),
			})),
		};
		tx.send(Ok(subscribed_event)).await.map_err(|_| Status::internal("channel closed"))?;

		// Spawn proxy: remote stream → local channel
		let shutdown_rx = self.shutdown_rx.clone();
		spawn(proxy_remote(remote_sub, tx, shutdown_rx, |frames| {
			Ok(SubscriptionEvent {
				event: Some(subscription_event::Event::Change(ChangeEvent {
					frames: frames_to_proto(frames),
				})),
			})
		}));

		Ok(Response::new(ReceiverStream::new(rx)))
	}
}

#[tonic::async_trait]
impl ReifyDb for ReifyDbService {
	async fn admin(&self, request: Request<AdminRequest>) -> Result<Response<AdminResponse>, Status> {
		if !self.admin_enabled {
			return Err(Status::not_found("not found"));
		}
		let identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let frames = execute_admin(
			self.state.actor_system(),
			self.state.engine_clone(),
			inner.statements,
			identity,
			params,
			self.state.query_timeout(),
		)
		.await
		.map_err(GrpcError::from)?;

		Ok(Response::new(AdminResponse {
			frames: frames_to_proto(frames),
		}))
	}

	async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let frames = execute_command(
			self.state.actor_system(),
			self.state.engine_clone(),
			inner.statements,
			identity,
			params,
			self.state.query_timeout(),
		)
		.await
		.map_err(GrpcError::from)?;

		Ok(Response::new(CommandResponse {
			frames: frames_to_proto(frames),
		}))
	}

	async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let statements = inner.statements.join("; ");
		let frames = execute_query(
			self.state.actor_system(),
			self.state.engine_clone(),
			statements,
			identity,
			params,
			self.state.query_timeout(),
		)
		.await
		.map_err(GrpcError::from)?;

		Ok(Response::new(QueryResponse {
			frames: frames_to_proto(frames),
		}))
	}

	type SubscribeStream = ReceiverStream<Result<SubscriptionEvent, Status>>;

	async fn subscribe(
		&self,
		request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let identity = self.extract_identity(&request)?;
		let inner = request.into_inner();

		match create_subscription(&self.state, identity, &inner.query).await.map_err(GrpcError::from)? {
			CreateSubscriptionResult::Local(subscription_id) => self.subscribe_local(subscription_id).await,
			CreateSubscriptionResult::Remote {
				address,
				query,
			} => self.subscribe_remote(address, &query).await,
		}
	}

	async fn unsubscribe(
		&self,
		request: Request<UnsubscribeRequest>,
	) -> Result<Response<UnsubscribeResponse>, Status> {
		let _identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let subscription_id = SubscriptionId(
			inner.subscription_id
				.parse::<u64>()
				.map_err(|_| Status::invalid_argument("Invalid subscription ID"))?,
		);

		// Unregister from poller
		self.poller.unregister(&subscription_id);

		// Unregister from registry
		self.registry.unregister(&subscription_id);

		// Cleanup the subscription from the database
		let engine = self.state.engine_clone();
		let system = self.state.actor_system();
		let result = system.compute(move || cleanup_subscription_sync(&engine, subscription_id)).await;
		match result {
			Ok(Ok(())) => info!("gRPC subscription {} unsubscribed", subscription_id),
			Ok(Err(e)) => {
				warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e)
			}
			Err(e) => warn!("Compute pool error cleaning up subscription {}: {:?}", subscription_id, e),
		}

		Ok(Response::new(UnsubscribeResponse {
			subscription_id: inner.subscription_id,
		}))
	}
}
