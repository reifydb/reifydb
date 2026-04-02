// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_auth::service::AuthResponse as EngineAuthResponse;
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	execute::execute,
	interceptor::{Operation, Protocol, RequestContext, RequestMetadata},
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
use tonic::{Request, Response, Status, metadata::KeyAndValueRef};
use tracing::{debug, info, warn};

use crate::{
	convert::{frames_to_proto, proto_params_to_params},
	error::GrpcError,
	generated::{
		AdminRequest, AdminResponse, AuthenticateRequest, AuthenticateResponse, ChangeEvent, CommandRequest,
		CommandResponse, LogoutRequest, LogoutResponse, Params as ProtoParams, QueryRequest, QueryResponse,
		SubscribeRequest, SubscribedEvent, SubscriptionEvent, UnsubscribeRequest, UnsubscribeResponse,
		reify_db_server::ReifyDb, subscription_event,
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
			return Ok(extract_identity_from_auth_header(self.state.auth_service(), header)?);
		}

		// No credentials provided — anonymous access
		Ok(IdentityId::anonymous())
	}

	fn build_metadata<T>(request: &Request<T>) -> RequestMetadata {
		let mut meta = RequestMetadata::new(Protocol::Grpc);
		for key_and_value in request.metadata().iter() {
			if let KeyAndValueRef::Ascii(key, value) = key_and_value
				&& let Ok(v) = value.to_str()
			{
				meta.insert(key.as_str(), v);
			}
		}
		meta
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
		token: Option<String>,
	) -> Result<Response<ReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let remote_sub = connect_remote(&address, query, token.as_deref())
			.await
			.map_err(|e| Status::unavailable(e.to_string()))?;

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
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let ctx = RequestContext {
			identity,
			operation: Operation::Admin,
			statements: inner.statements,
			params,
			metadata,
		};

		let (frames, _duration) = execute(
			self.state.request_interceptors(),
			self.state.actor_system(),
			self.state.engine_clone(),
			ctx,
			self.state.query_timeout(),
			self.state.clock(),
		)
		.await
		.map_err(GrpcError::from)?;

		Ok(Response::new(AdminResponse {
			frames: frames_to_proto(frames),
		}))
	}

	async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let ctx = RequestContext {
			identity,
			operation: Operation::Command,
			statements: inner.statements,
			params,
			metadata,
		};

		let (frames, _duration) = execute(
			self.state.request_interceptors(),
			self.state.actor_system(),
			self.state.engine_clone(),
			ctx,
			self.state.query_timeout(),
			self.state.clock(),
		)
		.await
		.map_err(GrpcError::from)?;

		Ok(Response::new(CommandResponse {
			frames: frames_to_proto(frames),
		}))
	}

	async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;

		let ctx = RequestContext {
			identity,
			operation: Operation::Query,
			statements: inner.statements,
			params,
			metadata,
		};

		let (frames, _duration) = execute(
			self.state.request_interceptors(),
			self.state.actor_system(),
			self.state.engine_clone(),
			ctx,
			self.state.query_timeout(),
			self.state.clock(),
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
		let token = request
			.metadata()
			.get("authorization")
			.and_then(|v| v.to_str().ok())
			.and_then(|h| h.strip_prefix("Bearer "))
			.map(|t| t.to_string());
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();

		match create_subscription(&self.state, identity, &inner.query, metadata)
			.await
			.map_err(GrpcError::from)?
		{
			CreateSubscriptionResult::Local(subscription_id) => self.subscribe_local(subscription_id).await,
			CreateSubscriptionResult::Remote {
				address,
				query,
			} => self.subscribe_remote(address, &query, token).await,
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

	async fn authenticate(
		&self,
		request: Request<AuthenticateRequest>,
	) -> Result<Response<AuthenticateResponse>, Status> {
		let inner = request.into_inner();
		match self.state.auth_service().authenticate(&inner.method, inner.credentials) {
			Ok(EngineAuthResponse::Authenticated {
				identity,
				token,
			}) => Ok(Response::new(AuthenticateResponse {
				status: "authenticated".to_string(),
				token,
				identity: identity.to_string(),
				reason: String::new(),
			})),
			Ok(EngineAuthResponse::Failed {
				reason,
			}) => Ok(Response::new(AuthenticateResponse {
				status: "failed".to_string(),
				token: String::new(),
				identity: String::new(),
				reason,
			})),
			Ok(EngineAuthResponse::Challenge {
				..
			}) => Err(Status::unimplemented("Challenge-response auth not supported over gRPC")),
			Err(e) => Err(Status::internal(e.to_string())),
		}
	}

	async fn logout(&self, request: Request<LogoutRequest>) -> Result<Response<LogoutResponse>, Status> {
		let token = request
			.metadata()
			.get("authorization")
			.and_then(|v| v.to_str().ok())
			.and_then(|h| h.strip_prefix("Bearer "))
			.map(|t| t.trim().to_string())
			.ok_or_else(|| Status::unauthenticated("Missing authorization token"))?;

		if token.is_empty() {
			return Err(Status::unauthenticated("Empty token"));
		}

		let revoked = self.state.auth_service().revoke_token(&token);

		if revoked {
			Ok(Response::new(LogoutResponse {
				status: "ok".to_string(),
			}))
		} else {
			Err(Status::unauthenticated("Invalid or expired token"))
		}
	}
}
