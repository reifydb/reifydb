// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::{
	actors::{
		grpc::GrpcMessage,
		server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse},
	},
	interface::catalog::id::SubscriptionId,
	metric::ExecutionMetrics,
};
use reifydb_runtime::actor::reply::reply_channel;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	execute::ExecuteError,
	interceptor::{Operation, Protocol, RequestContext, RequestMetadata, ResponseContext},
	remote::{connect_remote, proxy_remote},
	subscribe::{CreateSubscriptionResult, cleanup_subscription_sync, create_subscription},
};
use reifydb_type::{
	params::Params,
	value::{duration::Duration as ReifyDuration, frame::frame::Frame, identity::IdentityId},
};
use tokio::{
	select, spawn,
	sync::{mpsc, watch},
	task::spawn_blocking,
	time::timeout,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
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
	server_state::GrpcServerState,
	subscription::GrpcSubscriptionRegistry,
};

pub struct ReifyDbService {
	state: GrpcServerState,
	admin_enabled: bool,
	registry: Arc<GrpcSubscriptionRegistry>,
	shutdown_rx: watch::Receiver<bool>,
}

impl ReifyDbService {
	pub fn new(
		state: GrpcServerState,
		admin_enabled: bool,
		registry: Arc<GrpcSubscriptionRegistry>,
		shutdown_rx: watch::Receiver<bool>,
	) -> Self {
		Self {
			state,
			admin_enabled,
			registry,
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

	/// Dispatch a query/command/admin operation through the actor with interceptors.
	async fn execute_via_actor(&self, mut ctx: RequestContext) -> Result<(Vec<Frame>, Duration), GrpcError> {
		// Pre-interceptors
		if !self.state.request_interceptors().is_empty() {
			self.state.request_interceptors().pre_execute(&mut ctx).await.map_err(GrpcError::from)?;
		}

		let start = self.state.clock().instant();

		// Build message and send to per-request actor
		let (reply, receiver) = reply_channel();
		let msg = match ctx.operation {
			Operation::Query => GrpcMessage::Query {
				identity: ctx.identity,
				statements: ctx.statements.clone(),
				params: ctx.params.clone(),
				reply,
			},
			Operation::Command => GrpcMessage::Command {
				identity: ctx.identity,
				statements: ctx.statements.clone(),
				params: ctx.params.clone(),
				reply,
			},
			Operation::Admin => GrpcMessage::Admin {
				identity: ctx.identity,
				statements: ctx.statements.clone(),
				params: ctx.params.clone(),
				reply,
			},
			Operation::Subscribe => unreachable!("subscribe uses a different path"),
		};

		let (actor_ref, _handle) = self.state.spawn_actor();
		actor_ref.send(msg).ok().ok_or_else(|| GrpcError::from(ExecuteError::Disconnected))?;

		// Await reply with timeout
		let server_response = timeout(self.state.query_timeout(), receiver.recv())
			.await
			.map_err(|_| GrpcError::from(ExecuteError::Timeout))?
			.map_err(|_| GrpcError::from(ExecuteError::Disconnected))?;

		let wall_duration = start.elapsed();

		// Convert response
		let (frames, compute_duration) = match server_response {
			ServerResponse::Success {
				frames,
				duration,
			} => (frames, duration),
			ServerResponse::EngineError {
				diagnostic,
				statement,
			} => {
				return Err(GrpcError::from(ExecuteError::Engine {
					diagnostic: Arc::from(diagnostic),
					statement,
				}));
			}
		};

		// Post-interceptors
		if !self.state.request_interceptors().is_empty() {
			let response_ctx = ResponseContext {
				identity: ctx.identity,
				operation: ctx.operation,
				statements: ctx.statements,
				params: ctx.params,
				metadata: ctx.metadata,
				metrics: ExecutionMetrics::default(),
				result: Ok(frames.len()),
				total: ReifyDuration::from_nanoseconds(wall_duration.as_nanos() as i64)
					.unwrap_or_default(),
				compute: ReifyDuration::from_nanoseconds(compute_duration.as_nanos() as i64)
					.unwrap_or_default(),
			};
			self.state.request_interceptors().post_execute(&response_ctx).await;
		}

		Ok((frames, wall_duration))
	}

	async fn subscribe_local(
		&self,
		subscription_id: SubscriptionId,
	) -> Result<Response<UnboundedReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let (tx, rx) = mpsc::unbounded_channel();

		// Send initial subscribed event
		let subscribed_event = SubscriptionEvent {
			event: Some(subscription_event::Event::Subscribed(SubscribedEvent {
				subscription_id: subscription_id.0.to_string(),
			})),
		};
		if tx.send(Ok(subscribed_event)).is_err() {
			return Err(Status::internal("Failed to send subscribed event"));
		}

		// Register with registry
		self.registry.register(subscription_id, tx.clone());

		info!("gRPC subscription created: {}", subscription_id);

		// Spawn cleanup task that monitors when the receiver is dropped or shutdown is signaled
		let registry = self.registry.clone();
		let engine = self.state.engine_clone();
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

			registry.unregister(&subscription_id);

			// Only run database cleanup on client disconnect, not server shutdown
			if client_disconnected {
				let engine_clone = engine.clone();
				let result = spawn_blocking(move || {
					cleanup_subscription_sync(&engine_clone, subscription_id)
				})
				.await;
				match result {
					Ok(Ok(())) => debug!("Cleaned up gRPC subscription {}", subscription_id),
					Ok(Err(e)) => warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					),
					Err(e) => warn!(
						"Blocking task error cleaning up subscription {}: {:?}",
						subscription_id, e
					),
				}
			}
		});

		Ok(Response::new(UnboundedReceiverStream::new(rx)))
	}

	async fn subscribe_remote(
		&self,
		address: String,
		query: &str,
		token: Option<String>,
	) -> Result<Response<UnboundedReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let remote_sub = connect_remote(&address, query, token.as_deref())
			.await
			.map_err(|e| Status::unavailable(e.to_string()))?;

		let (tx, rx) = mpsc::unbounded_channel();

		// Forward initial SubscribedEvent
		let subscribed_event = SubscriptionEvent {
			event: Some(subscription_event::Event::Subscribed(SubscribedEvent {
				subscription_id: remote_sub.subscription_id().to_string(),
			})),
		};
		tx.send(Ok(subscribed_event)).map_err(|_| Status::internal("channel closed"))?;

		// Spawn proxy: remote stream → local channel
		let shutdown_rx = self.shutdown_rx.clone();
		spawn(proxy_remote(remote_sub, tx, shutdown_rx, |frames| {
			Ok(SubscriptionEvent {
				event: Some(subscription_event::Event::Change(ChangeEvent {
					frames: frames_to_proto(frames),
				})),
			})
		}));

		Ok(Response::new(UnboundedReceiverStream::new(rx)))
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

		let (frames, _duration) = self.execute_via_actor(ctx).await?;

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

		let (frames, _duration) = self.execute_via_actor(ctx).await?;

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

		let (frames, _duration) = self.execute_via_actor(ctx).await?;

		Ok(Response::new(QueryResponse {
			frames: frames_to_proto(frames),
		}))
	}

	type SubscribeStream = UnboundedReceiverStream<Result<SubscriptionEvent, Status>>;

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

		// Subscribe still uses execute() via create_subscription for now
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

		// Unregister from registry
		self.registry.unregister(&subscription_id);

		// Cleanup the subscription from the database
		let engine = self.state.engine_clone();
		let result = spawn_blocking(move || cleanup_subscription_sync(&engine, subscription_id)).await;
		match result {
			Ok(Ok(())) => info!("gRPC subscription {} unsubscribed", subscription_id),
			Ok(Err(e)) => {
				warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e)
			}
			Err(e) => warn!("Blocking task error cleaning up subscription {}: {:?}", subscription_id, e),
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

		let (reply, receiver) = reply_channel();
		let (actor_ref, _handle) = self.state.spawn_actor();
		actor_ref
			.send(GrpcMessage::Authenticate {
				method: inner.method,
				credentials: inner.credentials,
				reply,
			})
			.ok()
			.ok_or_else(|| Status::internal("actor mailbox closed"))?;

		let auth_response = receiver.recv().await.map_err(|_| Status::internal("actor stopped"))?;

		match auth_response {
			ServerAuthResponse::Authenticated {
				identity,
				token,
			} => Ok(Response::new(AuthenticateResponse {
				status: "authenticated".to_string(),
				token,
				identity: identity.to_string(),
				reason: String::new(),
			})),
			ServerAuthResponse::Failed {
				reason,
			} => Ok(Response::new(AuthenticateResponse {
				status: "failed".to_string(),
				token: String::new(),
				identity: String::new(),
				reason,
			})),
			ServerAuthResponse::Challenge {
				..
			} => Err(Status::unimplemented("Challenge-response auth not supported over gRPC")),
			ServerAuthResponse::Error(reason) => Err(Status::internal(reason)),
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

		let (reply, receiver) = reply_channel();
		let (actor_ref, _handle) = self.state.spawn_actor();
		actor_ref
			.send(GrpcMessage::Logout {
				token,
				reply,
			})
			.ok()
			.ok_or_else(|| Status::internal("actor mailbox closed"))?;

		let logout_response = receiver.recv().await.map_err(|_| Status::internal("actor stopped"))?;

		match logout_response {
			ServerLogoutResponse::Ok => Ok(Response::new(LogoutResponse {
				status: "ok".to_string(),
			})),
			ServerLogoutResponse::InvalidToken => Err(Status::unauthenticated("Invalid or expired token")),
			ServerLogoutResponse::Error(reason) => Err(Status::internal(reason)),
		}
	}
}
