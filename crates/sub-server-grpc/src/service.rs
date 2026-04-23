// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{RawChangePayload, WireFormat as ClientWireFormat};
use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::{binding::BindingFormat, id::SubscriptionId},
	metric::ExecutionMetrics,
};
use reifydb_remote_proxy::{RemoteSubscription, connect_remote, proxy_remote, proxy_remote_to_sink};
use reifydb_runtime::actor::reply::reply_channel;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	binding::dispatch_binding,
	dispatch::dispatch,
	interceptor::{Protocol, RequestContext, RequestMetadata},
	subscribe::{CreateSubscriptionResult, cleanup_subscription_sync, create_subscription},
};
use reifydb_subscription::batch::BatchId;
use reifydb_type::{params::Params, value::identity::IdentityId};
use reifydb_wire_format::{encode::encode_frames, options::EncodeOptions};
use tokio::{
	select, spawn,
	sync::{mpsc, watch},
	task::spawn_blocking,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
	Request, Response, Status,
	metadata::{KeyAndValueRef, MetadataMap},
};
use tracing::{debug, info, warn};

use crate::{
	convert::{frames_to_proto, proto_params_to_params},
	error::GrpcError,
	generated::{
		AdminRequest, AdminResponse, AuthenticateRequest, AuthenticateResponse, BatchMember,
		BatchSubscribeRequest, BatchSubscribedEvent, BatchSubscriptionEvent, BatchUnsubscribeRequest,
		BatchUnsubscribeResponse, ChangeEvent, CommandRequest, CommandResponse, FramesPayload, LogoutRequest,
		LogoutResponse, OperationRequest, OperationResponse, Params as ProtoParams, QueryRequest,
		QueryResponse, SubscribeRequest, SubscribedEvent, SubscriptionEvent, UnsubscribeRequest,
		UnsubscribeResponse, admin_response, batch_subscription_event, change_event, command_response,
		operation_response, query_response, reify_db_server::ReifyDb, subscription_event,
	},
	server_state::GrpcServerState,
	subscription::{GrpcSubscriptionRegistry, WireFormat},
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

	/// Build request metadata for internal per-query `create_subscription` calls inside
	/// `batch_subscribe`. The headers from the original gRPC request are already captured
	/// during `extract_identity`; downstream creators just need the protocol tag.
	fn build_metadata_placeholder() -> RequestMetadata {
		RequestMetadata::new(Protocol::Grpc)
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
		format: WireFormat,
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
		self.registry.register(subscription_id, tx.clone(), format);

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
		rql: &str,
		token: Option<String>,
		format: WireFormat,
	) -> Result<Response<UnboundedReceiverStream<Result<SubscriptionEvent, Status>>>, Status> {
		let client_format = match format {
			WireFormat::Rbcf => ClientWireFormat::Rbcf,
			WireFormat::Proto => ClientWireFormat::Proto,
		};
		let remote_sub = connect_remote(&address, rql, token.as_deref(), client_format)
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
		spawn(proxy_remote(remote_sub, tx, shutdown_rx, move |payload| {
			let payload = match (format, payload) {
				(WireFormat::Rbcf, RawChangePayload::Rbcf(bytes)) => change_event::Payload::Rbcf(bytes),
				(_, payload) => {
					let frames = payload.into_frames();
					match format {
						WireFormat::Rbcf => change_event::Payload::Rbcf(
							encode_frames(&frames, &EncodeOptions::fast())
								.unwrap_or_default(),
						),
						WireFormat::Proto => change_event::Payload::Frames(FramesPayload {
							frames: frames_to_proto(frames),
						}),
					}
				}
			};
			Ok(SubscriptionEvent {
				event: Some(subscription_event::Event::Change(ChangeEvent {
					payload: Some(payload),
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
			rql: inner.rql,
			params,
			metadata,
		};

		let format = WireFormat::from_proto_i32(inner.format);
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;

		let payload = match format {
			WireFormat::Rbcf => admin_response::Payload::Rbcf(
				encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default(),
			),
			WireFormat::Proto => admin_response::Payload::Frames(FramesPayload {
				frames: frames_to_proto(frames),
			}),
		};

		let mut response = Response::new(AdminResponse {
			payload: Some(payload),
		});
		insert_meta_headers(response.metadata_mut(), &metrics);
		Ok(response)
	}

	async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;
		let ctx = RequestContext {
			identity,
			operation: Operation::Command,
			rql: inner.rql,
			params,
			metadata,
		};

		let format = WireFormat::from_proto_i32(inner.format);
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;

		let payload = match format {
			WireFormat::Rbcf => command_response::Payload::Rbcf(
				encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default(),
			),
			WireFormat::Proto => command_response::Payload::Frames(FramesPayload {
				frames: frames_to_proto(frames),
			}),
		};

		let mut response = Response::new(CommandResponse {
			payload: Some(payload),
		});
		insert_meta_headers(response.metadata_mut(), &metrics);
		Ok(response)
	}

	async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let params = Self::extract_params(inner.params)?;
		let ctx = RequestContext {
			identity,
			operation: Operation::Query,
			rql: inner.rql,
			params,
			metadata,
		};

		let format = WireFormat::from_proto_i32(inner.format);
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;

		let payload = match format {
			WireFormat::Rbcf => query_response::Payload::Rbcf(
				encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default(),
			),
			WireFormat::Proto => query_response::Payload::Frames(FramesPayload {
				frames: frames_to_proto(frames),
			}),
		};

		let mut response = Response::new(QueryResponse {
			payload: Some(payload),
		});
		insert_meta_headers(response.metadata_mut(), &metrics);
		Ok(response)
	}

	type SubscribeStream = UnboundedReceiverStream<Result<SubscriptionEvent, Status>>;

	async fn subscribe(
		&self,
		request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let format = WireFormat::from_proto_i32(inner.format);

		// Subscribe still uses execute() via create_subscription for now
		match create_subscription(&self.state, identity, &inner.rql, metadata).await.map_err(GrpcError::from)? {
			CreateSubscriptionResult::Local(subscription_id) => {
				self.subscribe_local(subscription_id, format).await
			}
			CreateSubscriptionResult::Remote {
				address,
				rql,
				token: ns_token,
			} => self.subscribe_remote(address, &rql, ns_token, format).await,
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

	type BatchSubscribeStream = UnboundedReceiverStream<Result<BatchSubscriptionEvent, Status>>;

	async fn batch_subscribe(
		&self,
		request: Request<BatchSubscribeRequest>,
	) -> Result<Response<Self::BatchSubscribeStream>, Status> {
		let identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let format = WireFormat::from_proto_i32(inner.format);

		if inner.rql.is_empty() {
			return Err(Status::invalid_argument("BatchSubscribe requires at least one query"));
		}

		// Resolve every member first, tracking which are local vs remote so we can roll back on failure.
		let mut resolved: Vec<GrpcResolvedMember> = Vec::with_capacity(inner.rql.len());
		for (index, rql) in inner.rql.iter().enumerate() {
			let metadata = Self::build_metadata_placeholder();
			match create_subscription(&self.state, identity, rql, metadata).await {
				Ok(CreateSubscriptionResult::Local(subscription_id)) => {
					resolved.push(GrpcResolvedMember::Local {
						index,
						subscription_id,
					});
				}
				Ok(CreateSubscriptionResult::Remote {
					address,
					rql: upstream_rql,
					token: ns_token,
				}) => {
					let client_format = match format {
						WireFormat::Rbcf => ClientWireFormat::Rbcf,
						WireFormat::Proto => ClientWireFormat::Proto,
					};
					match connect_remote(
						&address,
						&upstream_rql,
						ns_token.as_deref(),
						client_format,
					)
					.await
					{
						Ok(remote_sub) => {
							let remote_id = remote_sub.subscription_id().to_string();
							match remote_id.parse::<u64>() {
								Ok(n) => resolved.push(GrpcResolvedMember::Remote {
									index,
									subscription_id: SubscriptionId(n),
									remote_sub: Box::new(remote_sub),
								}),
								Err(_) => {
									rollback_grpc_batch(&self.state, &resolved)
										.await;
									return Err(Status::internal(
										"Invalid remote subscription ID format",
									));
								}
							}
						}
						Err(e) => {
							rollback_grpc_batch(&self.state, &resolved).await;
							return Err(Status::unavailable(e.to_string()));
						}
					}
				}
				Err(e) => {
					rollback_grpc_batch(&self.state, &resolved).await;
					return Err(Status::from(GrpcError::from(e)));
				}
			}
		}

		let (batch_tx, batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();

		// Register local members + batch before shipping the initial event so that any
		// try_deliver arriving immediately is routed correctly.
		let member_ids: Vec<SubscriptionId> = resolved.iter().map(|m| m.subscription_id()).collect();

		for member in &resolved {
			if let GrpcResolvedMember::Local {
				subscription_id,
				..
			} = member
			{
				self.registry.register_batch_member(*subscription_id, format);
			}
		}

		let batch_id = self.registry.register_batch(
			member_ids.clone(),
			batch_tx.clone(),
			format,
			self.state.clock(),
			self.state.rng(),
		);

		// Emit initial BatchSubscribedEvent.
		let members_wire: Vec<BatchMember> = resolved
			.iter()
			.map(|m| BatchMember {
				index: m.index() as u32,
				subscription_id: m.subscription_id().to_string(),
			})
			.collect();
		let subscribed_event = BatchSubscriptionEvent {
			event: Some(batch_subscription_event::Event::Subscribed(BatchSubscribedEvent {
				batch_id: batch_id.to_string(),
				members: members_wire,
			})),
		};
		if batch_tx.send(Ok(subscribed_event)).is_err() {
			self.registry.unsubscribe_batch(batch_id);
			return Err(Status::internal("Failed to send subscribed event"));
		}

		// Spawn proxy tasks for remote members.
		let mut remote_handles = Vec::new();
		for member in resolved {
			if let GrpcResolvedMember::Remote {
				subscription_id,
				remote_sub,
				..
			} = member
			{
				let registry = self.registry.clone();
				let shutdown = self.shutdown_rx.clone();
				let handle = spawn(async move {
					let registry_push = registry.clone();
					proxy_remote_to_sink(*remote_sub, shutdown, move |payload| {
						let frames = payload.into_frames();
						registry_push.push_batch_frames(batch_id, subscription_id, frames)
					})
					.await;
					let _ = registry.emit_batch_member_closed(batch_id, subscription_id);
				});
				remote_handles.push(handle);
			}
		}

		info!("gRPC batch {} created ({} members, format={:?})", batch_id, member_ids.len(), format);

		// Cleanup task: on client disconnect OR server shutdown, unsubscribe the batch.
		let registry = self.registry.clone();
		let engine = self.state.engine_clone();
		let batch_tx_for_close = batch_tx.clone();
		let mut shutdown_rx = self.shutdown_rx.clone();
		spawn(async move {
			let client_disconnected = select! {
				_ = batch_tx_for_close.closed() => true,
				_ = shutdown_rx.changed() => false,
			};

			debug!("gRPC batch {} stream closed (client_disconnected={})", batch_id, client_disconnected);

			for handle in remote_handles {
				handle.abort();
			}

			let removed_members = registry.unsubscribe_batch(batch_id);

			if client_disconnected && let Some(members) = removed_members {
				for member in members {
					let engine_clone = engine.clone();
					let _ = spawn_blocking(move || {
						// cleanup_subscription_sync is a noop for remote-only IDs,
						// harmless.
						cleanup_subscription_sync(&engine_clone, member)
					})
					.await;
				}
			}
		});

		Ok(Response::new(UnboundedReceiverStream::new(batch_rx)))
	}

	async fn batch_unsubscribe(
		&self,
		request: Request<BatchUnsubscribeRequest>,
	) -> Result<Response<BatchUnsubscribeResponse>, Status> {
		let _identity = self.extract_identity(&request)?;
		let inner = request.into_inner();
		let batch_id: BatchId =
			inner.batch_id.parse().map_err(|_| Status::invalid_argument("Invalid batch ID"))?;

		if let Some(members) = self.registry.unsubscribe_batch(batch_id) {
			for member in members {
				let engine = self.state.engine_clone();
				let _ = spawn_blocking(move || cleanup_subscription_sync(&engine, member)).await;
			}
		}

		info!("gRPC batch {} unsubscribed", batch_id);
		Ok(Response::new(BatchUnsubscribeResponse {
			batch_id: inner.batch_id,
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
			.send(ServerMessage::Authenticate {
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
			.send(ServerMessage::Logout {
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

	async fn call(&self, request: Request<OperationRequest>) -> Result<Response<OperationResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();

		// Resolve the binding by gRPC rpc name via the protocol-specific index.
		let binding = self
			.state
			.engine()
			.materialized_catalog()
			.find_grpc_binding_by_name(&inner.name)
			.ok_or_else(|| Status::not_found(format!("no gRPC binding named `{}`", inner.name)))?;

		// Resolve procedure + namespace.
		let procedure = self
			.state
			.engine()
			.materialized_catalog()
			.find_procedure(binding.procedure_id)
			.ok_or_else(|| Status::internal("binding references missing procedure"))?;
		let namespace = self
			.state
			.engine()
			.materialized_catalog()
			.find_namespace(binding.namespace)
			.ok_or_else(|| Status::internal("binding references missing namespace"))?;

		// Decode typed params from the wire and validate against the procedure's declared set
		// (unknown keys, missing required) — without unpacking the Arc<HashMap>.
		let params = Self::extract_params(inner.params)?;
		match &params {
			Params::None => {
				if let Some(p) = procedure.params().first() {
					return Err(Status::invalid_argument(format!(
						"missing required parameter `{}`",
						p.name
					)));
				}
			}
			Params::Named(map) => {
				for k in map.keys() {
					if !procedure.params().iter().any(|p| &p.name == k) {
						return Err(Status::invalid_argument(format!(
							"unknown parameter `{}`",
							k
						)));
					}
				}
				for p in procedure.params() {
					if !map.contains_key(&p.name) {
						return Err(Status::invalid_argument(format!(
							"missing required parameter `{}`",
							p.name
						)));
					}
				}
			}
			Params::Positional(_) => {
				return Err(Status::invalid_argument("Call requires named params"));
			}
		}

		let (frames, metrics) =
			dispatch_binding(&self.state, namespace.name(), procedure.name(), params, identity, metadata)
				.await
				.map_err(GrpcError::from)?;

		// Encode per binding format. Rbcf → bytes; Frames → proto FramesPayload.
		// Json isn't representable over this gRPC schema — fall back to FramesPayload.
		let payload = match binding.format {
			BindingFormat::Rbcf => operation_response::Payload::Rbcf(
				encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default(),
			),
			_ => operation_response::Payload::Frames(FramesPayload {
				frames: frames_to_proto(frames),
			}),
		};

		let mut response = Response::new(OperationResponse {
			payload: Some(payload),
		});
		insert_meta_headers(response.metadata_mut(), &metrics);
		Ok(response)
	}
}

fn insert_meta_headers(metadata: &mut MetadataMap, metrics: &ExecutionMetrics) {
	metadata.insert("x-fingerprint", metrics.fingerprint.to_hex().parse().unwrap());
	metadata.insert("x-duration", metrics.total.to_string().parse().unwrap());
}

/// A resolved batch member inside `batch_subscribe`, classified by source.
enum GrpcResolvedMember {
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

impl GrpcResolvedMember {
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

/// Roll back any batch members already resolved before a subsequent failure.
async fn rollback_grpc_batch(state: &GrpcServerState, resolved: &[GrpcResolvedMember]) {
	for member in resolved {
		if let GrpcResolvedMember::Local {
			subscription_id,
			..
		} = member
		{
			let engine = state.engine_clone();
			let sub_id = *subscription_id;
			let _ = spawn_blocking(move || cleanup_subscription_sync(&engine, sub_id)).await;
		}
		// Remote members: dropping RemoteSubscription closes its gRPC stream → remote-side cleanup.
	}
}
