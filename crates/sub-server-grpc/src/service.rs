// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::{binding::BindingFormat, id::SubscriptionId},
	metric::ExecutionMetrics,
};
use reifydb_engine::subscription::HydrateError;
use reifydb_runtime::actor::reply::reply_channel;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	binding::dispatch_binding,
	dispatch::dispatch,
	interceptor::{Protocol, RequestContext, RequestMetadata},
	subscription::{
		cleanup::cleanup_subscription_sync,
		errors::CreateSubscriptionError,
		handler::{
			BatchSubscribeError, SubscribeError, handle_batch_subscribe as shared_batch_subscribe,
			handle_subscribe as shared_subscribe,
		},
	},
};
use reifydb_subscription::batch::BatchId;
use reifydb_value::{
	params::Params,
	value::{identity::IdentityId, uuid::Uuid7},
};
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
		AdminRequest, AdminResponse, AuthenticateRequest, AuthenticateResponse, BatchSubscribeRequest,
		BatchSubscriptionEvent, BatchUnsubscribeRequest, BatchUnsubscribeResponse, CommandRequest,
		CommandResponse, FramesPayload, LogoutRequest, LogoutResponse, OperationRequest, OperationResponse,
		Params as ProtoParams, QueryRequest, QueryResponse, SubscribeRequest, SubscriptionEvent,
		UnsubscribeRequest, UnsubscribeResponse, admin_response, command_response, operation_response,
		query_response, reify_db_server::ReifyDb,
	},
	server_state::GrpcServerState,
	subscription::{GrpcWireSink, SubscriptionRegistry, WireFormat},
};

pub struct ReifyDbService {
	state: GrpcServerState,
	admin_enabled: bool,
	registry: Arc<SubscriptionRegistry>,
	shutdown_rx: watch::Receiver<bool>,
}

impl ReifyDbService {
	pub fn new(
		state: GrpcServerState,
		admin_enabled: bool,
		registry: Arc<SubscriptionRegistry>,
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

		let (tx, rx) = mpsc::unbounded_channel();
		let connection_id = Uuid7::generate(self.state.clock(), self.state.rng());
		let sink = GrpcWireSink::Single(tx.clone());

		match shared_subscribe(
			&self.state,
			connection_id,
			identity,
			inner.rql,
			sink,
			&self.registry,
			format,
			self.shutdown_rx.clone(),
			metadata,
		)
		.await
		{
			Ok(ack) => {
				let subscription_id = ack.subscription_id;
				let registry = self.registry.clone();
				let engine = self.state.engine_clone();
				let mut shutdown_rx = self.shutdown_rx.clone();
				let remote_handle = ack.remote_handle;
				spawn(async move {
					let client_disconnected = select! {
						_ = tx.closed() => true,
						_ = shutdown_rx.changed() => { drop(tx); false }
					};

					debug!(
						"gRPC subscription {} stream closed, cleaning up (client_disconnected={})",
						subscription_id, client_disconnected
					);

					if let Some(handle) = remote_handle {
						handle.abort();
					}
					registry.cleanup_connection(connection_id);

					if client_disconnected {
						let engine_clone = engine.clone();
						let _ = spawn_blocking(move || {
							cleanup_subscription_sync(&engine_clone, subscription_id)
						})
						.await;
					}
				});

				info!("gRPC subscription created: {}", subscription_id);
				Ok(Response::new(UnboundedReceiverStream::new(rx)))
			}
			Err(err) => Err(subscribe_error_to_status(err)),
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

		self.registry.unsubscribe(subscription_id);

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
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let format = WireFormat::from_proto_i32(inner.format);

		let (batch_tx, batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let connection_id = Uuid7::generate(self.state.clock(), self.state.rng());
		let batch_sink = GrpcWireSink::Batch(batch_tx.clone());

		match shared_batch_subscribe(
			&self.state,
			connection_id,
			identity,
			&inner.rql,
			batch_sink,
			&self.registry,
			format,
			self.shutdown_rx.clone(),
			metadata,
		)
		.await
		{
			Ok(ack) => {
				let batch_id = ack.batch_id;
				let registry = self.registry.clone();
				let engine = self.state.engine_clone();
				let batch_tx_for_close = batch_tx.clone();
				let mut shutdown_rx = self.shutdown_rx.clone();
				let remote_handles = ack.remote_handles;
				spawn(async move {
					let client_disconnected = select! {
						_ = batch_tx_for_close.closed() => true,
						_ = shutdown_rx.changed() => false,
					};

					debug!(
						"gRPC batch {} stream closed (client_disconnected={})",
						batch_id, client_disconnected
					);

					for handle in remote_handles {
						handle.abort();
					}

					let removed_members = registry.cleanup_connection(connection_id);

					if client_disconnected && !removed_members.is_empty() {
						for member in removed_members {
							let engine_clone = engine.clone();
							let _ = spawn_blocking(move || {
								cleanup_subscription_sync(&engine_clone, member)
							})
							.await;
						}
					}
				});

				info!(
					"gRPC batch {} created ({} members, format={:?})",
					batch_id,
					ack.members.len(),
					format
				);
				Ok(Response::new(UnboundedReceiverStream::new(batch_rx)))
			}
			Err(err) => Err(batch_subscribe_error_to_status(err)),
		}
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
				match spawn_blocking(move || cleanup_subscription_sync(&engine, member)).await {
					Ok(Ok(())) => {}
					Ok(Err(e)) => warn!("Failed to cleanup batch member {}: {:?}", member, e),
					Err(e) => warn!("Cleanup task panicked for batch member {}: {:?}", member, e),
				}
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

		let binding = self
			.state
			.engine()
			.catalog()
			.cache()
			.find_grpc_binding_by_name(&inner.name)
			.ok_or_else(|| Status::not_found(format!("no gRPC binding named `{}`", inner.name)))?;

		let procedure = self
			.state
			.engine()
			.catalog()
			.cache()
			.find_procedure(binding.procedure_id)
			.ok_or_else(|| Status::internal("binding references missing procedure"))?;
		let namespace = self
			.state
			.engine()
			.catalog()
			.cache()
			.find_namespace(binding.namespace)
			.ok_or_else(|| Status::internal("binding references missing namespace"))?;

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

fn subscribe_error_to_status(err: SubscribeError) -> Status {
	match err {
		SubscribeError::Create(CreateSubscriptionError::Execute(e)) => Status::from(GrpcError::from(e)),
		SubscribeError::Create(CreateSubscriptionError::ExtractionFailed) => {
			Status::internal("Failed to extract subscription ID")
		}
		SubscribeError::RemoteConnect(msg) => Status::unavailable(msg),
		SubscribeError::InvalidRemoteId => Status::internal("Invalid remote subscription ID format"),
		SubscribeError::LeaseFailed {
			code,
			message,
		} if code == "TXN_012" || code == "HYDRATION_VERSION_EVICTED" => Status::out_of_range(message),
		SubscribeError::LeaseFailed {
			message,
			..
		} => Status::internal(message),
		SubscribeError::HydrationBackpressure => Status::resource_exhausted(
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		),
		SubscribeError::HydrationFailed {
			error,
			rql,
			max_rows,
		} => hydrate_error_to_status(error, &rql, max_rows),
		SubscribeError::HydrationServiceUnavailable(msg) => {
			Status::internal(format!("subscription service unavailable: {}", msg))
		}
	}
}

fn batch_subscribe_error_to_status(err: BatchSubscribeError) -> Status {
	match err {
		BatchSubscribeError::Empty => Status::invalid_argument("BatchSubscribe requires at least one query"),
		BatchSubscribeError::Create(CreateSubscriptionError::Execute(e)) => Status::from(GrpcError::from(e)),
		BatchSubscribeError::Create(CreateSubscriptionError::ExtractionFailed) => {
			Status::internal("Failed to extract subscription ID")
		}
		BatchSubscribeError::RemoteConnect(msg) => Status::unavailable(msg),
		BatchSubscribeError::InvalidRemoteId => Status::internal("Invalid remote subscription ID format"),
		BatchSubscribeError::LeaseFailed {
			code,
			message,
		} if code == "TXN_012" || code == "HYDRATION_VERSION_EVICTED" => Status::out_of_range(message),
		BatchSubscribeError::LeaseFailed {
			message,
			..
		} => Status::internal(message),
		BatchSubscribeError::HydrationBackpressure => Status::resource_exhausted(
			"Live diffs overflowed warming buffer during hydration; retry with smaller TAKE or lower hydration.max_rows",
		),
		BatchSubscribeError::HydrationFailed {
			error,
			rql,
			max_rows,
		} => hydrate_error_to_status(error, &rql, max_rows),
		BatchSubscribeError::HydrationServiceUnavailable(msg) => {
			Status::internal(format!("subscription service unavailable: {}", msg))
		}
	}
}

fn hydrate_error_to_status(err: HydrateError, rql: &str, cap: u64) -> Status {
	let msg = err.wire_message(rql, cap);
	let evicted = err.is_version_evicted();
	match err {
		HydrateError::SubscriptionNotFound => Status::not_found(msg),
		HydrateError::UnsupportedSourceType => Status::unimplemented(msg),
		HydrateError::RowCapExceeded {
			..
		} => Status::resource_exhausted(msg),
		HydrateError::Engine(_) if evicted => Status::out_of_range(msg),
		HydrateError::Engine(_) => Status::internal(msg),
		HydrateError::Internal(_) => Status::internal(msg),
	}
}
