// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::{binding::Binding, id::SubscriptionId, namespace::Namespace, procedure::Procedure},
	metrics::execution::ExecutionMetrics,
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
			BatchAck, BatchSubscribeError, SubscribeAck, SubscribeError,
			handle_batch_subscribe as shared_batch_subscribe, handle_subscribe as shared_subscribe,
		},
	},
};
use reifydb_subscription::batch::BatchId;
use reifydb_value::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId, uuid::Uuid7},
};
use tokio::{
	select, spawn,
	sync::{mpsc, watch},
	task::spawn_blocking,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
	Code, Request, Response, Status,
	metadata::{KeyAndValueRef, MetadataMap},
};
use tracing::{debug, warn};

use crate::{
	convert::proto_params_to_params,
	error::{GrpcError, diagnostic_status},
	generated::{
		AdminRequest, AdminResponse, AuthenticateRequest, AuthenticateResponse, BatchSubscribeRequest,
		BatchSubscriptionEvent, BatchUnsubscribeRequest, BatchUnsubscribeResponse, CommandRequest,
		CommandResponse, LogoutRequest, LogoutResponse, OperationRequest, OperationResponse,
		Params as ProtoParams, QueryRequest, QueryResponse, SubscribeRequest, SubscriptionEvent,
		UnsubscribeRequest, UnsubscribeResponse, reify_db_server::ReifyDb,
	},
	server_state::GrpcServerState,
	subscription::{GrpcWireSink, SubscriptionRegistry, WireFormat},
};

type SingleSink = (
	mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
	mpsc::UnboundedReceiver<Result<SubscriptionEvent, Status>>,
	Uuid7,
	GrpcWireSink,
);

type BatchSink = (
	mpsc::UnboundedSender<Result<BatchSubscriptionEvent, Status>>,
	mpsc::UnboundedReceiver<Result<BatchSubscriptionEvent, Status>>,
	Uuid7,
	GrpcWireSink,
);

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

	#[inline]
	fn admin_context(&self, request: Request<AdminRequest>) -> Result<RequestContext, GrpcError> {
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
		Ok(ctx)
	}

	#[inline]
	fn build_admin_response(rbcf: Vec<u8>, metrics: &ExecutionMetrics) -> Response<AdminResponse> {
		let mut response = Response::new(AdminResponse {
			rbcf,
		});
		insert_meta_headers(response.metadata_mut(), metrics);
		response
	}

	#[inline]
	fn command_context(&self, request: Request<CommandRequest>) -> Result<RequestContext, GrpcError> {
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
		Ok(ctx)
	}

	#[inline]
	fn build_command_response(rbcf: Vec<u8>, metrics: &ExecutionMetrics) -> Response<CommandResponse> {
		let mut response = Response::new(CommandResponse {
			rbcf,
		});
		insert_meta_headers(response.metadata_mut(), metrics);
		response
	}

	#[inline]
	fn query_context(&self, request: Request<QueryRequest>) -> Result<RequestContext, GrpcError> {
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
		Ok(ctx)
	}

	#[inline]
	fn build_query_response(rbcf: Vec<u8>, metrics: &ExecutionMetrics) -> Response<QueryResponse> {
		let mut response = Response::new(QueryResponse {
			rbcf,
		});
		insert_meta_headers(response.metadata_mut(), metrics);
		response
	}

	#[inline]
	fn build_single_sink(&self) -> SingleSink {
		let (tx, rx) = mpsc::unbounded_channel();
		let connection_id = Uuid7::generate(self.state.clock(), self.state.rng());
		let sink = GrpcWireSink::Single(tx.clone());
		(tx, rx, connection_id, sink)
	}

	#[inline]
	fn spawn_single_cleanup(
		&self,
		ack: SubscribeAck,
		tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
		rx: mpsc::UnboundedReceiver<Result<SubscriptionEvent, Status>>,
		connection_id: Uuid7,
	) -> Response<UnboundedReceiverStream<Result<SubscriptionEvent, Status>>> {
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

		debug!("gRPC subscription created: {}", subscription_id);
		Response::new(UnboundedReceiverStream::new(rx))
	}

	#[inline]
	fn build_batch_sink(&self) -> BatchSink {
		let (batch_tx, batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let connection_id = Uuid7::generate(self.state.clock(), self.state.rng());
		let batch_sink = GrpcWireSink::Batch(batch_tx.clone());
		(batch_tx, batch_rx, connection_id, batch_sink)
	}

	#[inline]
	fn spawn_batch_cleanup(
		&self,
		ack: BatchAck,
		batch_tx: mpsc::UnboundedSender<Result<BatchSubscriptionEvent, Status>>,
		batch_rx: mpsc::UnboundedReceiver<Result<BatchSubscriptionEvent, Status>>,
		connection_id: Uuid7,
		format: WireFormat,
	) -> Response<UnboundedReceiverStream<Result<BatchSubscriptionEvent, Status>>> {
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

			debug!("gRPC batch {} stream closed (client_disconnected={})", batch_id, client_disconnected);

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

		debug!("gRPC batch {} created ({} members, format={:?})", batch_id, ack.members.len(), format);
		Response::new(UnboundedReceiverStream::new(batch_rx))
	}

	#[inline]
	fn resolve_binding(&self, name: &str) -> Result<Binding, Status> {
		self.state.engine().catalog().cache().find_grpc_binding_by_name(name).ok_or_else(|| {
			diagnostic_status(Code::NotFound, "NOT_FOUND", format!("no gRPC binding named `{}`", name))
		})
	}

	#[inline]
	fn resolve_procedure_namespace(&self, binding: &Binding) -> Result<(Procedure, Namespace), Status> {
		let procedure = self.state.engine().catalog().cache().find_procedure(binding.procedure_id).ok_or_else(
			|| {
				diagnostic_status(
					Code::Internal,
					"INTERNAL_ERROR",
					"binding references missing procedure".to_string(),
				)
			},
		)?;
		let namespace =
			self.state.engine().catalog().cache().find_namespace(binding.namespace).ok_or_else(|| {
				diagnostic_status(
					Code::Internal,
					"INTERNAL_ERROR",
					"binding references missing namespace".to_string(),
				)
			})?;
		Ok((procedure, namespace))
	}

	#[inline]
	fn validate_call_params(procedure: &Procedure, params: &Params) -> Result<(), Status> {
		match params {
			Params::None => {
				if let Some(p) = procedure.params().first() {
					return Err(diagnostic_status(
						Code::InvalidArgument,
						"INVALID_PARAMS",
						format!("missing required parameter `{}`", p.name),
					));
				}
			}
			Params::Named(map) => {
				for k in map.keys() {
					if !procedure.params().iter().any(|p| &p.name == k) {
						return Err(diagnostic_status(
							Code::InvalidArgument,
							"INVALID_PARAMS",
							format!("unknown parameter `{}`", k),
						));
					}
				}
				for p in procedure.params() {
					if !map.contains_key(&p.name) {
						return Err(diagnostic_status(
							Code::InvalidArgument,
							"INVALID_PARAMS",
							format!("missing required parameter `{}`", p.name),
						));
					}
				}
			}
			Params::Positional(_) => {
				return Err(diagnostic_status(
					Code::InvalidArgument,
					"INVALID_PARAMS",
					"Call requires named params".to_string(),
				));
			}
		}
		Ok(())
	}

	#[inline]
	fn build_call_response(rbcf: Vec<u8>, metrics: &ExecutionMetrics) -> Response<OperationResponse> {
		let mut response = Response::new(OperationResponse {
			rbcf,
		});
		insert_meta_headers(response.metadata_mut(), metrics);
		response
	}
}

fn encode_rbcf(frames: Vec<Frame>) -> Vec<u8> {
	encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default()
}

#[tonic::async_trait]
impl ReifyDb for ReifyDbService {
	async fn admin(&self, request: Request<AdminRequest>) -> Result<Response<AdminResponse>, Status> {
		if !self.admin_enabled {
			return Err(Status::not_found("not found"));
		}
		let ctx = self.admin_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_admin_response(encode_rbcf(frames), &metrics))
	}

	async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResponse>, Status> {
		let ctx = self.command_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_command_response(encode_rbcf(frames), &metrics))
	}

	async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
		let ctx = self.query_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_query_response(encode_rbcf(frames), &metrics))
	}

	type SubscribeStream = UnboundedReceiverStream<Result<SubscriptionEvent, Status>>;

	async fn subscribe(
		&self,
		request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();
		let format = WireFormat::Rbcf;

		let (tx, rx, connection_id, sink) = self.build_single_sink();

		match shared_subscribe(
			&self.state,
			connection_id,
			identity,
			inner.rql,
			Params::None,
			sink,
			&self.registry,
			format,
			self.shutdown_rx.clone(),
			metadata,
		)
		.await
		{
			Ok(ack) => Ok(self.spawn_single_cleanup(ack, tx, rx, connection_id)),
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
			Ok(Ok(())) => debug!("gRPC subscription {} unsubscribed", subscription_id),
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
		let format = WireFormat::Rbcf;

		let (batch_tx, batch_rx, connection_id, batch_sink) = self.build_batch_sink();

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
			Ok(ack) => Ok(self.spawn_batch_cleanup(ack, batch_tx, batch_rx, connection_id, format)),
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

		debug!("gRPC batch {} unsubscribed", batch_id);
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

		let binding = self.resolve_binding(&inner.name)?;
		let (procedure, namespace) = self.resolve_procedure_namespace(&binding)?;
		let params = Self::extract_params(inner.params)?;
		Self::validate_call_params(&procedure, &params)?;

		let (frames, metrics) =
			dispatch_binding(&self.state, namespace.name(), procedure.name(), params, identity, metadata)
				.await
				.map_err(GrpcError::from)?;
		Ok(Self::build_call_response(encode_rbcf(frames), &metrics))
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
