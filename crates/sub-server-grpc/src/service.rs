// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{
	actors::server::{
		Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage, method_not_enabled_reason,
	},
	interface::catalog::{
		binding::Binding, id::SubscriptionId, namespace::Namespace, procedure::Procedure,
		subscription::SubscribeOptions,
	},
	metrics::execution::ExecutionMetrics,
};
use reifydb_engine::subscription::HydrateError;
use reifydb_runtime::{actor::reply::reply_channel, sync::mutex::Mutex};
use reifydb_sub_core::{
	cleanup::cleanup_subscription_sync,
	errors::CreateSubscriptionError,
	handler::{SubscribeAck, SubscribeError, handle_subscribe as shared_subscribe},
};
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	binding::dispatch_binding,
	claim::{WireClaimRequest, native::dispatch_claim},
	dispatch::dispatch,
	execute::ExecuteError,
	interceptor::{Protocol, RequestContext, RequestMetadata},
};
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
use tracing::debug;

use crate::{
	convert::{proto_params_to_params, proto_subscribe_options},
	error::{GrpcError, diagnostic_status},
	generated::{
		AdminRequest, AdminResponse, AuthenticateRequest, AuthenticateResponse, CommandRequest,
		CommandResponse, LogoutRequest, LogoutResponse, OperationRequest, OperationResponse,
		Params as ProtoParams, QueryRequest, QueryResponse, QueueClaimRequest, QueueClaimResponse,
		SubscribeRequest, SubscriptionEvent, UnsubscribeRequest, UnsubscribeResponse, reify_db_server::ReifyDb,
	},
	server_state::GrpcServerState,
	subscription::{GrpcWireSink, SubscriptionRegistry, WireFormat},
};

type StreamSink = (
	mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
	mpsc::UnboundedReceiver<Result<SubscriptionEvent, Status>>,
	Uuid7,
	GrpcWireSink,
);

#[derive(Clone, Copy, PartialEq, Eq)]
struct StreamOwner {
	peer: SocketAddr,
	identity: IdentityId,
}

pub struct ReifyDbService {
	state: GrpcServerState,
	admin_enabled: bool,
	registry: Arc<SubscriptionRegistry>,
	owners: Arc<Mutex<HashMap<Uuid7, StreamOwner>>>,
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
			owners: Arc::new(Mutex::new(HashMap::new())),
			shutdown_rx,
		}
	}

	fn stream_owner<T>(request: &Request<T>, identity: IdentityId) -> Option<StreamOwner> {
		request.remote_addr().map(|peer| StreamOwner {
			peer,
			identity,
		})
	}

	fn owns(&self, caller: Option<StreamOwner>, connection_id: &Uuid7) -> bool {
		caller.is_some_and(|caller| self.owners.lock().get(connection_id) == Some(&caller))
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
	fn build_stream_sink(&self) -> StreamSink {
		let (tx, rx) = mpsc::unbounded_channel();
		let connection_id = Uuid7::generate(self.state.clock(), self.state.rng());
		let sink = GrpcWireSink::new(tx.clone());
		(tx, rx, connection_id, sink)
	}

	#[inline]
	fn spawn_stream_cleanup(
		&self,
		ack: SubscribeAck,
		tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
		rx: mpsc::UnboundedReceiver<Result<SubscriptionEvent, Status>>,
		connection_id: Uuid7,
		owner: Option<StreamOwner>,
	) -> Response<UnboundedReceiverStream<Result<SubscriptionEvent, Status>>> {
		let registry = self.registry.clone();
		let engine = self.state.engine_clone();
		let mut shutdown_rx = self.shutdown_rx.clone();
		let remote_handles = ack.remote_handles;
		let owners = self.owners.clone();
		if let Some(owner) = owner {
			owners.lock().insert(connection_id, owner);
		}
		spawn(async move {
			let client_disconnected = select! {
				_ = tx.closed() => true,
				_ = shutdown_rx.changed() => { drop(tx); false }
			};

			debug!(
				"gRPC subscription stream {} closed, cleaning up (client_disconnected={})",
				connection_id, client_disconnected
			);

			for (_, handle) in remote_handles {
				handle.abort();
			}
			let removed = registry.cleanup_connection(connection_id);
			owners.lock().remove(&connection_id);

			if client_disconnected {
				for subscription_id in removed {
					let engine = engine.clone();
					match spawn_blocking(move || {
						cleanup_subscription_sync(&engine, subscription_id)
					})
					.await
					{
						Ok(Ok(())) => {}
						Ok(Err(e)) => panic!("gRPC subscription stream cleanup failed: {e}"),
						Err(e) => panic!(
							"gRPC cleanup task for subscription {subscription_id} failed: {e}"
						),
					}
				}
			}
		});

		debug!(
			"gRPC subscription stream {} created ({} subscriptions)",
			connection_id,
			ack.subscriptions.len()
		);
		Response::new(UnboundedReceiverStream::new(rx))
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

fn encode_rbcf(frames: Vec<Frame>) -> Result<Vec<u8>, Status> {
	encode_frames(&frames, &EncodeOptions::fast())
		.map_err(|e| Status::internal(format!("failed to RBCF-encode response: {}", e)))
}

#[tonic::async_trait]
impl ReifyDb for ReifyDbService {
	async fn admin(&self, request: Request<AdminRequest>) -> Result<Response<AdminResponse>, Status> {
		if !self.admin_enabled {
			return Err(Status::not_found("not found"));
		}
		let ctx = self.admin_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_admin_response(encode_rbcf(frames)?, &metrics))
	}

	async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResponse>, Status> {
		let ctx = self.command_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_command_response(encode_rbcf(frames)?, &metrics))
	}

	async fn query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
		let ctx = self.query_context(request)?;
		let (frames, metrics) = dispatch(&self.state, ctx).await.map_err(GrpcError::from)?;
		Ok(Self::build_query_response(encode_rbcf(frames)?, &metrics))
	}

	type SubscribeStream = UnboundedReceiverStream<Result<SubscriptionEvent, Status>>;

	async fn subscribe(
		&self,
		request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let owner = Self::stream_owner(&request, identity);
		let inner = request.into_inner();
		let format = WireFormat::Rbcf;

		let (tx, rx, connection_id, sink) = self.build_stream_sink();

		let queries: Vec<(String, Params, SubscribeOptions)> = inner
			.subscriptions
			.into_iter()
			.enumerate()
			.map(|(index, subscription)| {
				let options = proto_subscribe_options(subscription.options).map_err(|e| {
					GrpcError::InvalidSubscribeOptions(format!("subscription {index}: {e}"))
				})?;
				Ok((subscription.rql, Params::None, options))
			})
			.collect::<Result<_, GrpcError>>()?;
		let host = self.state.subscribe_host(metadata);
		match shared_subscribe(
			&host,
			connection_id,
			identity,
			&queries,
			sink,
			&self.registry,
			format,
			self.shutdown_rx.clone(),
		)
		.await
		.unwrap_or_else(|e| panic!("gRPC subscribe failed: {e}"))
		{
			Ok(ack) => Ok(self.spawn_stream_cleanup(ack, tx, rx, connection_id, owner)),
			Err(err) => Err(subscribe_error_to_status(err)),
		}
	}

	async fn unsubscribe(
		&self,
		request: Request<UnsubscribeRequest>,
	) -> Result<Response<UnsubscribeResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let caller = Self::stream_owner(&request, identity);
		let inner = request.into_inner();
		let subscription_ids = inner
			.subscription_ids
			.iter()
			.map(|raw| raw.parse::<u64>().map(SubscriptionId))
			.collect::<Result<Vec<_>, _>>()
			.map_err(|_| Status::invalid_argument("Invalid subscription ID"))?;

		for subscription_id in subscription_ids {
			let removed = self
				.registry
				.connection_of(&subscription_id)
				.filter(|connection_id| self.owns(caller, connection_id))
				.map(|connection_id| self.registry.unsubscribe_owned(connection_id, &[subscription_id]))
				.unwrap_or_default();
			for subscription_id in removed {
				let engine = self.state.engine_clone();
				let result =
					spawn_blocking(move || cleanup_subscription_sync(&engine, subscription_id))
						.await;
				match result {
					Ok(Ok(())) => debug!("gRPC subscription {} unsubscribed", subscription_id),
					Ok(Err(e)) => panic!("gRPC unsubscribe failed: {e}"),
					Err(e) => {
						panic!(
							"gRPC unsubscribe task for subscription {subscription_id} failed: {e}"
						)
					}
				}
			}
		}

		Ok(Response::new(UnsubscribeResponse {
			subscription_ids: inner.subscription_ids,
		}))
	}

	async fn authenticate(
		&self,
		request: Request<AuthenticateRequest>,
	) -> Result<Response<AuthenticateResponse>, Status> {
		let caller = self.extract_identity(&request)?;
		let inner = request.into_inner();

		let (reply, receiver) = reply_channel();
		let (actor_ref, _handle) = self.state.spawn_actor();
		actor_ref
			.send(ServerMessage::Authenticate {
				caller,
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
			ServerAuthResponse::MethodNotEnabled {
				method,
			} => Ok(Response::new(AuthenticateResponse {
				status: "method_not_enabled".to_string(),
				token: String::new(),
				identity: String::new(),
				reason: method_not_enabled_reason(&method),
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
		Ok(Self::build_call_response(encode_rbcf(frames)?, &metrics))
	}

	async fn queue_claim(
		&self,
		request: Request<QueueClaimRequest>,
	) -> Result<Response<QueueClaimResponse>, Status> {
		let identity = self.extract_identity(&request)?;
		let metadata = Self::build_metadata(&request);
		let inner = request.into_inner();

		let claim = WireClaimRequest {
			queue: inner.queue,
			worker: inner.worker,
			max_n: inner.max_n,
			lease_ttl: inner.lease_ttl,
			wait_for: inner.wait_for,
		}
		.into_claim_request()
		.map_err(Status::invalid_argument)?;

		let (frames, metrics) =
			dispatch_claim(&self.state, identity, claim, metadata).await.map_err(GrpcError::from)?;

		let mut response = Response::new(QueueClaimResponse {
			rbcf: encode_rbcf(frames)?,
		});
		insert_meta_headers(response.metadata_mut(), &metrics);
		Ok(response)
	}
}

fn insert_meta_headers(metadata: &mut MetadataMap, metrics: &ExecutionMetrics) {
	metadata.insert("x-fingerprint", metrics.fingerprint.to_hex().parse().unwrap());
	metadata.insert("x-duration", metrics.total.to_string().parse().unwrap());
}

fn subscribe_error_to_status(err: SubscribeError<ExecuteError>) -> Status {
	match err {
		SubscribeError::Empty => Status::invalid_argument("Subscribe requires at least one query"),
		SubscribeError::Create {
			error: CreateSubscriptionError::Execute(e),
			..
		} => Status::from(GrpcError::from(e)),
		SubscribeError::RemoteConnect {
			message,
			..
		} => Status::unavailable(message),
		SubscribeError::InvalidRemoteId {
			..
		} => Status::internal("Invalid remote subscription ID format"),
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
			..
		} => hydrate_error_to_status(error, &rql, max_rows),
		SubscribeError::HydrationServiceUnavailable(msg) => {
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
