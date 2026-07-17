// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::{frame::decode::decode_frames, value::encode_value};
use reifydb_value::{
	error::{Diagnostic, Error},
	params::Params,
	value::{Value, frame::frame::Frame},
};
use serde_json::{Value as JsonValue, from_str as serde_json_from_str};
use tokio::time::{sleep, timeout};
use tonic::{
	Code, Request, Status,
	codec::Streaming,
	metadata::{Ascii, MetadataMap, MetadataValue},
	transport::Channel,
};

use super::generated::{
	AdminRequest as ProtoAdminRequest, AuthenticateRequest as ProtoAuthenticateRequest,
	BatchSubscribeRequest as ProtoBatchSubscribeRequest, BatchSubscriptionEvent,
	BatchUnsubscribeRequest as ProtoBatchUnsubscribeRequest, CommandRequest as ProtoCommandRequest,
	LogoutRequest as ProtoLogoutRequest, NamedParams, OperationRequest as ProtoOperationRequest,
	Params as ProtoParams, PositionalParams, QueryRequest as ProtoQueryRequest,
	SubscribeRequest as ProtoSubscribeRequest, SubscriptionEvent, TypedValue,
	UnsubscribeRequest as ProtoUnsubscribeRequest, batch_subscription_event, params::Params as ProtoParamsOneof,
	reify_db_client::ReifyDbClient, subscription_event,
};
use crate::{
	AdminResult, BatchChangeEntry, BatchChangePayload, BatchMemberClosedPayload, BatchMemberInfo, BatchPushEvent,
	ChangePayload, CommandResult, FrameChange, LoginResult, QueryResult, ReconnectOptions, ResponseMeta,
	WireFormat,
	changes::frames_to_changes,
	client::{BatchSubscription as ClientBatchSubscription, ReifyClient, Subscription as ClientSubscription},
	error::ClientError,
	reconnect::{backoff_millis, fire, millis_to_std},
	subscription::{BatchItem, SubscriptionConfig, build_subscription_rql},
};

fn extract_meta(metadata: &MetadataMap) -> Option<ResponseMeta> {
	let fingerprint = metadata.get("x-fingerprint").and_then(|v| v.to_str().ok())?;
	let duration = metadata.get("x-duration").and_then(|v| v.to_str().ok())?;
	Some(ResponseMeta {
		fingerprint: fingerprint.to_string(),
		duration: duration.to_string(),
	})
}

pub enum RawChangePayload {
	Rbcf(Vec<u8>),
	Empty,
}

impl RawChangePayload {
	pub fn into_frames(self) -> Vec<Frame> {
		match self {
			Self::Rbcf(bytes) => decode_frames(&bytes).unwrap_or_default(),
			Self::Empty => Vec::new(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct GrpcChange {
	pub changes: Vec<FrameChange>,
}

fn to_grpc_change(frames: Vec<Frame>) -> GrpcChange {
	GrpcChange {
		changes: frames_to_changes(frames),
	}
}

/// Options controlling a gRPC connection, including automatic reconnection.
#[derive(Clone)]
pub struct GrpcClientOptions {
	pub format: WireFormat,
	pub reconnect: ReconnectOptions,
}

impl GrpcClientOptions {
	/// Options for `format` with the default reconnection policy.
	pub fn new(format: WireFormat) -> Self {
		Self {
			format,
			reconnect: ReconnectOptions::default(),
		}
	}
}

#[derive(Clone)]
pub struct GrpcClient {
	inner: ReifyDbClient<Channel>,
	token: Option<String>,
	format: WireFormat,
	url: String,
	reconnect: ReconnectOptions,
	sub_id_counter: Arc<AtomicU64>,
}

impl GrpcClient {
	/// Connect with the default reconnection policy.
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		Self::connect_with_options(url, GrpcClientOptions::new(format)).await
	}

	/// Connect with explicit options (wire format + reconnection policy).
	pub async fn connect_with_options(url: &str, options: GrpcClientOptions) -> Result<Self, Error> {
		if options.format == WireFormat::Json {
			return Err(ClientError::UnsupportedWireFormat(
				"WireFormat::Json is not supported for GrpcClient".to_string(),
			)
			.into());
		}

		let channel = open_channel(url).await?;

		Ok(Self {
			inner: ReifyDbClient::new(channel),
			token: None,
			format: options.format,
			url: url.to_string(),
			reconnect: options.reconnect,
			sub_id_counter: Arc::new(AtomicU64::new(1)),
		})
	}

	pub fn authenticate(&mut self, token: &str) {
		self.token = Some(token.to_string());
	}

	/// Login with identifier and password. On success, stores the session token
	/// for subsequent requests and returns the login result.
	pub async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("identifier".to_string(), identifier.to_string());
		credentials.insert("password".to_string(), password.to_string());
		self.login("password", credentials).await
	}

	pub async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("token".to_string(), token.to_string());
		self.login("token", credentials).await
	}

	pub async fn login(
		&mut self,
		method: &str,
		credentials: HashMap<String, String>,
	) -> Result<LoginResult, Error> {
		let request = ProtoAuthenticateRequest {
			method: method.to_string(),
			credentials,
		};

		let mut client = self.inner.clone();
		let response = client.authenticate(Request::new(request)).await.map_err(status_to_error)?;
		let inner = response.into_inner();

		if inner.status == "authenticated" {
			self.token = Some(inner.token.clone());
			Ok(LoginResult {
				token: inner.token,
				identity: inner.identity,
			})
		} else {
			Err(ClientError::NotAuthenticated(inner.reason).into())
		}
	}

	/// Logout from the server, revoking the current session token.
	pub async fn logout(&mut self) -> Result<(), Error> {
		if self.token.is_none() {
			return Ok(());
		}

		let request = ProtoLogoutRequest {};
		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		client.logout(req).await.map_err(status_to_error)?;
		self.token = None;
		Ok(())
	}

	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.admin_with_meta(rql, params).await?.frames)
	}

	pub async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = ProtoAdminRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_proto),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let response = client.admin(req).await.map_err(status_to_error)?;
		let meta = extract_meta(response.metadata());
		let frames = decode_rbcf(&response.into_inner().rbcf)?;
		Ok(AdminResult {
			frames,
			meta,
		})
	}

	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.command_with_meta(rql, params).await?.frames)
	}

	pub async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = ProtoCommandRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_proto),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let response = client.command(req).await.map_err(status_to_error)?;
		let meta = extract_meta(response.metadata());
		let frames = decode_rbcf(&response.into_inner().rbcf)?;
		Ok(CommandResult {
			frames,
			meta,
		})
	}

	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.query_with_meta(rql, params).await?.frames)
	}

	pub async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = ProtoQueryRequest {
			rql: rql.to_string(),
			params: params.and_then(params_to_proto),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let response = client.query(req).await.map_err(status_to_error)?;
		let meta = extract_meta(response.metadata());
		let frames = decode_rbcf(&response.into_inner().rbcf)?;
		Ok(QueryResult {
			frames,
			meta,
		})
	}

	/// Invoke a gRPC binding by its globally-unique name.
	pub async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.call_with_meta(name, params).await?.frames)
	}

	pub async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = ProtoOperationRequest {
			name: name.to_string(),
			params: params.and_then(params_to_proto),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let response = client.call(req).await.map_err(status_to_error)?;
		let meta = extract_meta(response.metadata());
		let frames = decode_rbcf(&response.into_inner().rbcf)?;
		Ok(CommandResult {
			frames,
			meta,
		})
	}

	pub async fn subscribe(&self, rql: &str, config: SubscriptionConfig) -> Result<GrpcSubscription, Error> {
		let built = build_subscription_rql(rql, &config);
		let client_subscription_id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed).to_string();

		let mut client = self.inner.clone();
		let mut req = Request::new(ProtoSubscribeRequest {
			rql: built.clone(),
		});
		self.attach_auth(&mut req);

		let response = client.subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();
		consume_subscribed(&mut stream).await?;

		Ok(GrpcSubscription {
			client_subscription_id,
			stream,
			url: self.url.clone(),
			token: self.token.clone(),
			rql: built,
			reconnect: self.reconnect.clone(),
		})
	}

	pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		let request = ProtoUnsubscribeRequest {
			subscription_id: subscription_id.to_string(),
		};
		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);
		client.unsubscribe(req).await.map_err(status_to_error)?;
		Ok(())
	}

	/// Open a batch subscription over N queries. The server coalesces per-tick deltas
	/// into a single envelope keyed by member subscription id.
	pub async fn batch_subscribe(&self, items: &[BatchItem<'_>]) -> Result<BatchGrpcSubscription, Error> {
		let queries: Vec<String> = items.iter().map(|i| build_subscription_rql(i.rql, &i.config)).collect();
		let client_batch_id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed).to_string();

		let mut client = self.inner.clone();
		let mut req = Request::new(ProtoBatchSubscribeRequest {
			rql: queries.clone(),
		});
		self.attach_auth(&mut req);

		let response = client.batch_subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();
		let (_, members) = consume_batch_subscribed(&mut stream).await?;

		Ok(BatchGrpcSubscription {
			client_batch_id,
			members,
			stream,
			url: self.url.clone(),
			token: self.token.clone(),
			queries,
			reconnect: self.reconnect.clone(),
		})
	}

	pub async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		let request = ProtoBatchUnsubscribeRequest {
			batch_id: batch_id.to_string(),
		};
		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);
		client.batch_unsubscribe(req).await.map_err(status_to_error)?;
		Ok(())
	}

	fn attach_auth<T>(&self, request: &mut Request<T>) {
		attach_token(request, &self.token);
	}
}

async fn open_channel(url: &str) -> Result<Channel, Error> {
	let endpoint = Channel::from_shared(url.to_string())
		.map_err(|e| ClientError::Transport(format!("Invalid gRPC url: {}", e)))?;
	endpoint.tcp_nodelay(true)
		.connect()
		.await
		.map_err(|e| ClientError::Transport(format!("Failed to connect: {}", e)).into())
}

fn attach_token<T>(request: &mut Request<T>, token: &Option<String>) {
	if let Some(token) = token {
		let bearer = format!("Bearer {}", token);
		if let Ok(value) = bearer.parse::<MetadataValue<Ascii>>() {
			request.metadata_mut().insert("authorization", value);
		}
	}
}

async fn consume_subscribed(stream: &mut Streaming<SubscriptionEvent>) -> Result<String, Error> {
	let first = stream.message().await.map_err(status_to_error)?.ok_or_else(|| {
		ClientError::UnexpectedResponse("Stream closed before receiving subscription ID".to_string())
	})?;
	match first.event {
		Some(subscription_event::Event::Subscribed(s)) => Ok(s.subscription_id),
		_ => {
			Err(ClientError::UnexpectedResponse("Expected SubscribedEvent as first message".to_string())
				.into())
		}
	}
}

async fn consume_batch_subscribed(
	stream: &mut Streaming<BatchSubscriptionEvent>,
) -> Result<(String, Vec<BatchMemberHandle>), Error> {
	let first = stream.message().await.map_err(status_to_error)?.ok_or_else(|| {
		ClientError::UnexpectedResponse("Stream closed before receiving batch subscribed event".to_string())
	})?;
	match first.event {
		Some(batch_subscription_event::Event::Subscribed(s)) => {
			let members = s
				.members
				.into_iter()
				.map(|m| BatchMemberHandle {
					index: m.index as usize,
					subscription_id: m.subscription_id,
				})
				.collect();
			Ok((s.batch_id, members))
		}
		_ => Err(ClientError::UnexpectedResponse("Expected BatchSubscribedEvent as first message".to_string())
			.into()),
	}
}

pub struct GrpcSubscription {
	client_subscription_id: String,
	stream: Streaming<SubscriptionEvent>,
	url: String,
	token: Option<String>,
	rql: String,
	reconnect: ReconnectOptions,
}

/// Member information returned from a successful `batch_subscribe` - pairs the
/// client's query index with the server-assigned subscription id.
#[derive(Debug, Clone)]
pub struct BatchMemberHandle {
	pub index: usize,
	pub subscription_id: String,
}

/// A batch subscription over gRPC. Receives coalesced per-tick envelopes from
/// N underlying member subscriptions.
pub struct BatchGrpcSubscription {
	client_batch_id: String,
	members: Vec<BatchMemberHandle>,
	stream: Streaming<BatchSubscriptionEvent>,
	url: String,
	token: Option<String>,
	queries: Vec<String>,
	reconnect: ReconnectOptions,
}

/// One envelope delivered by a batch subscription: a map from member
/// `subscription_id` → typed change that arrived within that poller tick.
#[derive(Debug, Clone)]
pub struct BatchFramesEnvelope {
	pub batch_id: String,
	pub entries: HashMap<String, GrpcChange>,
	pub entry_errors: HashMap<String, String>,
}

/// A non-data server-initiated notification on a batch stream: either a member
/// closed (upstream ended, batch still alive) or the batch itself closed.
#[derive(Debug, Clone)]
pub enum BatchStreamEvent {
	Change(BatchFramesEnvelope),
	MemberClosed {
		batch_id: String,
		subscription_id: String,
	},
}

impl BatchGrpcSubscription {
	pub fn batch_id(&self) -> &str {
		&self.client_batch_id
	}

	pub fn members(&self) -> &[BatchMemberHandle] {
		&self.members
	}

	/// Receive the next envelope. When the server stream ends or errors, the batch is
	/// re-established (with re-auth and resubscribe) under the same stable client batch id;
	/// `None` is returned only once reconnection attempts are exhausted.
	///
	/// `BatchMemberClosed` notifications are surfaced so callers can track which
	/// members have stopped producing.
	pub async fn recv(&mut self) -> Option<BatchStreamEvent> {
		loop {
			match self.stream.message().await {
				Ok(Some(msg)) => {
					match msg.event {
						Some(batch_subscription_event::Event::Change(change)) => {
							let mut entries: HashMap<String, GrpcChange> = HashMap::new();
							let mut entry_errors: HashMap<String, String> = HashMap::new();
							for entry in change.entries {
								let sub_id = entry.subscription_id;
								match entry.change.map(|c| c.rbcf) {
									Some(bytes) if !bytes.is_empty() => {
										match decode_frames(&bytes) {
											Ok(frames) => {
												entries.insert(
													sub_id,
													to_grpc_change(
														frames,
													),
												);
											}
											Err(e) => {
												entry_errors.insert(
													sub_id.clone(),
													e.to_string(),
												);
												entries.insert(sub_id, to_grpc_change(Vec::new()));
											}
										}
									}
									_ => {
										entries.insert(
											sub_id,
											to_grpc_change(Vec::new()),
										);
									}
								}
							}
							return Some(BatchStreamEvent::Change(BatchFramesEnvelope {
								batch_id: self.client_batch_id.clone(),
								entries,
								entry_errors,
							}));
						}
						Some(batch_subscription_event::Event::MemberClosed(m)) => {
							return Some(BatchStreamEvent::MemberClosed {
								batch_id: self.client_batch_id.clone(),
								subscription_id: m.subscription_id,
							});
						}
						Some(batch_subscription_event::Event::Subscribed(_)) => continue,
						None => continue,
					}
				}
				Ok(None) | Err(_) => {
					fire(&self.reconnect.on_disconnect);
					if self.reconnect_stream().await {
						continue;
					}
					return None;
				}
			}
		}
	}

	async fn reconnect_stream(&mut self) -> bool {
		let mut attempt = 0u32;
		while attempt < self.reconnect.max_reconnect_attempts {
			attempt += 1;
			sleep(millis_to_std(backoff_millis(self.reconnect.reconnect_delay_ms, attempt))).await;

			let channel = match timeout(
				millis_to_std(self.reconnect.connect_timeout_ms),
				open_channel(&self.url),
			)
			.await
			{
				Ok(Ok(channel)) => channel,
				_ => continue,
			};
			let mut client = ReifyDbClient::new(channel);
			let mut req = Request::new(ProtoBatchSubscribeRequest {
				rql: self.queries.clone(),
			});
			attach_token(&mut req, &self.token);
			let response = match client.batch_subscribe(req).await {
				Ok(response) => response,
				Err(_) => continue,
			};
			let mut stream = response.into_inner();
			let (_, members) = match consume_batch_subscribed(&mut stream).await {
				Ok(result) => result,
				Err(_) => continue,
			};
			self.members = members;
			self.stream = stream;
			fire(&self.reconnect.on_reconnect);
			return true;
		}
		false
	}
}

impl GrpcSubscription {
	/// The stable client subscription id, preserved across reconnects.
	pub fn subscription_id(&self) -> &str {
		&self.client_subscription_id
	}

	/// Receive the next change. When the server stream ends or errors, the subscription is
	/// re-established (with re-auth and resubscribe) under the same stable client id; `None`
	/// is returned only once reconnection attempts are exhausted.
	pub async fn recv(&mut self) -> Option<GrpcChange> {
		loop {
			match self.stream.message().await {
				Ok(Some(msg)) => match msg.event {
					Some(subscription_event::Event::Change(change)) => {
						let frames = if change.rbcf.is_empty() {
							Vec::new()
						} else {
							decode_frames(&change.rbcf).unwrap_or_default()
						};
						return Some(to_grpc_change(frames));
					}
					Some(subscription_event::Event::Subscribed(_)) => continue,
					None => continue,
				},
				Ok(None) | Err(_) => {
					fire(&self.reconnect.on_disconnect);
					if self.reconnect_stream().await {
						continue;
					}
					return None;
				}
			}
		}
	}

	/// Receive the next raw change payload without any reconnection handling; returns `None`
	/// when the stream ends or errors.
	pub async fn recv_raw(&mut self) -> Option<RawChangePayload> {
		loop {
			let msg = self.stream.message().await.ok()??;
			match msg.event {
				Some(subscription_event::Event::Change(change)) => {
					let payload = if change.rbcf.is_empty() {
						RawChangePayload::Empty
					} else {
						RawChangePayload::Rbcf(change.rbcf)
					};
					return Some(payload);
				}
				Some(subscription_event::Event::Subscribed(_)) => {
					continue;
				}
				None => continue,
			}
		}
	}

	async fn reconnect_stream(&mut self) -> bool {
		let mut attempt = 0u32;
		while attempt < self.reconnect.max_reconnect_attempts {
			attempt += 1;
			sleep(millis_to_std(backoff_millis(self.reconnect.reconnect_delay_ms, attempt))).await;

			let channel = match timeout(
				millis_to_std(self.reconnect.connect_timeout_ms),
				open_channel(&self.url),
			)
			.await
			{
				Ok(Ok(channel)) => channel,
				_ => continue,
			};
			let mut client = ReifyDbClient::new(channel);
			let mut req = Request::new(ProtoSubscribeRequest {
				rql: self.rql.clone(),
			});
			attach_token(&mut req, &self.token);
			let response = match client.subscribe(req).await {
				Ok(response) => response,
				Err(_) => continue,
			};
			let mut stream = response.into_inner();
			if consume_subscribed(&mut stream).await.is_err() {
				continue;
			}
			self.stream = stream;
			fire(&self.reconnect.on_reconnect);
			return true;
		}
		false
	}
}

fn decode_rbcf(bytes: &[u8]) -> Result<Vec<Frame>, Error> {
	decode_frames(bytes).map_err(|e| ClientError::Decode(format!("failed to decode RBCF payload: {}", e)).into())
}

fn params_to_proto(params: Params) -> Option<ProtoParams> {
	match params {
		Params::None => None,
		Params::Positional(values) => Some(ProtoParams {
			params: Some(ProtoParamsOneof::Positional(PositionalParams {
				values: Arc::unwrap_or_clone(values).into_iter().map(value_to_typed_value).collect(),
			})),
		}),
		Params::Named(map) => Some(ProtoParams {
			params: Some(ProtoParamsOneof::Named(NamedParams {
				values: Arc::unwrap_or_clone(map)
					.into_iter()
					.map(|(k, v)| (k, value_to_typed_value(v)))
					.collect(),
			})),
		}),
	}
}

fn value_to_typed_value(value: Value) -> TypedValue {
	TypedValue {
		encoded: encode_value(&value).unwrap_or_default(),
	}
}

pub struct GrpcSubscriptionAdapter {
	inner: GrpcSubscription,
}

#[async_trait::async_trait]
impl ClientSubscription for GrpcSubscriptionAdapter {
	fn subscription_id(&self) -> &str {
		self.inner.subscription_id()
	}

	async fn recv(&mut self) -> Option<ChangePayload> {
		let change = self.inner.recv().await?;
		Some(ChangePayload {
			subscription_id: self.inner.subscription_id().to_string(),
			content_type: "application/vnd.reifydb.grpc".to_string(),
			body: JsonValue::Null,
			changes: change.changes,
		})
	}
}

pub struct BatchGrpcSubscriptionAdapter {
	inner: BatchGrpcSubscription,
	members_info: Vec<BatchMemberInfo>,
}

#[async_trait::async_trait]
impl ClientBatchSubscription for BatchGrpcSubscriptionAdapter {
	fn batch_id(&self) -> &str {
		self.inner.batch_id()
	}

	fn members(&self) -> &[BatchMemberInfo] {
		&self.members_info
	}

	async fn recv(&mut self) -> Option<BatchPushEvent> {
		let event = self.inner.recv().await?;
		Some(match event {
			BatchStreamEvent::Change(env) => {
				let batch_id = env.batch_id.clone();
				let entries = env
					.entries
					.into_iter()
					.map(|(sub_id, change)| BatchChangeEntry {
						subscription_id: sub_id,
						content_type: "application/vnd.reifydb.grpc".to_string(),
						body: JsonValue::Null,
						changes: change.changes,
						decode_error: None,
					})
					.collect();
				BatchPushEvent::Change(BatchChangePayload {
					batch_id,
					entries,
				})
			}
			BatchStreamEvent::MemberClosed {
				batch_id,
				subscription_id,
			} => BatchPushEvent::MemberClosed(BatchMemberClosedPayload {
				batch_id,
				subscription_id,
			}),
		})
	}
}

fn status_to_error(status: Status) -> Error {
	if let Ok(diag) = serde_json_from_str::<Diagnostic>(status.message()) {
		return Error(Box::new(diag));
	}
	if matches!(status.code(), Code::Unavailable | Code::Cancelled | Code::Unknown) {
		return ClientError::ConnectionLost.into();
	}
	ClientError::Transport(format!("gRPC {:?}: {}", status.code(), status.message())).into()
}

#[async_trait::async_trait]
impl ReifyClient for GrpcClient {
	fn wire_format(&self) -> WireFormat {
		self.format
	}

	fn is_authenticated(&self) -> bool {
		self.token.is_some()
	}

	async fn authenticate(&mut self, token: &str) -> Result<(), Error> {
		GrpcClient::authenticate(self, token);
		Ok(())
	}

	async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error> {
		GrpcClient::login_with_password(self, identifier, password).await
	}

	async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error> {
		GrpcClient::login_with_token(self, token).await
	}

	async fn logout(&mut self) -> Result<(), Error> {
		GrpcClient::logout(self).await
	}

	async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		GrpcClient::admin(self, rql, params).await
	}

	async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		GrpcClient::admin_with_meta(self, rql, params).await
	}

	async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		GrpcClient::command(self, rql, params).await
	}

	async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		GrpcClient::command_with_meta(self, rql, params).await
	}

	async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		GrpcClient::query(self, rql, params).await
	}

	async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		GrpcClient::query_with_meta(self, rql, params).await
	}

	async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		GrpcClient::call(self, name, params).await
	}

	async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		GrpcClient::call_with_meta(self, name, params).await
	}

	async fn subscribe(&self, rql: &str, config: SubscriptionConfig) -> Result<Box<dyn ClientSubscription>, Error> {
		let inner = GrpcClient::subscribe(self, rql, config).await?;
		Ok(Box::new(GrpcSubscriptionAdapter {
			inner,
		}))
	}

	async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		GrpcClient::unsubscribe(self, subscription_id).await
	}

	async fn batch_subscribe<'a>(
		&self,
		items: &[BatchItem<'a>],
	) -> Result<Box<dyn ClientBatchSubscription>, Error> {
		let inner = GrpcClient::batch_subscribe(self, items).await?;
		let members_info: Vec<BatchMemberInfo> = inner
			.members()
			.iter()
			.map(|m| BatchMemberInfo {
				index: m.index,
				subscription_id: m.subscription_id.clone(),
			})
			.collect();
		Ok(Box::new(BatchGrpcSubscriptionAdapter {
			inner,
			members_info,
		}))
	}

	async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		GrpcClient::batch_unsubscribe(self, batch_id).await
	}
}
