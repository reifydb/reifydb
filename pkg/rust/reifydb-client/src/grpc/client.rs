// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

pub use reifydb_codec::wire::RawChangePayload;
use reifydb_codec::{frame::decode::decode_frames, value::encode_value};
use reifydb_value::{
	error::{Diagnostic, Error},
	params::Params,
	value::{Value, frame::frame::Frame},
};
use serde_json::{Value as JsonValue, from_str as serde_json_from_str};
use tokio::{
	sync::Mutex,
	time::{sleep, timeout},
};
use tonic::{
	Code, Request, Status,
	codec::Streaming,
	metadata::{Ascii, MetadataMap, MetadataValue},
	transport::Channel,
};

use super::generated::{
	AdminRequest as ProtoAdminRequest, AuthenticateRequest as ProtoAuthenticateRequest,
	BatchSubscribeItem as ProtoBatchSubscribeItem, BatchSubscribeRequest as ProtoBatchSubscribeRequest,
	BatchSubscriptionEvent, BatchUnsubscribeRequest as ProtoBatchUnsubscribeRequest,
	CommandRequest as ProtoCommandRequest, HydrationOptions as ProtoHydrationOptions,
	LogoutRequest as ProtoLogoutRequest, NamedParams, OperationRequest as ProtoOperationRequest,
	Params as ProtoParams, PositionalParams, QueryRequest as ProtoQueryRequest,
	QueueClaimRequest as ProtoQueueClaimRequest, SubscribeOptions as ProtoSubscribeOptions,
	SubscribeRequest as ProtoSubscribeRequest, SubscriptionEvent, TypedValue,
	UnsubscribeRequest as ProtoUnsubscribeRequest, batch_subscription_event, params::Params as ProtoParamsOneof,
	reify_db_client::ReifyDbClient, subscription_event,
};
use crate::{
	AdminResult, BatchChangeEntry, BatchChangePayload, BatchPushEvent, BatchSubscriptionClosedPayload,
	BatchSubscriptionInfo, ChangePayload, CommandResult, FrameChange, LoginResult, QueryResult, QueueClaimRequest,
	ReconnectOptions, ResponseMeta, WireFormat,
	changes::frames_to_changes,
	client::{BatchSubscription as ClientBatchSubscription, ReifyClient, Subscription as ClientSubscription},
	error::ClientError,
	reconnect::{backoff_millis, fire, millis_to_std},
	subscription::{BatchSubscribeItem, SubscriptionConfig},
};

fn extract_meta(metadata: &MetadataMap) -> Option<ResponseMeta> {
	let fingerprint = metadata.get("x-fingerprint").and_then(|v| v.to_str().ok())?;
	let duration = metadata.get("x-duration").and_then(|v| v.to_str().ok())?;
	Some(ResponseMeta {
		fingerprint: fingerprint.to_string(),
		duration: duration.to_string(),
	})
}

#[derive(Debug, Clone)]
pub struct GrpcChange {
	pub changes: Vec<FrameChange>,
	pub decode_error: Option<String>,
}

fn to_grpc_change(frames: Vec<Frame>) -> GrpcChange {
	GrpcChange {
		changes: frames_to_changes(frames),
		decode_error: None,
	}
}

fn change_from_rbcf(bytes: &[u8]) -> GrpcChange {
	match decode_frames(bytes) {
		Ok(frames) => to_grpc_change(frames),
		Err(e) => GrpcChange {
			decode_error: Some(e.to_string()),
			..to_grpc_change(Vec::new())
		},
	}
}

#[derive(Clone)]
pub struct GrpcClientOptions {
	pub format: WireFormat,
	pub reconnect: ReconnectOptions,
}

impl GrpcClientOptions {
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
	batch_server_ids: Arc<Mutex<HashMap<String, String>>>,
}

impl GrpcClient {
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		Self::connect_with_options(url, GrpcClientOptions::new(format)).await
	}

	pub async fn connect_with_options(url: &str, options: GrpcClientOptions) -> Result<Self, Error> {
		if options.format == WireFormat::Frames {
			return Err(ClientError::UnsupportedWireFormat(
				"WireFormat::Frames is not supported for GrpcClient".to_string(),
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
			batch_server_ids: Arc::new(Mutex::new(HashMap::new())),
		})
	}

	pub fn authenticate(&mut self, token: &str) {
		self.token = Some(token.to_string());
	}

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
			params: params_to_proto(params.unwrap_or(Params::None))?,
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
			params: params_to_proto(params.unwrap_or(Params::None))?,
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
			params: params_to_proto(params.unwrap_or(Params::None))?,
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

	pub async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.call_with_meta(name, params).await?.frames)
	}

	pub async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = ProtoOperationRequest {
			name: name.to_string(),
			params: params_to_proto(params.unwrap_or(Params::None))?,
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

	pub async fn queue_claim(&self, request: QueueClaimRequest) -> Result<Vec<Frame>, Error> {
		let mut client = self.inner.clone();
		let mut req = Request::new(ProtoQueueClaimRequest {
			queue: request.queue,
			worker: request.worker,
			max_n: request.max_n,
			lease_ttl: request.lease_ttl,
			wait_for: request.wait_for,
		});
		self.attach_auth(&mut req);

		let response = client.queue_claim(req).await.map_err(status_to_error)?;
		decode_rbcf(&response.into_inner().rbcf)
	}

	pub async fn subscribe(&self, rql: &str, config: SubscriptionConfig) -> Result<GrpcSubscription, Error> {
		let request = ProtoSubscribeRequest {
			rql: rql.to_string(),
			options: Some(subscription_options_to_proto(&config)?),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request.clone());
		self.attach_auth(&mut req);

		let response = client.subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();
		let subscription_id = consume_subscribed(&mut stream).await?;

		Ok(GrpcSubscription {
			subscription_id,
			stream,
			url: self.url.clone(),
			token: self.token.clone(),
			request,
			reconnect: self.reconnect.clone(),
			attempt: 0,
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

	pub async fn batch_subscribe(&self, items: &[BatchSubscribeItem<'_>]) -> Result<BatchGrpcSubscription, Error> {
		let request = ProtoBatchSubscribeRequest {
			subscriptions: items
				.iter()
				.map(|i| {
					Ok(ProtoBatchSubscribeItem {
						rql: i.rql.to_string(),
						options: Some(subscription_options_to_proto(&i.config)?),
					})
				})
				.collect::<Result<_, Error>>()?,
		};
		let client_batch_id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed).to_string();

		let mut client = self.inner.clone();
		let mut req = Request::new(request.clone());
		self.attach_auth(&mut req);

		let response = client.batch_subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();
		let (server_batch_id, acked) = consume_batch_subscribed(&mut stream).await?;
		self.batch_server_ids.lock().await.insert(client_batch_id.clone(), server_batch_id);

		Ok(BatchGrpcSubscription {
			client_batch_id,
			acked,
			stream,
			batch_server_ids: self.batch_server_ids.clone(),
			url: self.url.clone(),
			token: self.token.clone(),
			request,
			reconnect: self.reconnect.clone(),
			attempt: 0,
		})
	}

	pub async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		let server_batch_id = self.batch_server_ids.lock().await.remove(batch_id);
		let request = ProtoBatchUnsubscribeRequest {
			batch_id: server_batch_id.unwrap_or_else(|| batch_id.to_string()),
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
) -> Result<(String, Vec<BatchSubscriptionHandle>), Error> {
	let first = stream.message().await.map_err(status_to_error)?.ok_or_else(|| {
		ClientError::UnexpectedResponse("Stream closed before receiving batch subscribed event".to_string())
	})?;
	match first.event {
		Some(batch_subscription_event::Event::Subscribed(s)) => {
			let subscriptions = s
				.subscriptions
				.into_iter()
				.map(|m| BatchSubscriptionHandle {
					index: m.index as usize,
					subscription_id: m.subscription_id,
				})
				.collect();
			Ok((s.batch_id, subscriptions))
		}
		_ => Err(ClientError::UnexpectedResponse("Expected BatchSubscribedEvent as first message".to_string())
			.into()),
	}
}

pub struct GrpcSubscription {
	subscription_id: String,
	stream: Streaming<SubscriptionEvent>,
	url: String,
	token: Option<String>,
	request: ProtoSubscribeRequest,
	reconnect: ReconnectOptions,
	attempt: u32,
}

#[derive(Debug, Clone)]
pub struct BatchSubscriptionHandle {
	pub index: usize,
	pub subscription_id: String,
}

pub struct BatchGrpcSubscription {
	client_batch_id: String,
	acked: Vec<BatchSubscriptionHandle>,
	stream: Streaming<BatchSubscriptionEvent>,
	batch_server_ids: Arc<Mutex<HashMap<String, String>>>,
	url: String,
	token: Option<String>,
	request: ProtoBatchSubscribeRequest,
	reconnect: ReconnectOptions,
	attempt: u32,
}

#[derive(Debug, Clone)]
pub struct BatchFramesEnvelope {
	pub batch_id: String,
	pub entries: HashMap<String, GrpcChange>,
}

#[derive(Debug, Clone)]
pub enum BatchStreamEvent {
	Change(BatchFramesEnvelope),
	SubscriptionClosed {
		batch_id: String,
		subscription_id: String,
	},
}

impl BatchGrpcSubscription {
	pub fn batch_id(&self) -> &str {
		&self.client_batch_id
	}

	pub fn subscriptions(&self) -> &[BatchSubscriptionHandle] {
		&self.acked
	}

	pub async fn recv(&mut self) -> Option<BatchStreamEvent> {
		loop {
			match self.stream.message().await {
				Ok(Some(msg)) => {
					self.attempt = 0;
					match msg.event {
						Some(batch_subscription_event::Event::Change(change)) => {
							let mut entries: HashMap<String, GrpcChange> = HashMap::new();
							for entry in change.entries {
								let sub_id = entry.subscription_id;
								match entry.change.map(|c| c.rbcf) {
									Some(bytes) if !bytes.is_empty() => {
										entries.insert(
											sub_id,
											change_from_rbcf(&bytes),
										);
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
							}));
						}
						Some(batch_subscription_event::Event::SubscriptionClosed(m)) => {
							return Some(BatchStreamEvent::SubscriptionClosed {
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

	async fn open_stream(
		&self,
	) -> Option<(Streaming<BatchSubscriptionEvent>, String, Vec<BatchSubscriptionHandle>)> {
		let channel = open_channel(&self.url).await.ok()?;
		let mut client = ReifyDbClient::new(channel);
		let mut req = Request::new(self.request.clone());
		attach_token(&mut req, &self.token);
		let mut stream = client.batch_subscribe(req).await.ok()?.into_inner();
		let (server_batch_id, acked) = consume_batch_subscribed(&mut stream).await.ok()?;
		Some((stream, server_batch_id, acked))
	}

	async fn reconnect_stream(&mut self) -> bool {
		while self.attempt < self.reconnect.max_reconnect_attempts {
			self.attempt += 1;
			sleep(millis_to_std(backoff_millis(self.reconnect.reconnect_delay_ms, self.attempt))).await;

			let opened =
				timeout(millis_to_std(self.reconnect.connect_timeout_ms), self.open_stream()).await;
			if let Ok(Some((stream, server_batch_id, acked))) = opened {
				if let Some(entry) = self.batch_server_ids.lock().await.get_mut(&self.client_batch_id) {
					*entry = server_batch_id;
				}
				self.acked = acked;
				self.stream = stream;
				fire(&self.reconnect.on_reconnect);
				return true;
			}
		}
		false
	}
}

impl GrpcSubscription {
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}

	pub async fn recv(&mut self) -> Option<GrpcChange> {
		loop {
			match self.stream.message().await {
				Ok(Some(msg)) => {
					self.attempt = 0;
					match msg.event {
						Some(subscription_event::Event::Change(change)) => {
							if change.rbcf.is_empty() {
								return Some(to_grpc_change(Vec::new()));
							}
							return Some(change_from_rbcf(&change.rbcf));
						}
						Some(subscription_event::Event::Subscribed(_)) => continue,
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

	pub async fn recv_raw(&mut self) -> Result<Option<RawChangePayload>, Error> {
		loop {
			let Some(msg) = self.stream.message().await.map_err(status_to_error)? else {
				return Ok(None);
			};
			match msg.event {
				Some(subscription_event::Event::Change(change)) => {
					let payload = if change.rbcf.is_empty() {
						RawChangePayload::Empty
					} else {
						RawChangePayload::Rbcf(change.rbcf)
					};
					return Ok(Some(payload));
				}
				Some(subscription_event::Event::Subscribed(_)) => {
					continue;
				}
				None => continue,
			}
		}
	}

	async fn open_stream(&self) -> Option<Streaming<SubscriptionEvent>> {
		let channel = open_channel(&self.url).await.ok()?;
		let mut client = ReifyDbClient::new(channel);
		let mut req = Request::new(self.request.clone());
		attach_token(&mut req, &self.token);
		let mut stream = client.subscribe(req).await.ok()?.into_inner();
		consume_subscribed(&mut stream).await.ok()?;
		Some(stream)
	}

	async fn reconnect_stream(&mut self) -> bool {
		while self.attempt < self.reconnect.max_reconnect_attempts {
			self.attempt += 1;
			sleep(millis_to_std(backoff_millis(self.reconnect.reconnect_delay_ms, self.attempt))).await;

			let opened =
				timeout(millis_to_std(self.reconnect.connect_timeout_ms), self.open_stream()).await;
			if let Ok(Some(stream)) = opened {
				self.stream = stream;
				fire(&self.reconnect.on_reconnect);
				return true;
			}
		}
		false
	}
}

fn decode_rbcf(bytes: &[u8]) -> Result<Vec<Frame>, Error> {
	decode_frames(bytes).map_err(|e| ClientError::Decode(format!("failed to decode RBCF payload: {}", e)).into())
}

fn params_to_proto(params: Params) -> Result<Option<ProtoParams>, Error> {
	Ok(match params {
		Params::None => None,
		Params::Positional(values) => Some(ProtoParams {
			params: Some(ProtoParamsOneof::Positional(PositionalParams {
				values: Arc::unwrap_or_clone(values)
					.into_iter()
					.map(value_to_typed_value)
					.collect::<Result<_, Error>>()?,
			})),
		}),
		Params::Named(map) => Some(ProtoParams {
			params: Some(ProtoParamsOneof::Named(NamedParams {
				values: Arc::unwrap_or_clone(map)
					.into_iter()
					.map(|(k, v)| Ok((k, value_to_typed_value(v)?)))
					.collect::<Result<_, Error>>()?,
			})),
		}),
	})
}

fn subscription_options_to_proto(config: &SubscriptionConfig) -> Result<ProtoSubscribeOptions, Error> {
	Ok(ProtoSubscribeOptions {
		hydration: Some(ProtoHydrationOptions {
			enabled: Some(config.hydration.enabled),
			max_rows: config.hydration.max_rows,
		}),
		throttle: config
			.throttle
			.map(|throttle| value_to_typed_value(Value::Duration(throttle.duration())))
			.transpose()?,
		linger: config
			.linger
			.map(|linger| value_to_typed_value(Value::Duration(linger.duration())))
			.transpose()?,
	})
}

fn value_to_typed_value(value: Value) -> Result<TypedValue, Error> {
	Ok(TypedValue {
		encoded: encode_value(&value)
			.map_err(|e| ClientError::Encode(format!("failed to encode value: {}", e)))?,
	})
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
			decode_error: change.decode_error,
		})
	}
}

pub struct BatchGrpcSubscriptionAdapter {
	inner: BatchGrpcSubscription,
	subscription_infos: Vec<BatchSubscriptionInfo>,
}

#[async_trait::async_trait]
impl ClientBatchSubscription for BatchGrpcSubscriptionAdapter {
	fn batch_id(&self) -> &str {
		self.inner.batch_id()
	}

	fn subscriptions(&self) -> &[BatchSubscriptionInfo] {
		&self.subscription_infos
	}

	async fn recv(&mut self) -> Option<BatchPushEvent> {
		let event = self.inner.recv().await?;
		Some(match event {
			BatchStreamEvent::Change(env) => {
				let batch_id = env.batch_id.clone();
				let entries = env
					.entries
					.into_iter()
					.map(|(sub_id, change)| batch_change_entry(sub_id, change))
					.collect();
				BatchPushEvent::Change(BatchChangePayload {
					batch_id,
					entries,
				})
			}
			BatchStreamEvent::SubscriptionClosed {
				batch_id,
				subscription_id,
			} => BatchPushEvent::SubscriptionClosed(BatchSubscriptionClosedPayload {
				batch_id,
				subscription_id,
			}),
		})
	}
}

fn batch_change_entry(subscription_id: String, change: GrpcChange) -> BatchChangeEntry {
	BatchChangeEntry {
		subscription_id,
		content_type: "application/vnd.reifydb.grpc".to_string(),
		body: JsonValue::Null,
		changes: change.changes,
		decode_error: change.decode_error,
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
		items: &[BatchSubscribeItem<'a>],
	) -> Result<Box<dyn ClientBatchSubscription>, Error> {
		let inner = GrpcClient::batch_subscribe(self, items).await?;
		let subscription_infos: Vec<BatchSubscriptionInfo> = inner
			.subscriptions()
			.iter()
			.map(|m| BatchSubscriptionInfo {
				index: m.index,
				subscription_id: m.subscription_id.clone(),
			})
			.collect();
		Ok(Box::new(BatchGrpcSubscriptionAdapter {
			inner,
			subscription_infos,
		}))
	}

	async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		GrpcClient::batch_unsubscribe(self, batch_id).await
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
	use reifydb_value::value::{
		Value,
		container::number::NumberContainer,
		diff_type::DiffType,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	};

	use super::{GrpcChange, batch_change_entry, change_from_rbcf};
	use crate::ChangeKind;

	fn update_frame(id: i32) -> Frame {
		Frame::new(vec![FrameColumn {
			name: "id".to_string(),
			data: FrameColumnData::Int4(NumberContainer::from_vec(vec![id])),
		}])
		.with_op(DiffType::Update)
	}

	fn rbcf(frames: &[Frame]) -> Vec<u8> {
		encode_frames(frames, &EncodeOptions::default()).unwrap()
	}

	#[test]
	fn corrupt_rbcf_reaches_the_subscriber_as_a_decode_error() {
		// A payload that fails to decode must surface its error, never pass as an empty change.
		let mut bytes = rbcf(&[update_frame(7)]);
		bytes.truncate(bytes.len() / 2);

		let change = change_from_rbcf(&bytes);

		assert!(change.decode_error.is_some(), "a failed decode must carry its error");
		assert!(change.changes.is_empty());
	}

	#[test]
	fn valid_rbcf_decodes_to_its_changes_with_no_error() {
		// A clean payload must keep its rows and op, otherwise the error path swallowed a good change.
		let change = change_from_rbcf(&rbcf(&[update_frame(7)]));

		assert_eq!(change.decode_error, None);
		assert_eq!(change.changes.len(), 1);
		assert_eq!(change.changes[0].kind, ChangeKind::Update);
		assert_eq!(change.changes[0].frame.columns[0].data.get_value(0), Value::Int4(7));
	}

	#[test]
	fn batch_entry_carries_the_subscription_decode_error() {
		// A subscription's decode error must reach its batch entry, never be replaced by none.
		let change = GrpcChange {
			changes: Vec::new(),
			decode_error: Some("bad rbcf".to_string()),
		};

		let entry = batch_change_entry("server-1".to_string(), change);

		assert_eq!(entry.subscription_id, "server-1");
		assert_eq!(entry.decode_error, Some("bad rbcf".to_string()));
	}
}
