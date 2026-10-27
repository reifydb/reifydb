// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

pub use reifydb_codec::wire::RawChangePayload;
use reifydb_codec::{frame::decode::decode_frames, value::encode_value};
use reifydb_value::{
	error::{Diagnostic, Error},
	params::Params,
	value::{Value, frame::frame::Frame},
};
use serde_json::{Value as JsonValue, from_str as serde_json_from_str};
use tokio::{
	spawn,
	sync::{Mutex, mpsc},
	task::JoinHandle,
	time::{sleep, timeout},
};
use tonic::{
	Code, Request, Status,
	codec::Streaming,
	metadata::{Ascii, MetadataMap, MetadataValue},
	transport::Channel,
};

use super::generated::{
	AdminRequest as ProtoAdminRequest, AuthenticateRequest as ProtoAuthenticateRequest, ChangeEvent,
	CommandRequest as ProtoCommandRequest, HydrationOptions as ProtoHydrationOptions,
	LogoutRequest as ProtoLogoutRequest, NamedParams, OperationRequest as ProtoOperationRequest,
	Params as ProtoParams, PositionalParams, QueryRequest as ProtoQueryRequest,
	QueueClaimRequest as ProtoQueueClaimRequest, SubscribeItem as ProtoSubscribeItem,
	SubscribeOptions as ProtoSubscribeOptions, SubscribeRequest as ProtoSubscribeRequest, SubscriptionEvent,
	TypedValue, UnsubscribeRequest as ProtoUnsubscribeRequest, params::Params as ProtoParamsOneof,
	reify_db_client::ReifyDbClient, subscription_event,
};
use crate::{
	AdminResult, ChangePayload, CommandResult, FrameChange, LoginResult, QueryResult, QueueClaimRequest,
	ReconnectOptions, ResponseMeta, WireFormat,
	changes::frames_to_changes,
	client::{ReifyClient, Subscription as ClientSubscription},
	error::ClientError,
	reconnect::{backoff_millis, fire, millis_to_std},
	subscription::{SubscribeItem, SubscriptionConfig},
};

type RouteSender = mpsc::UnboundedSender<Result<RawChangePayload, Error>>;

struct Route {
	handle_id: String,
	sender: RouteSender,
}

type Routes = Arc<Mutex<HashMap<String, Route>>>;

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
	reconnect: ReconnectOptions,
	routes: Routes,
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
			reconnect: options.reconnect,
			routes: Arc::new(Mutex::new(HashMap::new())),
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

		let mut request = Request::new(request);
		attach_token(&mut request, &self.token);
		let mut client = self.inner.clone();
		let response = client.authenticate(request).await.map_err(status_to_error)?;
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

	pub async fn subscribe(&self, items: &[SubscribeItem<'_>]) -> Result<Vec<GrpcSubscription>, Error> {
		let items: Vec<ProtoSubscribeItem> = items
			.iter()
			.map(|item| {
				Ok(ProtoSubscribeItem {
					rql: item.rql.to_string(),
					options: Some(subscription_options_to_proto(&item.config)?),
				})
			})
			.collect::<Result<_, Error>>()?;

		let mut client = self.inner.clone();
		let mut req = Request::new(ProtoSubscribeRequest {
			subscriptions: items.clone(),
		});
		self.attach_auth(&mut req);

		let response = client.subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();
		let server_ids = consume_subscribed(&mut stream, items.len()).await?;

		let mut receivers = Vec::with_capacity(server_ids.len());
		{
			let mut routes = self.routes.lock().await;
			for server_id in &server_ids {
				let (sender, receiver) = mpsc::unbounded_channel();
				routes.insert(
					server_id.clone(),
					Route {
						handle_id: server_id.clone(),
						sender,
					},
				);
				receivers.push(receiver);
			}
		}

		let reader = StreamReader {
			items,
			server_ids: server_ids.clone(),
			routes: self.routes.clone(),
			client: self.inner.clone(),
			token: self.token.clone(),
			reconnect: self.reconnect.clone(),
		};
		let task = Arc::new(StreamTask(spawn(reader.run(stream))));

		Ok(server_ids
			.into_iter()
			.zip(receivers)
			.map(|(subscription_id, rx)| GrpcSubscription {
				subscription_id,
				rx,
				_task: task.clone(),
			})
			.collect())
	}

	pub async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), Error> {
		let targets: Vec<String> = {
			let mut routes = self.routes.lock().await;
			subscription_ids
				.iter()
				.map(|handle_id| {
					let server_id = routes
						.iter()
						.find(|(_, route)| route.handle_id == *handle_id)
						.map(|(server_id, _)| server_id.clone());
					match server_id {
						Some(server_id) => {
							routes.remove(&server_id);
							server_id
						}
						None => handle_id.to_string(),
					}
				})
				.collect()
		};
		let request = ProtoUnsubscribeRequest {
			subscription_ids: targets,
		};
		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);
		client.unsubscribe(req).await.map_err(status_to_error)?;
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

async fn consume_subscribed(stream: &mut Streaming<SubscriptionEvent>, expected: usize) -> Result<Vec<String>, Error> {
	let first = stream.message().await.map_err(status_to_error)?.ok_or_else(|| {
		ClientError::UnexpectedResponse("Stream closed before receiving subscription IDs".to_string())
	})?;
	let Some(subscription_event::Event::Subscribed(subscribed)) = first.event else {
		return Err(
			ClientError::UnexpectedResponse("Expected SubscribedEvent as first message".to_string()).into()
		);
	};
	let mut acked = subscribed.subscriptions;
	if acked.len() != expected {
		return Err(ClientError::UnexpectedResponse(format!(
			"Subscribed acknowledged {} of {} subscriptions",
			acked.len(),
			expected
		))
		.into());
	}
	acked.sort_by_key(|item| item.index);
	Ok(acked.into_iter().map(|item| item.subscription_id).collect())
}

struct StreamTask(JoinHandle<()>);

impl Drop for StreamTask {
	fn drop(&mut self) {
		self.0.abort();
	}
}

struct StreamReader {
	items: Vec<ProtoSubscribeItem>,
	server_ids: Vec<String>,
	routes: Routes,
	client: ReifyDbClient<Channel>,
	token: Option<String>,
	reconnect: ReconnectOptions,
}

impl StreamReader {
	async fn run(mut self, mut stream: Streaming<SubscriptionEvent>) {
		loop {
			let ended = loop {
				match stream.message().await {
					Ok(Some(message)) => match message.event {
						Some(subscription_event::Event::Change(change)) => {
							route_change_event(change, &mut *self.routes.lock().await)
						}
						Some(subscription_event::Event::SubscriptionClosed(closed)) => {
							self.routes.lock().await.remove(&closed.subscription_id);
						}
						Some(subscription_event::Event::Subscribed(_)) | None => {}
					},
					Ok(None) => break None,
					Err(status) => break Some(status_to_error(status)),
				}
			};
			if self.live().await.is_empty() {
				return;
			}
			fire(&self.reconnect.on_disconnect);
			match self.reopen().await {
				Some(reopened) => {
					stream = reopened;
					fire(&self.reconnect.on_reconnect);
				}
				None => {
					let error = if self.reconnect.max_reconnect_attempts > 0 {
						Some(Error::from(ClientError::ConnectionLost))
					} else {
						ended
					};
					self.end(error).await;
					return;
				}
			}
		}
	}

	async fn live(&self) -> Vec<usize> {
		let routes = self.routes.lock().await;
		(0..self.server_ids.len())
			.filter(|k| routes.get(&self.server_ids[*k]).is_some_and(|route| !route.sender.is_closed()))
			.collect()
	}

	async fn reopen(&mut self) -> Option<Streaming<SubscriptionEvent>> {
		let mut attempt = 0u32;
		while attempt < self.reconnect.max_reconnect_attempts {
			attempt += 1;
			sleep(millis_to_std(backoff_millis(self.reconnect.reconnect_delay_ms, attempt))).await;
			let live = self.live().await;
			if live.is_empty() {
				return None;
			}
			let opened = timeout(millis_to_std(self.reconnect.connect_timeout_ms), self.open(&live)).await;
			if let Ok(Some((stream, server_ids))) = opened {
				let mut routes = self.routes.lock().await;
				for (k, server_id) in live.into_iter().zip(server_ids) {
					if let Some(route) = routes.remove(&self.server_ids[k]) {
						routes.insert(server_id.clone(), route);
					}
					self.server_ids[k] = server_id;
				}
				return Some(stream);
			}
		}
		None
	}

	async fn open(&self, live: &[usize]) -> Option<(Streaming<SubscriptionEvent>, Vec<String>)> {
		let mut client = self.client.clone();
		let mut req = Request::new(ProtoSubscribeRequest {
			subscriptions: live.iter().map(|k| self.items[*k].clone()).collect(),
		});
		attach_token(&mut req, &self.token);
		let mut stream = client.subscribe(req).await.ok()?.into_inner();
		let server_ids = consume_subscribed(&mut stream, live.len()).await.ok()?;
		Some((stream, server_ids))
	}

	async fn end(&self, error: Option<Error>) {
		let mut routes = self.routes.lock().await;
		for server_id in &self.server_ids {
			if let Some(route) = routes.remove(server_id)
				&& let Some(error) = &error
			{
				let _ = route.sender.send(Err(Error(error.0.clone())));
			}
		}
	}
}

fn route_change_event(change: ChangeEvent, routes: &mut HashMap<String, Route>) {
	for entry in change.entries {
		let payload = if entry.rbcf.is_empty() {
			RawChangePayload::Empty
		} else {
			RawChangePayload::Rbcf(entry.rbcf)
		};
		let delivered = routes.get(&entry.subscription_id).map(|route| route.sender.send(Ok(payload)).is_ok());
		if delivered == Some(false) {
			routes.remove(&entry.subscription_id);
		}
	}
}

pub struct GrpcSubscription {
	subscription_id: String,
	rx: mpsc::UnboundedReceiver<Result<RawChangePayload, Error>>,
	_task: Arc<StreamTask>,
}

impl GrpcSubscription {
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}

	pub async fn recv(&mut self) -> Option<GrpcChange> {
		match self.rx.recv().await? {
			Ok(RawChangePayload::Rbcf(bytes)) => Some(change_from_rbcf(&bytes)),
			Ok(RawChangePayload::Empty) => Some(to_grpc_change(Vec::new())),
			Err(_) => None,
		}
	}

	pub async fn recv_raw(&mut self) -> Result<Option<RawChangePayload>, Error> {
		self.rx.recv().await.transpose()
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

	async fn subscribe<'a>(&self, items: &[SubscribeItem<'a>]) -> Result<Vec<Box<dyn ClientSubscription>>, Error> {
		Ok(GrpcClient::subscribe(self, items)
			.await?
			.into_iter()
			.map(|inner| {
				Box::new(GrpcSubscriptionAdapter {
					inner,
				}) as Box<dyn ClientSubscription>
			})
			.collect())
	}

	async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), Error> {
		GrpcClient::unsubscribe(self, subscription_ids).await
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
	use reifydb_core::value::{batch::batch, column::factory};
	use reifydb_value::value::{Value, diff_type::DiffType, frame::frame::Frame};
	use tokio::sync::mpsc::unbounded_channel;

	use super::{RawChangePayload, Route, change_from_rbcf, route_change_event};
	use crate::{
		ChangeKind,
		grpc::generated::{ChangeEntry, ChangeEvent},
	};

	fn update_frame(id: i32) -> Frame {
		Frame::from(batch(vec![factory::int4("id", [id])]).expect("an int4 column forms a batch"))
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
		let id = change.changes[0]
			.frame
			.column("id")
			.expect("the id column reads")
			.expect("the id column decodes");
		assert_eq!(id.get_value(0), Value::Int4(7));
	}

	#[test]
	fn a_change_event_splits_into_one_payload_per_listed_subscription() {
		// Sending every entry to every handle, or dropping the empty one, hands a subscription another's rows.
		let (first_tx, mut first_rx) = unbounded_channel();
		let (second_tx, mut second_rx) = unbounded_channel();
		let mut routes = HashMap::from([
			(
				"1".to_string(),
				Route {
					handle_id: "1".to_string(),
					sender: first_tx,
				},
			),
			(
				"2".to_string(),
				Route {
					handle_id: "2".to_string(),
					sender: second_tx,
				},
			),
		]);
		let bytes = rbcf(&[update_frame(7)]);

		route_change_event(
			ChangeEvent {
				entries: vec![
					ChangeEntry {
						subscription_id: "1".to_string(),
						rbcf: bytes.clone(),
					},
					ChangeEntry {
						subscription_id: "2".to_string(),
						rbcf: Vec::new(),
					},
					ChangeEntry {
						subscription_id: "3".to_string(),
						rbcf: bytes.clone(),
					},
				],
			},
			&mut routes,
		);

		assert!(matches!(first_rx.try_recv(), Ok(Ok(RawChangePayload::Rbcf(b))) if b == bytes));
		assert!(first_rx.try_recv().is_err(), "handle 1 must receive only its own entry");
		assert!(matches!(second_rx.try_recv(), Ok(Ok(RawChangePayload::Empty))));
		assert!(second_rx.try_recv().is_err(), "handle 2 must receive only its own entry");
		assert_eq!(routes.len(), 2, "an entry for an unknown id must not create or drop a route");
	}
}
