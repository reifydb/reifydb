// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[cfg_attr(any(feature = "http", feature = "ws"), serde(rename_all = "lowercase"))]
pub enum WireFormat {
	#[default]
	Frames,
	Rbcf,
}

#[cfg(any(feature = "ws", feature = "grpc"))]
mod changes;
#[cfg(any(feature = "ws", feature = "grpc"))]
pub mod client;
#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub mod dst;
#[cfg(any(feature = "http", feature = "ws", feature = "grpc"))]
pub mod error;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "http")]
pub mod http;
#[cfg(any(feature = "ws", feature = "grpc"))]
mod reconnect;
#[cfg(any(feature = "http", feature = "ws"))]
mod session;
#[cfg(any(feature = "ws", feature = "grpc", all(feature = "dst", reifydb_single_threaded)))]
pub mod subscription;
#[cfg(feature = "ws")]
mod utils;
#[cfg(feature = "ws")]
pub mod ws;

#[cfg(any(feature = "http", feature = "ws"))]
use std::collections::HashMap;
#[cfg(any(feature = "http", feature = "ws", feature = "grpc"))]
use std::sync::Arc;

#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub use dst::DstClient;
#[cfg(any(feature = "http", feature = "ws", feature = "grpc"))]
pub use error::ClientError;
#[cfg(feature = "grpc")]
pub use grpc::{
	BatchFramesEnvelope, BatchGrpcSubscription, BatchMemberHandle, BatchStreamEvent, GrpcChange, GrpcClient,
	GrpcClientOptions, GrpcSubscription, RawChangePayload,
};
#[cfg(feature = "http")]
pub use http::HttpClient;
pub use reifydb_client_derive::FromFrame;
#[cfg(any(feature = "http", feature = "ws"))]
use reifydb_codec::json::{NONE_MARKER, wire_type::WireValueType};
pub use reifydb_value as value;
#[cfg(any(feature = "ws", feature = "grpc"))]
use reifydb_value::error::Error;
pub use reifydb_value::{
	params::Params,
	value::{
		Value,
		frame::{
			column::FrameColumn,
			data::FrameColumnData,
			extract::FrameError,
			frame::Frame,
			from_frame::FromFrameError,
			row::{FrameRow, FrameRows},
		},
		iso::{IsoDate, IsoDateTime, IsoDuration, IsoTime},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		try_from::{FromValueError, TryFromValue, TryFromValueCoerce},
		value_type::ValueType,
	},
};
#[cfg(any(feature = "http", feature = "ws", feature = "grpc"))]
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
#[cfg(any(feature = "ws", feature = "grpc", all(feature = "dst", reifydb_single_threaded)))]
pub use subscription::{BatchItem, HydrationConfig, Linger, SubscriptionConfig, Throttle, build_subscription_rql};
#[cfg(feature = "ws")]
pub use ws::{WsBatchSubscription, WsClient, WsClientOptions};

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ResponseMeta {
	pub fingerprint: String,
	pub duration: String,
}

#[derive(Debug)]
pub struct AdminResult {
	pub frames: Vec<Frame>,
	pub meta: Option<ResponseMeta>,
}

#[derive(Debug)]
pub struct CommandResult {
	pub frames: Vec<Frame>,
	pub meta: Option<ResponseMeta>,
}

#[derive(Debug)]
pub struct QueryResult {
	pub frames: Vec<Frame>,
	pub meta: Option<ResponseMeta>,
}

#[derive(Debug, Clone)]
pub struct LoginResult {
	pub token: String,
	pub identity: String,
}

#[cfg(any(feature = "ws", feature = "grpc"))]
pub fn connection_lost_error() -> Error {
	ClientError::ConnectionLost.into()
}

#[cfg(any(feature = "ws", feature = "grpc"))]
#[derive(Clone)]
pub struct ReconnectOptions {
	pub max_reconnect_attempts: u32,
	pub reconnect_delay_ms: u64,
	pub connect_timeout_ms: u64,
	pub on_disconnect: Option<Arc<dyn Fn() + Send + Sync>>,
	pub on_reconnect: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(any(feature = "ws", feature = "grpc"))]
impl Default for ReconnectOptions {
	fn default() -> Self {
		Self {
			max_reconnect_attempts: 5,
			reconnect_delay_ms: 1000,
			connect_timeout_ms: 30_000,
			on_disconnect: None,
			on_reconnect: None,
		}
	}
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct WireValue {
	#[serde(rename = "type")]
	pub r#type: WireValueType,
	pub value: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireParams {
	Positional(Vec<WireValue>),
	Named(HashMap<String, WireValue>),
}

// a Rust Value has no Some wrapper, so a client can send a none of T but not a Some(None) of Option(T)
#[cfg(any(feature = "http", feature = "ws"))]
fn value_to_wire(value: Value) -> WireValue {
	let text = match &value {
		Value::None {
			..
		} => NONE_MARKER.to_string(),
		Value::Duration(d) => d.to_iso_string(),
		Value::Blob(b) => b.to_hex(),
		Value::Any(v) => return value_to_wire(*v.clone()),
		other => other.to_string(),
	};
	WireValue {
		r#type: WireValueType(value.get_type()),
		value: text,
	}
}

#[cfg(any(feature = "http", feature = "ws"))]
pub fn params_to_wire(params: Params) -> Option<WireParams> {
	match params {
		Params::None => None,
		Params::Positional(values) => Some(WireParams::Positional(
			Arc::unwrap_or_clone(values).into_iter().map(value_to_wire).collect(),
		)),
		Params::Named(map) => Some(WireParams::Named(
			Arc::unwrap_or_clone(map).into_iter().map(|(k, v)| (k, value_to_wire(v))).collect(),
		)),
	}
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Admin(AdminRequest),
	Command(CommandRequest),
	Query(QueryRequest),
	Subscribe(SubscribeRequest),
	Unsubscribe(UnsubscribeRequest),
	BatchSubscribe(BatchSubscribeRequest),
	BatchUnsubscribe(BatchUnsubscribeRequest),
	Call(CallRequest),
	QueueClaim(WsQueueClaimRequest),
	Logout,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	pub rql: String,
	pub params: Option<WireParams>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub method: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub credentials: Option<HashMap<String, String>>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub rql: String,
	pub params: Option<WireParams>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub rql: String,
	pub params: Option<WireParams>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	pub rql: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSubscribeRequest {
	pub queries: Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUnsubscribeRequest {
	pub batch_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CallRequest {
	pub name: String,
	pub params: Option<WireParams>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws", feature = "grpc"))]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueClaimRequest {
	pub queue: String,
	pub worker: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max_n: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub lease_ttl: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub wait_for: Option<String>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct WsQueueClaimRequest {
	pub queue: String,
	pub worker: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max_n: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub lease_ttl: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub wait_for: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<WireFormat>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
	Auth(AuthResponse),
	Err(ErrResponse),
	Admin(AdminResponse),
	Command(CommandResponse),
	Query(QueryResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
	BatchSubscribed(BatchSubscribedResponse),
	BatchUnsubscribed(BatchUnsubscribedResponse),
	Call(CallResponse),
	Logout(LogoutResponsePayload),
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminResponse {
	pub content_type: String,
	pub body: JsonValue,
	#[serde(default)]
	pub meta: Option<ResponseMeta>,
}

#[cfg(any(feature = "http", feature = "ws"))]
use reifydb_value::error::Diagnostic;

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub status: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub identity: Option<String>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
	pub content_type: String,
	pub body: JsonValue,
	#[serde(default)]
	pub meta: Option<ResponseMeta>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
	pub content_type: String,
	pub body: JsonValue,
	#[serde(default)]
	pub meta: Option<ResponseMeta>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CallResponse {
	pub content_type: String,
	pub body: JsonValue,
	#[serde(default)]
	pub meta: Option<ResponseMeta>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribedResponse {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribedResponse {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSubscribedResponse {
	pub batch_id: String,
	pub members: Vec<BatchMemberInfo>,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BatchMemberInfo {
	pub index: usize,
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUnsubscribedResponse {
	pub batch_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct LogoutResponsePayload {
	pub status: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(WireChangePayload),
	BatchChange(WireBatchChangePayload),
	BatchMemberClosed(BatchMemberClosedPayload),
	BatchClosed(BatchClosedPayload),
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireChangePayload {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireBatchChangePayload {
	pub batch_id: String,
	pub entries: Vec<WireBatchChangeEntry>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireBatchChangeEntry {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
	Insert,
	Update,
	Remove,
}

#[derive(Debug, Clone)]
pub struct FrameChange {
	pub kind: ChangeKind,
	pub frame: Frame,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ChangePayload {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
	#[cfg_attr(any(feature = "http", feature = "ws"), serde(skip, default))]
	pub changes: Vec<FrameChange>,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BatchChangePayload {
	pub batch_id: String,
	pub entries: Vec<BatchChangeEntry>,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BatchChangeEntry {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
	#[cfg_attr(any(feature = "http", feature = "ws"), serde(skip, default))]
	pub changes: Vec<FrameChange>,
	#[cfg_attr(any(feature = "http", feature = "ws"), serde(skip, default))]
	pub decode_error: Option<String>,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BatchMemberClosedPayload {
	pub batch_id: String,
	pub subscription_id: String,
}

#[cfg_attr(any(feature = "http", feature = "ws"), derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BatchClosedPayload {
	pub batch_id: String,
}

#[derive(Debug, Clone)]
pub enum BatchPushEvent {
	Change(BatchChangePayload),
	MemberClosed(BatchMemberClosedPayload),
	Closed(BatchClosedPayload),
}

#[cfg(all(test, any(feature = "http", feature = "ws")))]
mod tests {
	use serde_json::{json, to_value};

	use super::*;

	fn option(inner: ValueType) -> ValueType {
		ValueType::Option(Box::new(inner))
	}

	#[test]
	fn a_present_value_carries_its_own_type_and_text() {
		let wire = value_to_wire(Value::Int4(5));
		assert_eq!(wire.r#type.0, ValueType::Int4);
		assert_eq!(wire.value, "5");
	}

	#[test]
	fn a_none_of_int4_is_an_option_of_int4_with_the_bare_marker() {
		let wire = value_to_wire(Value::none_of(ValueType::Int4));
		assert_eq!(wire.r#type.0, option(ValueType::Int4));
		assert_eq!(wire.value, NONE_MARKER);
	}

	#[test]
	fn a_none_of_option_int4_is_a_two_layer_option_with_the_bare_marker() {
		// The none sits at the outermost layer, so the marker carries no depth suffix even
		// though the type has two Option layers.
		let wire = value_to_wire(Value::none_of(option(ValueType::Int4)));
		assert_eq!(wire.r#type.0, option(option(ValueType::Int4)));
		assert_eq!(wire.value, NONE_MARKER);
	}

	#[test]
	fn a_parameter_serializes_the_type_as_the_descriptor_the_server_parses() {
		// The server reads a parameter type as a descriptor object, not as a bare name, so a
		// regression here silently breaks every parameterized statement this client sends.
		let wire = value_to_wire(Value::Int4(5));
		assert_eq!(to_value(&wire).unwrap(), json!({"type": {"id": "Int4"}, "value": "5"}));
	}

	#[test]
	fn a_none_parameter_serializes_the_option_layer_as_a_nested_descriptor() {
		let wire = value_to_wire(Value::none_of(option(ValueType::Int4)));
		assert_eq!(
			to_value(&wire).unwrap(),
			json!({
				"type": {"id": "Option", "underlying": {"id": "Option", "underlying": {"id": "Int4"}}},
				"value": NONE_MARKER
			})
		);
	}
}
