// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use num_bigint::BigInt;
use reifydb_type::{
	error::{Diagnostic, Error},
	fragment::Fragment,
	params::Params,
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		temporal::parse::datetime::parse_datetime,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use reifydb_wire_format::decode::decode_frames;
use serde_json::from_str as serde_json_from_str;
use tonic::{
	Request, Status,
	codec::Streaming,
	metadata::{Ascii, MetadataValue},
	transport::Channel,
};
use uuid::Uuid;

use super::generated::{
	AdminRequest as ProtoAdminRequest, AuthenticateRequest as ProtoAuthenticateRequest,
	CommandRequest as ProtoCommandRequest, Format, Frame as ProtoFrame, LogoutRequest as ProtoLogoutRequest,
	NamedParams, Params as ProtoParams, PositionalParams, QueryRequest as ProtoQueryRequest,
	SubscribeRequest as ProtoSubscribeRequest, SubscriptionEvent, TypedValue,
	UnsubscribeRequest as ProtoUnsubscribeRequest, admin_response, change_event, command_response,
	params::Params as ProtoParamsOneof, query_response, reify_db_client::ReifyDbClient, subscription_event,
};
use crate::{AdminResult, CommandResult, LoginResult, QueryResult, WireFormat};

#[derive(Clone)]
pub struct GrpcClient {
	inner: ReifyDbClient<Channel>,
	token: Option<String>,
	format: WireFormat,
}

impl GrpcClient {
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		if format == WireFormat::Json {
			return Err(Error(Box::new(Diagnostic {
				code: "INVALID_FORMAT".to_string(),
				message: "WireFormat::Json is not supported for GrpcClient".to_string(),
				..Default::default()
			})));
		}

		let channel =
			Channel::from_shared(url.to_string()).unwrap().tcp_nodelay(true).connect().await.map_err(
				|e| {
					Error(Box::new(Diagnostic {
						code: "GRPC_CONNECT".to_string(),
						message: format!("Failed to connect: {}", e),
						..Default::default()
					}))
				},
			)?;

		Ok(Self {
			inner: ReifyDbClient::new(channel),
			token: None,
			format,
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
			Err(Error(Box::new(Diagnostic {
				code: "AUTH_FAILED".to_string(),
				message: inner.reason,
				..Default::default()
			})))
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

	fn wire_format(&self) -> i32 {
		match self.format {
			WireFormat::Rbcf => Format::Rbcf as i32,
			_ => Format::Proto as i32,
		}
	}

	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = ProtoAdminRequest {
			statements: vec![rql.to_string()],
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.admin(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_admin_payload(inner.payload)?;
		Ok(AdminResult {
			frames,
		})
	}

	pub async fn admin_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<AdminResult, Error> {
		let request = ProtoAdminRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.admin(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_admin_payload(inner.payload)?;
		Ok(AdminResult {
			frames,
		})
	}

	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let request = ProtoCommandRequest {
			statements: vec![rql.to_string()],
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.command(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_command_payload(inner.payload)?;
		Ok(CommandResult {
			frames,
		})
	}

	pub async fn command_batch(
		&self,
		statements: Vec<&str>,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		let request = ProtoCommandRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.command(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_command_payload(inner.payload)?;
		Ok(CommandResult {
			frames,
		})
	}

	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = ProtoQueryRequest {
			statements: vec![rql.to_string()],
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.query(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_query_payload(inner.payload)?;
		Ok(QueryResult {
			frames,
		})
	}

	pub async fn query_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<QueryResult, Error> {
		let request = ProtoQueryRequest {
			statements: statements.into_iter().map(String::from).collect(),
			params: params.and_then(params_to_proto),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let inner = client.query(req).await.map_err(status_to_error)?.into_inner();
		let frames = decode_query_payload(inner.payload)?;
		Ok(QueryResult {
			frames,
		})
	}

	pub async fn subscribe(&self, rql: &str) -> Result<GrpcSubscription, Error> {
		let request = ProtoSubscribeRequest {
			rql: rql.to_string(),
			format: self.wire_format(),
		};

		let mut client = self.inner.clone();
		let mut req = Request::new(request);
		self.attach_auth(&mut req);

		let response = client.subscribe(req).await.map_err(status_to_error)?;
		let mut stream = response.into_inner();

		// Consume the initial SubscribedEvent to extract subscription_id
		let first = stream.message().await.map_err(status_to_error)?.ok_or_else(|| {
			Error(Box::new(Diagnostic {
				code: "GRPC_SUBSCRIBE".to_string(),
				message: "Stream closed before receiving subscription ID".to_string(),
				..Default::default()
			}))
		})?;

		let subscription_id = match first.event {
			Some(subscription_event::Event::Subscribed(s)) => s.subscription_id,
			_ => {
				return Err(Error(Box::new(Diagnostic {
					code: "GRPC_SUBSCRIBE".to_string(),
					message: "Expected SubscribedEvent as first message".to_string(),
					..Default::default()
				})));
			}
		};

		Ok(GrpcSubscription {
			subscription_id,
			stream,
			format: self.format,
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

	fn attach_auth<T>(&self, request: &mut Request<T>) {
		if let Some(ref token) = self.token {
			let bearer = format!("Bearer {}", token);
			if let Ok(value) = bearer.parse::<MetadataValue<Ascii>>() {
				request.metadata_mut().insert("authorization", value);
			}
		}
	}
}

pub struct GrpcSubscription {
	subscription_id: String,
	stream: Streaming<SubscriptionEvent>,
	#[allow(dead_code)]
	format: WireFormat,
}

impl GrpcSubscription {
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}

	pub async fn recv(&mut self) -> Option<Vec<Frame>> {
		loop {
			let msg = self.stream.message().await.ok()??;
			match msg.event {
				Some(subscription_event::Event::Change(change)) => {
					let frames = match change.payload {
						Some(change_event::Payload::Rbcf(bytes)) => {
							decode_frames(&bytes).unwrap_or_default()
						}
						Some(change_event::Payload::Frames(fp)) => {
							proto_frames_to_frames(fp.frames)
						}
						None => Vec::new(),
					};
					return Some(frames);
				}
				Some(subscription_event::Event::Subscribed(_)) => {
					// Unexpected but skip
					continue;
				}
				None => continue,
			}
		}
	}
}

fn decode_admin_payload(payload: Option<admin_response::Payload>) -> Result<Vec<Frame>, Error> {
	match payload {
		Some(admin_response::Payload::Rbcf(bytes)) => decode_rbcf(&bytes),
		Some(admin_response::Payload::Frames(fp)) => Ok(proto_frames_to_frames(fp.frames)),
		None => Ok(Vec::new()),
	}
}

fn decode_command_payload(payload: Option<command_response::Payload>) -> Result<Vec<Frame>, Error> {
	match payload {
		Some(command_response::Payload::Rbcf(bytes)) => decode_rbcf(&bytes),
		Some(command_response::Payload::Frames(fp)) => Ok(proto_frames_to_frames(fp.frames)),
		None => Ok(Vec::new()),
	}
}

fn decode_query_payload(payload: Option<query_response::Payload>) -> Result<Vec<Frame>, Error> {
	match payload {
		Some(query_response::Payload::Rbcf(bytes)) => decode_rbcf(&bytes),
		Some(query_response::Payload::Frames(fp)) => Ok(proto_frames_to_frames(fp.frames)),
		None => Ok(Vec::new()),
	}
}

fn decode_rbcf(bytes: &[u8]) -> Result<Vec<Frame>, Error> {
	decode_frames(bytes).map_err(|e| {
		Error(Box::new(Diagnostic {
			code: "RBCF_DECODE".to_string(),
			message: format!("failed to decode RBCF payload: {}", e),
			..Default::default()
		}))
	})
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
	let (type_u32, bytes) = match value {
		Value::None {
			inner,
		} => ((0x80 | inner.to_u8()) as u32, vec![]),
		Value::Boolean(b) => (Type::Boolean.to_u8() as u32, vec![b as u8]),
		Value::Float4(f) => (Type::Float4.to_u8() as u32, f.to_le_bytes().to_vec()),
		Value::Float8(f) => (Type::Float8.to_u8() as u32, f.to_le_bytes().to_vec()),
		Value::Int1(v) => (Type::Int1.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Int2(v) => (Type::Int2.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Int4(v) => (Type::Int4.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Int8(v) => (Type::Int8.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Int16(v) => (Type::Int16.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Uint1(v) => (Type::Uint1.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Uint2(v) => (Type::Uint2.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Uint4(v) => (Type::Uint4.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Uint8(v) => (Type::Uint8.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Uint16(v) => (Type::Uint16.to_u8() as u32, v.to_le_bytes().to_vec()),
		Value::Utf8(s) => (Type::Utf8.to_u8() as u32, s.into_bytes()),
		Value::Uuid4(u) => (Type::Uuid4.to_u8() as u32, u.0.as_bytes().to_vec()),
		Value::Uuid7(u) => (Type::Uuid7.to_u8() as u32, u.0.as_bytes().to_vec()),
		Value::Date(d) => (Type::Date.to_u8() as u32, d.to_days_since_epoch().to_le_bytes().to_vec()),
		Value::DateTime(dt) => (Type::DateTime.to_u8() as u32, dt.to_nanos().to_le_bytes().to_vec()),
		Value::Time(t) => (Type::Time.to_u8() as u32, t.to_nanos_since_midnight().to_le_bytes().to_vec()),
		Value::Duration(d) => {
			let mut buf = Vec::with_capacity(16);
			buf.extend_from_slice(&d.get_months().to_le_bytes());
			buf.extend_from_slice(&d.get_days().to_le_bytes());
			buf.extend_from_slice(&d.get_nanos().to_le_bytes());
			(Type::Duration.to_u8() as u32, buf)
		}
		Value::Blob(b) => (Type::Blob.to_u8() as u32, b.as_bytes().to_vec()),
		Value::Decimal(d) => (Type::Decimal.to_u8() as u32, d.to_string().into_bytes()),
		Value::IdentityId(id) => (Type::IdentityId.to_u8() as u32, id.0.0.as_bytes().to_vec()),
		Value::Int(big) => (Type::Int.to_u8() as u32, big.0.to_signed_bytes_le()),
		Value::Uint(big) => (Type::Uint.to_u8() as u32, big.0.to_signed_bytes_le()),
		Value::Any(inner) => return value_to_typed_value(*inner),
		Value::DictionaryId(id) => (Type::DictionaryId.to_u8() as u32, id.to_u128().to_le_bytes().to_vec()),
		Value::Type(t) => (Type::Any.to_u8() as u32, vec![t.to_u8()]),
		Value::List(items) | Value::Tuple(items) => {
			let mut buf = Vec::new();
			buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
			for item in &items {
				let tv = value_to_typed_value(item.clone());
				buf.extend_from_slice(&tv.r#type.to_le_bytes());
				buf.extend_from_slice(&(tv.value.len() as u32).to_le_bytes());
				buf.extend_from_slice(&tv.value);
			}
			(Type::Any.to_u8() as u32, buf)
		}
		Value::Record(fields) => {
			let mut buf = Vec::new();
			buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
			for (key, value) in fields {
				let key_bytes = key.as_bytes();
				buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(key_bytes);
				let tv = value_to_typed_value(value);
				buf.extend_from_slice(&tv.r#type.to_le_bytes());
				buf.extend_from_slice(&(tv.value.len() as u32).to_le_bytes());
				buf.extend_from_slice(&tv.value);
			}
			(Type::Any.to_u8() as u32, buf)
		}
	};
	TypedValue {
		r#type: type_u32,
		value: bytes,
	}
}

fn proto_frames_to_frames(frames: Vec<ProtoFrame>) -> Vec<Frame> {
	frames.into_iter()
		.map(|f| {
			let row_numbers: Vec<RowNumber> = f.row_numbers.into_iter().map(RowNumber::new).collect();
			let columns: Vec<FrameColumn> = f
				.columns
				.into_iter()
				.map(|c| {
					let ty = Type::from_u8(c.r#type as u8);
					let data = decode_column_data(ty, &c.payload, &c.bitvec);
					FrameColumn {
						name: c.name,
						data,
					}
				})
				.collect();
			let created_at = f
				.created_at
				.iter()
				.filter_map(|s| parse_datetime(Fragment::internal(s)).ok())
				.collect();
			let updated_at = f
				.updated_at
				.iter()
				.filter_map(|s| parse_datetime(Fragment::internal(s)).ok())
				.collect();
			Frame {
				row_numbers,
				created_at,
				updated_at,
				columns,
			}
		})
		.collect()
}

fn decode_column_data(ty: Type, data: &[u8], bitvec_bytes: &[u8]) -> FrameColumnData {
	match ty {
		Type::Option(inner_type) => {
			let bitvec = decode_bitvec(bitvec_bytes);
			let inner = decode_column_data(*inner_type, data, &[]);
			FrameColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
		Type::Boolean => {
			let bitvec = decode_bitvec(data);
			let values: Vec<bool> = bitvec.iter().collect();
			FrameColumnData::Bool(BoolContainer::new(values))
		}
		Type::Float4 => {
			let values: Vec<f32> = data
				.chunks_exact(4)
				.map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Float4(NumberContainer::new(values))
		}
		Type::Float8 => {
			let values: Vec<f64> = data
				.chunks_exact(8)
				.map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Float8(NumberContainer::new(values))
		}
		Type::Int1 => {
			let values: Vec<i8> = data.iter().map(|&b| b as i8).collect();
			FrameColumnData::Int1(NumberContainer::new(values))
		}
		Type::Int2 => {
			let values: Vec<i16> = data
				.chunks_exact(2)
				.map(|chunk| i16::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Int2(NumberContainer::new(values))
		}
		Type::Int4 => {
			let values: Vec<i32> = data
				.chunks_exact(4)
				.map(|chunk| i32::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Int4(NumberContainer::new(values))
		}
		Type::Int8 => {
			let values: Vec<i64> = data
				.chunks_exact(8)
				.map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Int8(NumberContainer::new(values))
		}
		Type::Int16 => {
			let values: Vec<i128> = data
				.chunks_exact(16)
				.map(|chunk| i128::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Int16(NumberContainer::new(values))
		}
		Type::Uint1 => {
			let values: Vec<u8> = data.to_vec();
			FrameColumnData::Uint1(NumberContainer::new(values))
		}
		Type::Uint2 => {
			let values: Vec<u16> = data
				.chunks_exact(2)
				.map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Uint2(NumberContainer::new(values))
		}
		Type::Uint4 => {
			let values: Vec<u32> = data
				.chunks_exact(4)
				.map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Uint4(NumberContainer::new(values))
		}
		Type::Uint8 => {
			let values: Vec<u64> = data
				.chunks_exact(8)
				.map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Uint8(NumberContainer::new(values))
		}
		Type::Uint16 => {
			let values: Vec<u128> = data
				.chunks_exact(16)
				.map(|chunk| u128::from_le_bytes(chunk.try_into().unwrap()))
				.collect();
			FrameColumnData::Uint16(NumberContainer::new(values))
		}
		Type::Utf8 => {
			let values = decode_length_prefixed_strings(data);
			FrameColumnData::Utf8(Utf8Container::new(values))
		}
		Type::Date => {
			let values: Vec<Date> = data
				.chunks_exact(4)
				.map(|chunk| {
					let days = i32::from_le_bytes(chunk.try_into().unwrap());
					Date::from_days_since_epoch(days)
						.unwrap_or_else(|| Date::from_ymd(1970, 1, 1).unwrap())
				})
				.collect();
			FrameColumnData::Date(TemporalContainer::new(values))
		}
		Type::DateTime => {
			let values: Vec<DateTime> = data
				.chunks_exact(8)
				.map(|chunk| {
					let nanos = u64::from_le_bytes(chunk.try_into().unwrap());
					DateTime::from_nanos(nanos)
				})
				.collect();
			FrameColumnData::DateTime(TemporalContainer::new(values))
		}
		Type::Time => {
			let values: Vec<Time> = data
				.chunks_exact(8)
				.map(|chunk| {
					let nanos = u64::from_le_bytes(chunk.try_into().unwrap());
					Time::from_nanos_since_midnight(nanos)
						.unwrap_or_else(|| Time::from_hms(0, 0, 0).unwrap())
				})
				.collect();
			FrameColumnData::Time(TemporalContainer::new(values))
		}
		Type::Duration => {
			let values: Vec<Duration> = data
				.chunks_exact(16)
				.map(|chunk| {
					let months = i32::from_le_bytes(chunk[..4].try_into().unwrap());
					let days = i32::from_le_bytes(chunk[4..8].try_into().unwrap());
					let nanos = i64::from_le_bytes(chunk[8..16].try_into().unwrap());
					Duration::new(months, days, nanos).unwrap()
				})
				.collect();
			FrameColumnData::Duration(TemporalContainer::new(values))
		}
		Type::IdentityId => {
			let values: Vec<IdentityId> = data
				.chunks_exact(16)
				.map(|chunk| {
					let uuid = Uuid::from_bytes(chunk.try_into().unwrap());
					IdentityId(Uuid7(uuid))
				})
				.collect();
			FrameColumnData::IdentityId(IdentityIdContainer::new(values))
		}
		Type::Uuid4 => {
			let values: Vec<Uuid4> = data
				.chunks_exact(16)
				.map(|chunk| {
					let uuid = Uuid::from_bytes(chunk.try_into().unwrap());
					Uuid4(uuid)
				})
				.collect();
			FrameColumnData::Uuid4(UuidContainer::new(values))
		}
		Type::Uuid7 => {
			let values: Vec<Uuid7> = data
				.chunks_exact(16)
				.map(|chunk| {
					let uuid = Uuid::from_bytes(chunk.try_into().unwrap());
					Uuid7(uuid)
				})
				.collect();
			FrameColumnData::Uuid7(UuidContainer::new(values))
		}
		Type::Blob => {
			let mut values = Vec::new();
			let mut pos = 0;
			while pos + 4 <= data.len() {
				let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
				pos += 4;
				let bytes = data[pos..pos + len].to_vec();
				pos += len;
				values.push(Blob::new(bytes));
			}
			FrameColumnData::Blob(BlobContainer::new(values))
		}
		Type::Int => {
			let mut values = Vec::new();
			let mut pos = 0;
			while pos + 4 <= data.len() {
				let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
				pos += 4;
				let bytes = &data[pos..pos + len];
				pos += len;
				values.push(Int(BigInt::from_signed_bytes_le(bytes)));
			}
			FrameColumnData::Int(NumberContainer::new(values))
		}
		Type::Uint => {
			let mut values = Vec::new();
			let mut pos = 0;
			while pos + 4 <= data.len() {
				let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
				pos += 4;
				let bytes = &data[pos..pos + len];
				pos += len;
				values.push(Uint(BigInt::from_signed_bytes_le(bytes)));
			}
			FrameColumnData::Uint(NumberContainer::new(values))
		}
		Type::Decimal => {
			let strings = decode_length_prefixed_strings(data);
			let values: Vec<Decimal> = strings
				.into_iter()
				.map(|s| s.parse::<Decimal>().unwrap_or_else(|_| Decimal::from_i64(0)))
				.collect();
			FrameColumnData::Decimal(NumberContainer::new(values))
		}
		Type::Any => {
			let mut values: Vec<Box<Value>> = Vec::new();
			let mut pos = 0;
			while pos < data.len() {
				let (val, consumed) = decode_any_value(&data[pos..]);
				pos += consumed;
				values.push(Box::new(val));
			}
			FrameColumnData::Any(AnyContainer::new(values))
		}
		Type::DictionaryId => {
			// Fallback: store as Utf8 for now (dictionary IDs need context)
			FrameColumnData::Utf8(Utf8Container::new(vec![]))
		}
		Type::List(_) | Type::Record(_) | Type::Tuple(_) => FrameColumnData::Utf8(Utf8Container::new(vec![])),
	}
}

fn decode_bitvec(data: &[u8]) -> BitVec {
	if data.len() < 4 {
		return BitVec::default();
	}
	let num_bits = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
	let byte_count = num_bits.div_ceil(8);
	let bits = data[4..4 + byte_count].to_vec();
	BitVec::from_raw(bits, num_bits)
}

fn decode_length_prefixed_strings(data: &[u8]) -> Vec<String> {
	let mut values = Vec::new();
	let mut pos = 0;
	while pos + 4 <= data.len() {
		let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
		pos += 4;
		let s = String::from_utf8_lossy(&data[pos..pos + len]).into_owned();
		pos += len;
		values.push(s);
	}
	values
}

fn decode_any_value(data: &[u8]) -> (Value, usize) {
	let type_tag = data[0];
	let ty = Type::from_u8(type_tag);
	let mut pos = 1;

	match ty {
		Type::Option(inner) => {
			// None value — the type tag has 0x80 set
			(
				Value::None {
					inner: *inner,
				},
				pos,
			)
		}
		Type::Boolean => {
			let v = data[pos] != 0;
			(Value::Boolean(v), pos + 1)
		}
		Type::Float4 => {
			let v = f32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
			(Value::float4(v), pos + 4)
		}
		Type::Float8 => {
			let v = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
			(Value::float8(v), pos + 8)
		}
		Type::Int1 => {
			let v = data[pos] as i8;
			(Value::Int1(v), pos + 1)
		}
		Type::Int2 => {
			let v = i16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
			(Value::Int2(v), pos + 2)
		}
		Type::Int4 => {
			let v = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
			(Value::Int4(v), pos + 4)
		}
		Type::Int8 => {
			let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
			(Value::Int8(v), pos + 8)
		}
		Type::Int16 => {
			let v = i128::from_le_bytes(data[pos..pos + 16].try_into().unwrap());
			(Value::Int16(v), pos + 16)
		}
		Type::Uint1 => {
			let v = data[pos];
			(Value::Uint1(v), pos + 1)
		}
		Type::Uint2 => {
			let v = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
			(Value::Uint2(v), pos + 2)
		}
		Type::Uint4 => {
			let v = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
			(Value::Uint4(v), pos + 4)
		}
		Type::Uint8 => {
			let v = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
			(Value::Uint8(v), pos + 8)
		}
		Type::Uint16 => {
			let v = u128::from_le_bytes(data[pos..pos + 16].try_into().unwrap());
			(Value::Uint16(v), pos + 16)
		}
		Type::Utf8 => {
			let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;
			let s = String::from_utf8_lossy(&data[pos..pos + len]).into_owned();
			(Value::Utf8(s), pos + len)
		}
		Type::Date => {
			let days = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
			let d = Date::from_days_since_epoch(days)
				.unwrap_or_else(|| Date::from_ymd(1970, 1, 1).unwrap());
			(Value::Date(d), pos + 4)
		}
		Type::DateTime => {
			let nanos = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
			let dt = DateTime::from_nanos(nanos);
			(Value::DateTime(dt), pos + 8)
		}
		Type::Time => {
			let nanos = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
			let t = Time::from_nanos_since_midnight(nanos)
				.unwrap_or_else(|| Time::from_hms(0, 0, 0).unwrap());
			(Value::Time(t), pos + 8)
		}
		Type::Duration => {
			let months = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
			let days = i32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
			let nanos = i64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
			(Value::Duration(Duration::new(months, days, nanos).unwrap()), pos + 16)
		}
		Type::IdentityId => {
			let uuid = Uuid::from_bytes(data[pos..pos + 16].try_into().unwrap());
			(Value::IdentityId(IdentityId(Uuid7(uuid))), pos + 16)
		}
		Type::Uuid4 => {
			let uuid = Uuid::from_bytes(data[pos..pos + 16].try_into().unwrap());
			(Value::Uuid4(Uuid4(uuid)), pos + 16)
		}
		Type::Uuid7 => {
			let uuid = Uuid::from_bytes(data[pos..pos + 16].try_into().unwrap());
			(Value::Uuid7(Uuid7(uuid)), pos + 16)
		}
		Type::Blob => {
			let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;
			let bytes = data[pos..pos + len].to_vec();
			(Value::Blob(Blob::new(bytes)), pos + len)
		}
		Type::Int => {
			let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;
			let bytes = &data[pos..pos + len];
			(Value::Int(Int(BigInt::from_signed_bytes_le(bytes))), pos + len)
		}
		Type::Uint => {
			let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;
			let bytes = &data[pos..pos + len];
			(Value::Uint(Uint(BigInt::from_signed_bytes_le(bytes))), pos + len)
		}
		Type::Decimal => {
			let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;
			let s = String::from_utf8_lossy(&data[pos..pos + len]).into_owned();
			let d = s.parse::<Decimal>().unwrap_or_else(|_| Decimal::from_i64(0));
			(Value::Decimal(d), pos + len)
		}
		Type::Any => {
			// Any wraps another value — recursively decode the inner
			let (inner_val, consumed) = decode_any_value(&data[pos..]);
			(Value::Any(Box::new(inner_val)), pos + consumed)
		}
		Type::DictionaryId | Type::List(_) | Type::Record(_) | Type::Tuple(_) => {
			// Shouldn't be nested in Any but handle gracefully
			(
				Value::None {
					inner: ty,
				},
				pos,
			)
		}
	}
}

fn status_to_error(status: Status) -> Error {
	if let Ok(diag) = serde_json_from_str::<Diagnostic>(status.message()) {
		return Error(Box::new(diag));
	}
	Error(Box::new(Diagnostic {
		code: format!("GRPC_{:?}", status.code()),
		message: status.message().to_string(),
		..Default::default()
	}))
}
