// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_sub_server::{format::WireFormat, wire::WireParams};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

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
	Logout,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	pub rql: String,

	pub params: Option<WireParams>,

	#[serde(default)]
	pub format: WireFormat,

	pub unwrap: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
	pub method: Option<String>,
	pub credentials: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub rql: String,

	pub params: Option<WireParams>,

	#[serde(default)]
	pub format: WireFormat,

	pub unwrap: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub rql: String,

	pub params: Option<WireParams>,

	#[serde(default)]
	pub format: WireFormat,

	pub unwrap: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	pub rql: String,

	#[serde(default)]
	pub format: WireFormat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	pub subscription_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSubscribeRequest {
	pub queries: Vec<String>,

	#[serde(default)]
	pub format: WireFormat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUnsubscribeRequest {
	pub batch_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CallRequest {
	pub name: String,

	pub params: Option<WireParams>,
}
