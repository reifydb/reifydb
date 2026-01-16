// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebSocket response types matching the client protocol.
//!
//! These types mirror the structures in `reifydb-client` to ensure
//! protocol compatibility. Changes to these types should be coordinated
//! with the client implementation.

use reifydb_sub_server::response::ResponseFrame;
use reifydb_type::{error::diagnostic::Diagnostic, fragment::Fragment};
use serde::Serialize;

/// WebSocket response envelope (matches client's `Response`)
#[derive(Debug, Serialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

/// Response payload variants (matches client's `ResponsePayload`)
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
	Auth(AuthResponse),
	Err(ErrResponse),
	Command(CommandResponse),
	Query(QueryResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {}

#[derive(Debug, Serialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize)]
pub struct CommandResponse {
	pub frames: Vec<ResponseFrame>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
	pub frames: Vec<ResponseFrame>,
}

#[derive(Debug, Serialize)]
pub struct SubscribedResponse {
	pub subscription_id: String,
}

#[derive(Debug, Serialize)]
pub struct UnsubscribedResponse {
	pub subscription_id: String,
}

/// Server-initiated push message (matches client's `ServerPush`)
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(ChangePayload),
}

/// Change notification payload
#[derive(Debug, Serialize)]
pub struct ChangePayload {
	pub subscription_id: String,
	pub frame: ResponseFrame,
}

impl Response {
	pub fn auth(id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Auth(AuthResponse {}),
		}
	}

	pub fn query(id: impl Into<String>, frames: Vec<ResponseFrame>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Query(QueryResponse {
				frames,
			}),
		}
	}

	pub fn command(id: impl Into<String>, frames: Vec<ResponseFrame>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Command(CommandResponse {
				frames,
			}),
		}
	}

	pub fn subscribed(id: impl Into<String>, subscription_id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Subscribed(SubscribedResponse {
				subscription_id: subscription_id.into(),
			}),
		}
	}

	pub fn unsubscribed(id: impl Into<String>, subscription_id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Unsubscribed(UnsubscribedResponse {
				subscription_id: subscription_id.into(),
			}),
		}
	}

	pub fn internal_error(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Err(ErrResponse {
				diagnostic: Diagnostic {
					code: code.into(),
					statement: None,
					message: message.into(),
					column: None,
					fragment: Fragment::None,
					label: None,
					help: None,
					notes: Vec::new(),
					cause: None,
					operator_chain: None,
				},
			}),
		}
	}

	pub fn error(id: impl Into<String>, diagnostic: Diagnostic) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Err(ErrResponse {
				diagnostic,
			}),
		}
	}

	pub fn to_json(&self) -> String {
		serde_json::to_string(self).expect("Failed to serialize Response")
	}
}

impl ServerPush {
	pub fn change(subscription_id: impl Into<String>, frame: ResponseFrame) -> Self {
		Self::Change(ChangePayload {
			subscription_id: subscription_id.into(),
			frame,
		})
	}

	pub fn to_json(&self) -> String {
		serde_json::to_string(self).expect("Failed to serialize ServerPush")
	}
}
