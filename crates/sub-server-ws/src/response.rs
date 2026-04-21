// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WebSocket response types matching the client protocol.
//!
//! These types mirror the structures in `reifydb-client` to ensure
//! protocol compatibility. Changes to these types should be coordinated
//! with the client implementation.

use std::collections::HashMap;

use reifydb_type::{error::Diagnostic, fragment::Fragment};
use serde::Serialize;
use serde_json::{Value as JsonValue, to_string};

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
	Admin(AdminResponse),
	Command(CommandResponse),
	Query(QueryResponse),
	CallOperation(CallOperationResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
	BatchSubscribed(BatchSubscribedResponse),
	BatchUnsubscribed(BatchUnsubscribedResponse),
	Logout(LogoutResponse),
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
	/// Authentication status: "ok" for token validation, "authenticated" for login, "challenge" for multi-step.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub status: Option<String>,
	/// Session token (present when login succeeds).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	/// Identity ID (present when login succeeds).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub identity: Option<String>,
	/// Challenge ID (present for multi-step auth).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub challenge_id: Option<String>,
	/// Challenge payload (present for multi-step auth).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize)]
pub struct ResponseMeta {
	pub fingerprint: String,
	pub duration: String,
}

#[derive(Debug, Serialize)]
pub struct AdminResponse {
	pub content_type: String,
	pub body: JsonValue,
	pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct CommandResponse {
	pub content_type: String,
	pub body: JsonValue,
	pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
	pub content_type: String,
	pub body: JsonValue,
	pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct CallOperationResponse {
	pub content_type: String,
	pub body: JsonValue,
	pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct SubscribedResponse {
	pub subscription_id: String,
}

#[derive(Debug, Serialize)]
pub struct UnsubscribedResponse {
	pub subscription_id: String,
}

#[derive(Debug, Serialize)]
pub struct BatchSubscribedResponse {
	pub batch_id: String,
	pub members: Vec<BatchMemberInfo>,
}

#[derive(Debug, Serialize)]
pub struct BatchMemberInfo {
	pub index: usize,
	pub subscription_id: String,
}

#[derive(Debug, Serialize)]
pub struct BatchUnsubscribedResponse {
	pub batch_id: String,
}

#[derive(Debug, Serialize)]
pub struct LogoutResponse {
	pub status: String,
}

/// Server-initiated push message (matches client's `ServerPush`)
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(ChangePayload),
	BatchChange(BatchChangePayload),
	BatchMemberClosed(BatchMemberClosedPayload),
	BatchClosed(BatchClosedPayload),
}

/// Change notification payload
#[derive(Debug, Serialize)]
pub struct ChangePayload {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
}

/// Batch-change push payload — one envelope containing deltas from N members.
#[derive(Debug, Serialize)]
pub struct BatchChangePayload {
	pub batch_id: String,
	pub entries: Vec<BatchChangeEntry>,
}

/// Per-source entry inside a BatchChange envelope.
#[derive(Debug, Serialize)]
pub struct BatchChangeEntry {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
}

/// One batch member has been closed (upstream ended); remaining members stay alive.
#[derive(Debug, Serialize)]
pub struct BatchMemberClosedPayload {
	pub batch_id: String,
	pub subscription_id: String,
}

/// The entire batch has been closed.
#[derive(Debug, Serialize)]
pub struct BatchClosedPayload {
	pub batch_id: String,
}

impl Response {
	pub fn auth(id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Auth(AuthResponse {
				status: None,
				token: None,
				identity: None,
				challenge_id: None,
				payload: None,
			}),
		}
	}

	pub fn auth_authenticated(id: impl Into<String>, token: String, identity: String) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Auth(AuthResponse {
				status: Some("authenticated".to_string()),
				token: Some(token),
				identity: Some(identity),
				challenge_id: None,
				payload: None,
			}),
		}
	}

	pub fn auth_challenge(id: impl Into<String>, challenge_id: String, payload: HashMap<String, String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Auth(AuthResponse {
				status: Some("challenge".to_string()),
				token: None,
				identity: None,
				challenge_id: Some(challenge_id),
				payload: Some(payload),
			}),
		}
	}

	pub fn admin(
		id: impl Into<String>,
		content_type: impl Into<String>,
		body: JsonValue,
		meta: ResponseMeta,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Admin(AdminResponse {
				content_type: content_type.into(),
				body,
				meta,
			}),
		}
	}

	pub fn query(
		id: impl Into<String>,
		content_type: impl Into<String>,
		body: JsonValue,
		meta: ResponseMeta,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Query(QueryResponse {
				content_type: content_type.into(),
				body,
				meta,
			}),
		}
	}

	pub fn command(
		id: impl Into<String>,
		content_type: impl Into<String>,
		body: JsonValue,
		meta: ResponseMeta,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Command(CommandResponse {
				content_type: content_type.into(),
				body,
				meta,
			}),
		}
	}

	pub fn call_operation(
		id: impl Into<String>,
		content_type: impl Into<String>,
		body: JsonValue,
		meta: ResponseMeta,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::CallOperation(CallOperationResponse {
				content_type: content_type.into(),
				body,
				meta,
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

	pub fn batch_subscribed(
		id: impl Into<String>,
		batch_id: impl Into<String>,
		members: Vec<BatchMemberInfo>,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::BatchSubscribed(BatchSubscribedResponse {
				batch_id: batch_id.into(),
				members,
			}),
		}
	}

	pub fn batch_unsubscribed(id: impl Into<String>, batch_id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::BatchUnsubscribed(BatchUnsubscribedResponse {
				batch_id: batch_id.into(),
			}),
		}
	}

	pub fn logout(id: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Logout(LogoutResponse {
				status: "ok".to_string(),
			}),
		}
	}

	pub fn internal_error(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Err(ErrResponse {
				diagnostic: Diagnostic {
					code: code.into(),
					rql: None,
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

	/// Create an error response for a rejected request (auth failure, rate limit, etc.).
	pub fn rejected(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::internal_error(id, code, message)
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
		to_string(self).expect("Failed to serialize Response")
	}
}

impl ServerPush {
	pub fn change(subscription_id: impl Into<String>, content_type: impl Into<String>, body: JsonValue) -> Self {
		Self::Change(ChangePayload {
			subscription_id: subscription_id.into(),
			content_type: content_type.into(),
			body,
		})
	}

	pub fn batch_change(batch_id: impl Into<String>, entries: Vec<BatchChangeEntry>) -> Self {
		Self::BatchChange(BatchChangePayload {
			batch_id: batch_id.into(),
			entries,
		})
	}

	pub fn batch_member_closed(batch_id: impl Into<String>, subscription_id: impl Into<String>) -> Self {
		Self::BatchMemberClosed(BatchMemberClosedPayload {
			batch_id: batch_id.into(),
			subscription_id: subscription_id.into(),
		})
	}

	pub fn batch_closed(batch_id: impl Into<String>) -> Self {
		Self::BatchClosed(BatchClosedPayload {
			batch_id: batch_id.into(),
		})
	}

	pub fn to_json(&self) -> String {
		to_string(self).expect("Failed to serialize ServerPush")
	}
}
