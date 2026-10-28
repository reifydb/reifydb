// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_value::{error::Diagnostic, fragment::Fragment};
use serde::Serialize;
use serde_json::{Value as JsonValue, to_string};

#[derive(Debug, Serialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
	Auth(AuthResponse),
	Err(ErrResponse),
	Admin(AdminResponse),
	Command(CommandResponse),
	Query(QueryResponse),
	Call(CallResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
	Logout(LogoutResponse),
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub status: Option<String>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub identity: Option<String>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub challenge_id: Option<String>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub index: Option<usize>,
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
pub struct CallResponse {
	pub content_type: String,
	pub body: JsonValue,
	pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct SubscribedResponse {
	pub subscriptions: Vec<SubscriptionInfo>,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionInfo {
	pub index: usize,
	pub subscription_id: String,
}

#[derive(Debug, Serialize)]
pub struct UnsubscribedResponse {
	pub subscription_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LogoutResponse {
	pub status: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(ChangePayload),
	SubscriptionClosed(SubscriptionClosedPayload),
}

#[derive(Debug, Serialize)]
pub struct ChangePayload {
	pub entries: Vec<ChangeEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangeEntry {
	pub subscription_id: String,
	pub content_type: String,
	pub body: JsonValue,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionClosedPayload {
	pub subscription_id: String,
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

	pub fn call(
		id: impl Into<String>,
		content_type: impl Into<String>,
		body: JsonValue,
		meta: ResponseMeta,
	) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Call(CallResponse {
				content_type: content_type.into(),
				body,
				meta,
			}),
		}
	}

	pub fn subscribed(id: impl Into<String>, subscriptions: Vec<SubscriptionInfo>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Subscribed(SubscribedResponse {
				subscriptions,
			}),
		}
	}

	pub fn unsubscribed(id: impl Into<String>, subscription_ids: Vec<String>) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Unsubscribed(UnsubscribedResponse {
				subscription_ids,
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
				index: None,
			}),
		}
	}

	pub fn rejected(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::internal_error(id, code, message)
	}

	pub fn error(id: impl Into<String>, diagnostic: Diagnostic) -> Self {
		Self {
			id: id.into(),
			payload: ResponsePayload::Err(ErrResponse {
				diagnostic,
				index: None,
			}),
		}
	}

	pub fn with_index(mut self, index: usize) -> Self {
		if let ResponsePayload::Err(err) = &mut self.payload {
			err.index = Some(index);
		}
		self
	}

	pub fn to_json(&self) -> String {
		to_string(self).expect("Failed to serialize Response")
	}
}

impl ServerPush {
	pub fn change(entries: Vec<ChangeEntry>) -> Self {
		Self::Change(ChangePayload {
			entries,
		})
	}

	pub fn subscription_closed(subscription_id: impl Into<String>) -> Self {
		Self::SubscriptionClosed(SubscriptionClosedPayload {
			subscription_id: subscription_id.into(),
		})
	}

	pub fn to_json(&self) -> String {
		to_string(self).expect("Failed to serialize ServerPush")
	}
}

#[cfg(test)]
mod tests {
	use serde_json::{Value as JsonValue, from_str, json};

	use super::{ChangeEntry, Response, ServerPush, SubscriptionInfo};

	fn parsed(text: &str) -> JsonValue {
		from_str(text).expect("the push must be valid json")
	}

	fn entry(subscription_id: &str, body: JsonValue) -> ChangeEntry {
		ChangeEntry {
			subscription_id: subscription_id.to_string(),
			content_type: "application/vnd.reifydb.frames+json".to_string(),
			body,
		}
	}

	#[test]
	fn a_change_push_carries_every_entry_and_no_batch_id() {
		// Dropping an entry or reviving a batch id breaks every client that routes entries by subscription id.
		let push = ServerPush::change(vec![entry("7", json!({"a": 1})), entry("9", json!({"b": 2}))]);

		assert_eq!(
			parsed(&push.to_json()),
			json!({"type": "Change", "payload": {"entries": [
				{"subscription_id": "7", "content_type": "application/vnd.reifydb.frames+json", "body": {"a": 1}},
				{"subscription_id": "9", "content_type": "application/vnd.reifydb.frames+json", "body": {"b": 2}}
			]}})
		);
	}

	#[test]
	fn subscribed_lists_each_index_with_its_own_id() {
		// Swapping index and id, or flattening to ids, maps a client's items to the wrong subscriptions.
		let response = Response::subscribed(
			"r",
			vec![
				SubscriptionInfo {
					index: 0,
					subscription_id: "7".to_string(),
				},
				SubscriptionInfo {
					index: 1,
					subscription_id: "9".to_string(),
				},
			],
		);

		assert_eq!(
			parsed(&response.to_json()),
			json!({"id": "r", "type": "Subscribed", "payload": {"subscriptions": [
				{"index": 0, "subscription_id": "7"},
				{"index": 1, "subscription_id": "9"}
			]}})
		);
	}

	#[test]
	fn a_subscription_closed_push_names_only_its_subscription() {
		// An extra field or a wrong tag makes a client close nothing, or close more than the one subscription.
		assert_eq!(
			parsed(&ServerPush::subscription_closed("7").to_json()),
			json!({"type": "SubscriptionClosed", "payload": {"subscription_id": "7"}})
		);
	}

	#[test]
	fn an_err_frame_for_a_failed_item_carries_its_index() {
		// Without the index a client cannot tell which item of its call failed.
		let response = Response::internal_error("s", "X", "bad").with_index(1);

		assert_eq!(parsed(&response.to_json())["payload"]["index"], json!(1));
	}

	#[test]
	fn an_err_frame_for_a_whole_call_failure_has_no_index() {
		// An index of none must be left out, never sent as a value a client reads as item 0 or null.
		let payload = &parsed(&Response::internal_error("s", "X", "bad").to_json())["payload"];

		assert!(payload.get("index").is_none(), "{payload}");
	}
}
