// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::subscription::{HydrationConfig, SubscribeOptions};
use reifydb_sub_server::{
	format::WireFormat,
	wire::{WireParams, WireValue},
};
use reifydb_value::params::Params;
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
	QueueClaim(QueueClaimRequest),
	Logout,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueClaimRequest {
	pub queue: String,

	pub worker: String,

	pub max_n: Option<u32>,

	pub lease_ttl: Option<String>,

	pub wait_for: Option<String>,

	#[serde(default)]
	pub format: WireFormat,

	pub unwrap: Option<bool>,
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
pub struct WireHydrationOptions {
	pub enabled: Option<bool>,

	pub max_rows: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WireSubscribeOptions {
	pub hydration: Option<WireHydrationOptions>,

	pub throttle: Option<WireValue>,

	pub linger: Option<WireValue>,
}

impl WireSubscribeOptions {
	pub fn into_options(self) -> Result<SubscribeOptions, String> {
		Ok(SubscribeOptions {
			hydration: self.hydration.map_or_else(HydrationConfig::default, |hydration| HydrationConfig {
				enabled: hydration.enabled.unwrap_or(HydrationConfig::default().enabled),
				max_rows: hydration.max_rows,
			}),
			throttle: self.throttle.map(|throttle| throttle.into_duration("throttle")).transpose()?,
			linger: self.linger.map(|linger| linger.into_duration("linger")).transpose()?,
		})
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	pub rql: String,

	pub params: Option<WireParams>,

	pub options: Option<WireSubscribeOptions>,

	#[serde(default)]
	pub format: WireFormat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	pub subscription_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WireBatchSubscribeItem {
	pub rql: String,

	pub params: Option<WireParams>,

	pub options: Option<WireSubscribeOptions>,
}

#[derive(Debug, PartialEq)]
pub enum SubscribeDecodeError {
	InvalidParams(String),
	InvalidOptions(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSubscribeRequest {
	pub subscriptions: Vec<WireBatchSubscribeItem>,

	#[serde(default)]
	pub format: WireFormat,
}

impl BatchSubscribeRequest {
	pub fn into_queries(self) -> Result<Vec<(String, Params, SubscribeOptions)>, SubscribeDecodeError> {
		self.subscriptions
			.into_iter()
			.enumerate()
			.map(|(index, subscription)| {
				let params = match subscription.params {
					None => Params::None,
					Some(wire) => wire.into_params().map_err(|e| {
						SubscribeDecodeError::InvalidParams(format!(
							"subscription {index}: {e}"
						))
					})?,
				};
				let options = match subscription.options {
					None => SubscribeOptions::default(),
					Some(wire) => wire.into_options().map_err(|e| {
						SubscribeDecodeError::InvalidOptions(format!(
							"subscription {index}: {e}"
						))
					})?,
				};
				Ok((subscription.rql, params, options))
			})
			.collect()
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUnsubscribeRequest {
	pub batch_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CallRequest {
	pub name: String,

	pub params: Option<WireParams>,

	#[serde(default)]
	pub format: Option<WireFormat>,
}

#[cfg(test)]
mod tests {
	use reifydb_value::{params::Params, value::Value};
	use serde_json::from_str;

	use super::{BatchSubscribeRequest, Request, RequestPayload, SubscribeDecodeError};

	fn batch(json: &str) -> BatchSubscribeRequest {
		match from_str::<Request>(json).expect("a batch subscribe request should deserialise").payload {
			RequestPayload::BatchSubscribe(req) => req,
			other => panic!("expected a batch subscribe payload, got {other:?}"),
		}
	}

	#[test]
	fn each_query_runs_with_the_params_at_its_own_position() {
		// Each query must bind the params at its own index, or a subscription runs with another's id.
		let queries = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"subscriptions":[{"rql":"from a","params":{"monitor_id":{"type":{"id":"Utf8"},"value":"m-1"}}},{"rql":"from b","params":{"monitor_id":{"type":{"id":"Utf8"},"value":"m-2"}}}]}}"#,
		)
		.into_queries()
		.unwrap();

		assert_eq!(queries.len(), 2);
		assert_eq!(queries[0].0, "from a");
		assert_eq!(queries[0].1.get_named("monitor_id"), Some(&Value::Utf8("m-1".to_string())));
		assert_eq!(queries[1].0, "from b");
		assert_eq!(queries[1].1.get_named("monitor_id"), Some(&Value::Utf8("m-2".to_string())));
	}

	#[test]
	fn an_entry_without_params_runs_its_query_without_params() {
		// A subscription without params must never borrow a neighbour's, or its filter binds a foreign value.
		let queries = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"subscriptions":[{"rql":"from a"},{"rql":"from b","params":[{"type":{"id":"Int4"},"value":"7"}]}]}}"#,
		)
		.into_queries()
		.unwrap();

		assert_eq!(queries[0].1, Params::None);
		assert_eq!(queries[1].1.get_positional(0), Some(&Value::Int4(7)));
	}

	#[test]
	fn a_request_without_params_runs_every_query_without_params() {
		// A client that sends no params field must keep working, otherwise every older batch is refused.
		let queries = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"subscriptions":[{"rql":"from a"},{"rql":"from b"}]}}"#,
		)
		.into_queries()
		.unwrap();

		assert_eq!(queries.len(), 2);
		assert_eq!(queries[0].0, "from a");
		assert_eq!(queries[0].1, Params::None);
		assert_eq!(queries[1].0, "from b");
		assert_eq!(queries[1].1, Params::None);
	}

	#[test]
	fn an_invalid_param_names_the_query_it_belongs_to() {
		// Without the index the client cannot tell which subscription of the batch carried the bad value.
		let err = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"subscriptions":[{"rql":"from a"},{"rql":"from b","params":{"n":{"type":{"id":"Int4"},"value":"abc"}}}]}}"#,
		)
		.into_queries()
		.unwrap_err();

		assert_eq!(
			err,
			SubscribeDecodeError::InvalidParams(
				"subscription 1: parameter $n: invalid data: cannot parse 'abc' as Int4".to_string()
			)
		);
	}
}
