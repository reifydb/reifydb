// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_sub_server::{format::WireFormat, wire::WireParams};
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
pub struct SubscribeRequest {
	pub rql: String,

	pub params: Option<WireParams>,

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

	pub params: Option<Vec<Option<WireParams>>>,

	#[serde(default)]
	pub format: WireFormat,
}

impl BatchSubscribeRequest {
	pub fn into_queries(self) -> Result<Vec<(String, Params)>, String> {
		let Some(params) = self.params else {
			return Ok(self.queries.into_iter().map(|rql| (rql, Params::None)).collect());
		};
		if params.len() != self.queries.len() {
			return Err(format!(
				"expected one params entry per query, got {} params for {} queries",
				params.len(),
				self.queries.len()
			));
		}
		self.queries
			.into_iter()
			.zip(params)
			.enumerate()
			.map(|(index, (rql, wire))| match wire {
				None => Ok((rql, Params::None)),
				Some(wire) => wire
					.into_params()
					.map(|params| (rql, params))
					.map_err(|e| format!("query {index}: {e}")),
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

	use super::{BatchSubscribeRequest, Request, RequestPayload};

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
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"queries":["from a","from b"],"params":[{"monitor_id":{"type":{"id":"Utf8"},"value":"m-1"}},{"monitor_id":{"type":{"id":"Utf8"},"value":"m-2"}}]}}"#,
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
	fn a_null_entry_runs_its_query_without_params() {
		// A member without params must never borrow a neighbour's, or its filter binds a foreign value.
		let queries = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"queries":["from a","from b"],"params":[null,[{"type":{"id":"Int4"},"value":"7"}]]}}"#,
		)
		.into_queries()
		.unwrap();

		assert_eq!(queries[0].1, Params::None);
		assert_eq!(queries[1].1.get_positional(0), Some(&Value::Int4(7)));
	}

	#[test]
	fn a_request_without_params_runs_every_query_without_params() {
		// A client that sends no params field must keep working, otherwise every older batch is refused.
		let queries =
			batch(r#"{"id":"b-1","type":"BatchSubscribe","payload":{"queries":["from a","from b"]}}"#)
				.into_queries()
				.unwrap();

		assert_eq!(queries, vec![("from a".to_string(), Params::None), ("from b".to_string(), Params::None)]);
	}

	#[test]
	fn a_params_list_that_does_not_match_the_queries_is_refused() {
		// A short params list must be refused, otherwise the unmatched queries silently run without params.
		let err = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"queries":["from a","from b"],"params":[null]}}"#,
		)
		.into_queries()
		.unwrap_err();

		assert_eq!(err, "expected one params entry per query, got 1 params for 2 queries");
	}

	#[test]
	fn an_invalid_param_names_the_query_it_belongs_to() {
		// Without the index the client cannot tell which member of the batch carried the bad value.
		let err = batch(
			r#"{"id":"b-1","type":"BatchSubscribe","payload":{"queries":["from a","from b"],"params":[null,{"n":{"type":{"id":"Int4"},"value":"abc"}}]}}"#,
		)
		.into_queries()
		.unwrap_err();

		assert_eq!(err, "query 1: parameter $n: invalid data: cannot parse 'abc' as Int4");
	}
}
