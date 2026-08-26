// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	fragment::Fragment,
	value::{duration::Duration, temporal::parse::duration::parse_duration},
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_N: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireClaimRequest {
	pub queue: String,
	pub worker: String,
	#[serde(default)]
	pub max_n: Option<u32>,
	#[serde(default)]
	pub lease_ttl: Option<String>,
	#[serde(default)]
	pub wait_for: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimRequest {
	pub queue: String,
	pub worker: String,
	pub max_n: u32,
	pub lease_ttl: Option<Duration>,
	pub wait_for: Duration,
}

impl WireClaimRequest {
	pub fn into_claim_request(self) -> Result<ClaimRequest, String> {
		Ok(ClaimRequest {
			queue: self.queue,
			worker: self.worker,
			max_n: self.max_n.unwrap_or(DEFAULT_MAX_N),
			lease_ttl: parse_optional_duration("lease_ttl", self.lease_ttl.as_deref())?,
			wait_for: parse_optional_duration("wait_for", self.wait_for.as_deref())?
				.unwrap_or(Duration::zero()),
		})
	}
}

fn parse_optional_duration(field: &str, raw: Option<&str>) -> Result<Option<Duration>, String> {
	let Some(raw) = raw else {
		return Ok(None);
	};

	parse_duration(Fragment::internal(raw))
		.map(Some)
		.map_err(|err| format!("invalid {field} value '{raw}': {err:?}"))
}

#[cfg(not(reifydb_single_threaded))]
pub mod native {
	use std::{collections::HashMap, sync::Arc};

	use reifydb_core::{
		actors::server::Operation, interface::catalog::id::QueueId, metrics::execution::ExecutionMetrics,
	};
	use reifydb_engine::queue::{lookup::find_queue_id, wake::QueueWakeRegistry};
	use reifydb_runtime::sync::waiter::WaiterHandle;
	use reifydb_value::{
		params::Params,
		value::{Value, duration::Duration, frame::frame::Frame, identity::IdentityId},
	};
	use tokio::{
		sync::oneshot,
		time::{Instant, timeout_at},
	};
	use tracing::instrument;

	use super::ClaimRequest;
	use crate::{
		dispatch::dispatch,
		execute::ExecuteError,
		interceptor::{RequestContext, RequestMetadata},
		state::AppState,
	};

	const CLAIM_WITH_TTL: &str = "CALL queue::claim($worker, $queue, $max_n, $lease_ttl)";

	#[instrument(name = "dispatch::claim", level = "debug", skip_all, fields(queue = %request.queue))]
	pub async fn dispatch_claim(
		state: &AppState,
		identity: IdentityId,
		request: ClaimRequest,
		metadata: RequestMetadata,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		let lease_ttl = request.lease_ttl.unwrap_or_else(|| state.claim_lease_ttl());
		let params = claim_params(&request.queue, &request.worker, request.max_n, lease_ttl);

		let attempt = || RequestContext {
			identity,
			operation: Operation::Command,
			rql: CLAIM_WITH_TTL.to_string(),
			params: params.clone(),
			metadata: metadata.clone(),
		};

		let first = dispatch(state, attempt()).await?;
		let budget = request.wait_for.min(state.claim_wait_max());
		if !budget.is_positive() || claimed_any(&first.0) {
			return Ok(first);
		}

		let Some(queue_id) = find_queue_id(state.engine(), identity, &request.queue) else {
			return Ok(first);
		};
		let registry = state.engine().queue_wake();
		let deadline = Instant::now() + budget.to_std();

		loop {
			let mut guard = ParkGuard::park(&registry, queue_id);

			let attempted = dispatch(state, attempt()).await?;
			if claimed_any(&attempted.0) {
				guard.forward_if_consumed();
				return Ok(attempted);
			}

			if timeout_at(deadline, guard.notified()).await.is_err() {
				return Ok(attempted);
			}
		}
	}

	struct ParkGuard<'a> {
		registry: &'a QueueWakeRegistry,
		queue: QueueId,
		waiter: Arc<WaiterHandle>,
		notified: Option<oneshot::Receiver<()>>,
	}

	impl<'a> ParkGuard<'a> {
		fn park(registry: &'a QueueWakeRegistry, queue: QueueId) -> Self {
			let (sender, receiver) = oneshot::channel();
			let waiter = Arc::new(WaiterHandle::with_callback(Box::new(move || {
				let _ = sender.send(());
			})));
			registry.register(queue, waiter.clone());

			Self {
				registry,
				queue,
				waiter,
				notified: Some(receiver),
			}
		}

		async fn notified(&mut self) {
			if let Some(receiver) = self.notified.take() {
				let _ = receiver.await;
			}
		}

		fn forward_if_consumed(&self) {
			if self.waiter.wait_timeout(Duration::zero()) {
				self.registry.nudge(self.queue, 1);
			}
		}
	}

	impl Drop for ParkGuard<'_> {
		fn drop(&mut self) {
			self.registry.deregister(self.queue, &self.waiter);
		}
	}

	fn claim_params(queue: &str, worker: &str, max_n: u32, lease_ttl: Duration) -> Params {
		Params::Named(Arc::new(HashMap::from_iter([
			("worker".to_string(), Value::Utf8(worker.to_string())),
			("queue".to_string(), Value::Utf8(queue.to_string())),
			("max_n".to_string(), Value::Uint4(max_n)),
			("lease_ttl".to_string(), Value::Duration(lease_ttl)),
		])))
	}

	fn claimed_any(frames: &[Frame]) -> bool {
		frames.iter().any(|frame| frame.row_count() > 0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn wire(wait_for: Option<&str>, lease_ttl: Option<&str>, max_n: Option<u32>) -> WireClaimRequest {
		WireClaimRequest {
			queue: "app::jobs".to_string(),
			worker: "w1".to_string(),
			max_n,
			lease_ttl: lease_ttl.map(str::to_string),
			wait_for: wait_for.map(str::to_string),
		}
	}

	#[test]
	fn test_an_absent_wait_for_is_a_non_blocking_claim() {
		// Every existing caller posts without wait_for and must keep getting an immediate answer.
		// Defaulting to anything positive would silently turn all of them into long-polls.
		let request = wire(None, None, None).into_claim_request().unwrap();

		assert!(request.wait_for.is_zero());
		assert_eq!(request.max_n, 1, "a claim with no max_n asks for a single item");
		assert!(request.lease_ttl.is_none(), "an absent lease ttl must defer to the server default");
	}

	#[test]
	fn test_durations_are_parsed_from_their_rql_literal_form() {
		let request = wire(Some("25s"), Some("2m"), Some(10)).into_claim_request().unwrap();

		assert_eq!(request.wait_for.as_nanos().unwrap(), 25_000_000_000);
		assert_eq!(request.lease_ttl.unwrap().as_nanos().unwrap(), 120_000_000_000);
		assert_eq!(request.max_n, 10);
	}

	#[test]
	fn test_a_malformed_duration_names_the_field_it_came_from() {
		// The two duration fields fail the same way, so an error that does not name the field
		// leaves the caller guessing which of the two they typed wrong.
		let err = wire(Some("soon"), None, None).into_claim_request().unwrap_err();
		assert!(err.contains("wait_for"), "error must name the offending field, got: {err}");

		let err = wire(None, Some("forever"), None).into_claim_request().unwrap_err();
		assert!(err.contains("lease_ttl"), "error must name the offending field, got: {err}");
	}
}
