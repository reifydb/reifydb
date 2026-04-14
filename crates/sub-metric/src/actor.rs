// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[allow(unused_imports)]
use std::{mem, sync::Arc, time::Duration as StdDuration};

use reifydb_catalog::catalog::Catalog;
#[allow(unused_imports)]
use reifydb_core::{
	actors::metric::MetricMessage,
	encoded::shape::RowShape,
	event::metric::{Request, RequestExecutedEvent},
	interface::catalog::ringbuffer::{RingBuffer, RingBufferMetadata},
	key::row::RowKey,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, SystemMetricRegistry},
};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_type::value::datetime::DateTime;

#[allow(dead_code)]
pub struct MetricCollectorActor {
	registry: Arc<MetricRegistry>,
	system_registry: Arc<SystemMetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	engine: StandardEngine,
	catalog: Catalog,
	flush_interval: StdDuration,
}

impl MetricCollectorActor {
	pub fn new(
		registry: Arc<MetricRegistry>,
		system_registry: Arc<SystemMetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
		engine: StandardEngine,
		catalog: Catalog,
	) -> Self {
		Self {
			registry,
			system_registry,
			accumulator,
			engine,
			catalog,
			flush_interval: StdDuration::from_secs(10),
		}
	}
}

#[allow(dead_code)]
pub struct MetricActorState {
	request_history_rb: Option<(RingBuffer, RowShape)>,
	statement_stats_rb: Option<(RingBuffer, RowShape)>,
	pending: Vec<RequestExecutedEvent>,
}

impl Actor for MetricCollectorActor {
	type Message = MetricMessage;
	type State = MetricActorState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.flush_interval, |nanos| MetricMessage::Tick(DateTime::from_nanos(nanos)));

		MetricActorState {
			request_history_rb: None,
			statement_stats_rb: None,
			pending: Vec::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricMessage::Tick(_) => {
				let _ = mem::take(&mut state.pending);
			}
			MetricMessage::RequestExecuted(event) => {
				state.pending.push(event);
			}
		}
		Directive::Continue
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		actors::metric::MetricMessage,
		event::metric::{Request, RequestExecutedEvent},
		fingerprint::{RequestFingerprint, StatementFingerprint},
		metric::StatementMetric,
	};
	use reifydb_type::value::{datetime::DateTime, duration::Duration};

	#[test]
	fn test_metric_message_construction() {
		let event = RequestExecutedEvent::new(
			Request::Query {
				fingerprint: RequestFingerprint::default(),
				statements: vec![StatementMetric {
					fingerprint: StatementFingerprint::new(1),
					normalized_rql: "From x".to_string(),
					compile_duration_us: 0,
					execute_duration_us: 0,
					rows_affected: 1,
				}],
			},
			Duration::from_microseconds(100).unwrap(),
			Duration::from_microseconds(50).unwrap(),
			true,
			DateTime::from_timestamp_millis(1000).unwrap(),
		);

		let _tick = MetricMessage::Tick(DateTime::from_nanos(0));
		let _req = MetricMessage::RequestExecuted(event);
	}
}
