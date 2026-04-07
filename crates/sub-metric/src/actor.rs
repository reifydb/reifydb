// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::event::metric::RequestExecutedEvent;
use reifydb_metric::{accumulator::StatementStatsAccumulator, registry::MetricRegistry};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_type::value::datetime::DateTime;

#[derive(Debug, Clone)]
pub enum MetricMsg {
	Tick(DateTime),
	RequestExecuted(RequestExecutedEvent),
}

pub struct MetricCollectorActor {
	registry: Arc<MetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	flush_interval: Duration,
}

impl MetricCollectorActor {
	pub fn new(registry: Arc<MetricRegistry>, accumulator: Arc<StatementStatsAccumulator>) -> Self {
		Self {
			registry,
			accumulator,
			flush_interval: Duration::from_secs(10),
		}
	}
}

impl Actor for MetricCollectorActor {
	type Message = MetricMsg;
	type State = ();

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.flush_interval, |nanos| MetricMsg::Tick(DateTime::from_nanos(nanos)));
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricMsg::Tick(_) => {
				let _registry_snap = self.registry.snapshot();
				let _acc_snap = self.accumulator.snapshot();
				// Future: Write these snapshots directly to `system::metrics::*` series.
			}
			MetricMsg::RequestExecuted(_event) => {
				// Buffer the event for writing to the request history series.
			}
		}
		Directive::Continue
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		event::metric::{Request, RequestExecutedEvent},
		fingerprint::{RequestFingerprint, StatementFingerprint},
		metric::StatementMetric,
	};
	use reifydb_metric::{accumulator::StatementStatsAccumulator, registry::MetricRegistry};
	use reifydb_runtime::actor::system::ActorSystem;
	use reifydb_type::value::{datetime::DateTime, duration::Duration};

	use super::{MetricCollectorActor, MetricMsg};

	#[test]
	fn test_actor_handles_messages() {
		let system = ActorSystem::new(1);
		let registry = Arc::new(MetricRegistry::new());
		let accumulator = Arc::new(StatementStatsAccumulator::new());

		let actor = MetricCollectorActor::new(registry, accumulator);
		let handle = system.spawn("test-metric-actor", actor);
		let actor_ref = handle.actor_ref().clone();

		// Send a Tick message
		actor_ref.send(MetricMsg::Tick(DateTime::from_nanos(0))).unwrap();

		// Send a RequestExecuted message
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
		actor_ref.send(MetricMsg::RequestExecuted(event)).unwrap();

		// Cleanup
		drop(actor_ref);
		system.shutdown();
		handle.join().unwrap();
	}
}
