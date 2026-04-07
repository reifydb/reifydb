// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::event::{EventBus, metric::RequestExecutedEvent};
use reifydb_metric::{accumulator::StatementStatsAccumulator, registry::MetricRegistry};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};

use crate::{
	actor::MetricCollectorActor, interceptor::RequestMetricsInterceptor, listener::RequestMetricsEventListener,
};

pub fn spawn_metric_collector(
	actor_system: &ActorSystem,
	event_bus: &EventBus,
	registry: Arc<MetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	clock: Clock,
) -> RequestMetricsInterceptor {
	let actor = MetricCollectorActor::new(registry, accumulator.clone());
	let handle = actor_system.spawn("metric-collector", actor);

	let listener = RequestMetricsEventListener::new(handle.actor_ref().clone());
	event_bus.register::<RequestExecutedEvent, _>(listener);

	RequestMetricsInterceptor::new(event_bus.clone(), accumulator, clock)
}
