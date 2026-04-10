// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	event::{EventBus, metric::RequestExecutedEvent},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metric::{accumulator::StatementStatsAccumulator, registry::MetricRegistry};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{actor::MetricCollectorActor, listener::RequestMetricsEventListener, subsystem::MetricSubsystem};

pub struct MetricSubsystemFactory {
	registry: Arc<MetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
}

impl MetricSubsystemFactory {
	pub fn new(registry: Arc<MetricRegistry>, accumulator: Arc<StatementStatsAccumulator>) -> Self {
		Self {
			registry,
			accumulator,
		}
	}
}

impl SubsystemFactory for MetricSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let runtime = ioc.resolve::<SharedRuntime>()?;
		let engine = ioc.resolve::<StandardEngine>()?;
		let event_bus = ioc.resolve::<EventBus>()?;
		let actor_system = runtime.actor_system();

		let actor =
			MetricCollectorActor::new(self.registry, self.accumulator, engine.clone(), engine.catalog());
		let handle = actor_system.spawn("metric-collector", actor);

		let listener = RequestMetricsEventListener::new(handle.actor_ref().clone());
		event_bus.register::<RequestExecutedEvent, _>(listener);

		Ok(Box::new(MetricSubsystem::new()))
	}
}
