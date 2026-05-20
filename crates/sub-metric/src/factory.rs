// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	event::{
		EventBus,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, ProfilerSnapshotEvent,
			RequestExecutedEvent,
		},
	},
	interface::catalog::id::NamespaceId,
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_runtime::SharedRuntime;
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;
use tracing::warn;

use crate::{
	actor::MetricCollectorActor,
	listener::{
		CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, ProfilerSnapshotListener,
		RequestMetricsEventListener,
	},
	profiler_vtable::MetricsProfilerCategoriesVTable,
	subsystem::MetricSubsystem,
};

pub struct MetricSubsystemFactory {
	registry: Arc<MetricRegistry>,
	static_registry: Arc<StaticMetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
}

impl MetricSubsystemFactory {
	pub fn new(
		registry: Arc<MetricRegistry>,
		static_registry: Arc<StaticMetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
	) -> Self {
		Self {
			registry,
			static_registry,
			accumulator,
		}
	}
}

impl SubsystemFactory for MetricSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let runtime = ioc.resolve::<SharedRuntime>()?;
		let event_bus = ioc.resolve::<EventBus>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let actor_system = runtime.actor_system();

		let actor = MetricCollectorActor::new(
			self.registry,
			self.static_registry,
			self.accumulator,
			event_bus.clone(),
			single_store,
			multi_store,
		);
		let handle = actor_system.spawn_background("metric-collector", actor);
		let actor_ref = handle.actor_ref().clone();

		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref.clone()));
		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref.clone()));
		event_bus.register::<ProfilerSnapshotEvent, _>(ProfilerSnapshotListener::new(actor_ref));

		match ioc.resolve::<StandardEngine>() {
			Ok(engine) => {
				engine.register_virtual_table(
					NamespaceId::SYSTEM_METRICS_PROFILER,
					"categories",
					MetricsProfilerCategoriesVTable::new(),
				)?;
			}
			Err(e) => {
				warn!(
					"StandardEngine not available in IoC; profiler categories vtable not registered: {e}"
				);
			}
		}

		Ok(Box::new(MetricSubsystem::new()))
	}
}
