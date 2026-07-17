// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::metric::MetricMessage,
	event::{
		EventBus,
		metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent},
	},
	interface::catalog::{config::GetConfig, id::NamespaceId},
	util::{ioc::IocContainer, memory::MemoryRegistry},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	context::clock::Clock,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use crate::{
	actor::MetricCollectorActor,
	domains::runtime::{SampleReader, collect::Collectors, runtime_sources},
	framework::current::CurrentVTable,
	listener::{CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, RequestMetricsEventListener},
	profiler_vtable::MetricsProfilerCategoriesVTable,
	subsystem::MetricSubsystem,
};

pub struct MetricSubsystemFactory;

impl MetricSubsystemFactory {
	pub fn new() -> Self {
		Self
	}
}

impl Default for MetricSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for MetricSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let registry = ioc.resolve::<MemoryRegistry>()?;
		let clock = ioc.resolve::<Clock>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			registry,
		};

		Self::register_current_vtables(&engine, &clock, &collectors)?;
		Self::wire_accounting(ioc, &engine, &spawner)?;

		Ok(Box::new(MetricSubsystem::new(SampleReader::new(collectors))))
	}
}

impl MetricSubsystemFactory {
	#[inline]
	fn register_current_vtables(engine: &StandardEngine, clock: &Clock, collectors: &Collectors) -> Result<()> {
		for source in runtime_sources(collectors) {
			let namespace = source.namespace();
			engine.register_virtual_table(namespace, "current", CurrentVTable::new(source, clock.clone()))?;
		}
		Ok(())
	}

	#[inline]
	fn wire_accounting(ioc: &IocContainer, engine: &StandardEngine, spawner: &ActorSpawner) -> Result<()> {
		let (Some(registry), Some(static_registry), Some(accumulator)) = (
			ioc.try_resolve::<Arc<MetricRegistry>>(),
			ioc.try_resolve::<Arc<StaticMetricRegistry>>(),
			ioc.try_resolve::<Arc<StatementStatsAccumulator>>(),
		) else {
			return Ok(());
		};

		let event_bus = ioc.resolve::<EventBus>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;

		let actor = MetricCollectorActor::new(
			registry,
			static_registry,
			accumulator,
			event_bus.clone(),
			single_store,
			multi_store,
		)
		.with_config(Arc::new(engine.catalog()) as Arc<dyn GetConfig>);

		let handle = spawner.spawn_coordination("metric-collector", actor);
		Self::register_listeners(&event_bus, handle.actor_ref().clone());

		engine.register_virtual_table(
			NamespaceId::SYSTEM_METRICS_PROFILER_CATEGORIES,
			"current",
			MetricsProfilerCategoriesVTable::new(),
		)?;
		Ok(())
	}

	#[inline]
	fn register_listeners(event_bus: &EventBus, actor_ref: ActorRef<MetricMessage>) {
		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref.clone()));
		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref));
	}
}
