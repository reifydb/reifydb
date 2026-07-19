// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::once, sync::Arc};

use reifydb_core::{
	actors::metrics::MetricsMessage,
	event::{
		EventBus,
		metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent},
	},
	interface::catalog::config::{ConfigKey, GetConfig},
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metrics::accumulator::StatementMetricsAccumulator;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	context::clock::Clock,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use crate::{
	actor::MetricsFlushActor,
	domains::{
		instruments::InstrumentsSource,
		read_buffer::read_buffer_sources,
		runtime::{SampleReader, collect::Collectors, runtime_sources},
	},
	framework::{current::CurrentVTable, source::MetricsSource},
	listener::{CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, RequestMetricsEventListener},
	sampler::MetricsSamplerActor,
	subsystem::MetricsSubsystem,
};

pub struct MetricsSubsystemFactory;

impl MetricsSubsystemFactory {
	pub fn new() -> Self {
		Self
	}
}

impl Default for MetricsSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for MetricsSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let registry = ioc.resolve::<MetricsRegistry>()?;
		let clock = ioc.resolve::<Clock>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			registry,
		};

		Self::register_current_vtables(&engine, &clock, &collectors, &multi_store)?;
		Self::wire_accounting(ioc, &engine, &spawner)?;

		let reader = SampleReader::new(collectors);
		Self::wire_sampler(&engine, &spawner, &clock, &reader);

		Ok(Box::new(MetricsSubsystem::new(reader)))
	}
}

impl MetricsSubsystemFactory {
	#[inline]
	fn register_current_vtables(
		engine: &StandardEngine,
		clock: &Clock,
		collectors: &Collectors,
		multi_store: &MultiStore,
	) -> Result<()> {
		let instruments: Arc<dyn MetricsSource> = Arc::new(InstrumentsSource::new(collectors.registry.clone()));
		for source in runtime_sources(collectors)
			.into_iter()
			.chain(read_buffer_sources(multi_store))
			.chain(once(instruments))
		{
			let namespace = source.namespace();
			engine.register_virtual_table(namespace, "current", CurrentVTable::new(source, clock.clone()))?;
		}
		Ok(())
	}

	#[inline]
	fn wire_accounting(ioc: &IocContainer, engine: &StandardEngine, spawner: &ActorSpawner) -> Result<()> {
		let Some(accumulator) = ioc.try_resolve::<Arc<StatementMetricsAccumulator>>() else {
			return Ok(());
		};

		let event_bus = ioc.resolve::<EventBus>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;

		let clock = ioc.resolve::<Clock>()?;
		let actor = MetricsFlushActor::new(accumulator, event_bus.clone(), single_store, multi_store)
			.with_drain(engine.clone(), clock)
			.with_config(Arc::new(engine.catalog()) as Arc<dyn GetConfig>);

		let handle = spawner.spawn_coordination("metrics-flush", actor);
		Self::register_listeners(&event_bus, handle.actor_ref().clone());

		Ok(())
	}

	#[inline]
	fn register_listeners(event_bus: &EventBus, actor_ref: ActorRef<MetricsMessage>) {
		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref.clone()));
		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref));
	}

	#[inline]
	fn wire_sampler(engine: &StandardEngine, spawner: &ActorSpawner, clock: &Clock, reader: &SampleReader) {
		let config = engine.catalog();
		let Some(interval) = config.get_config_duration_opt(ConfigKey::MetricsSampleInterval) else {
			return;
		};
		let actor = MetricsSamplerActor::new(engine.clone(), reader.clone(), clock.clone(), interval);
		spawner.spawn_coordination("metrics-sampler", actor);
	}
}
