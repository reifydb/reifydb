// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::metrics::MetricsMessage,
	event::{
		EventBus,
		metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent},
	},
	interface::catalog::config::GetConfig,
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
		runtime::{Domain, SampleReader, collect::Collectors, runtime_source},
	},
	framework::{
		current::{CurrentCache, CurrentVTable},
		source::MetricsSource,
	},
	listener::{CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, RequestMetricsEventListener},
	refresh::{RefreshActor, RefreshDomain},
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

		Self::wire_refresh(&engine, &spawner, &clock, &collectors, &multi_store)?;
		Self::wire_accounting(ioc, &engine, &spawner)?;

		Ok(Box::new(MetricsSubsystem::new(SampleReader::new(collectors))))
	}
}

impl MetricsSubsystemFactory {
	#[inline]
	fn wire_refresh(
		engine: &StandardEngine,
		spawner: &ActorSpawner,
		clock: &Clock,
		collectors: &Collectors,
		multi_store: &MultiStore,
	) -> Result<()> {
		let config = engine.catalog();
		for domain in RefreshDomain::ALL {
			let sources = Self::sources_for(domain, collectors, multi_store);
			let mut targets = Vec::with_capacity(sources.len());
			for source in sources {
				let cache = CurrentCache::new(source.columns());
				engine.register_virtual_table(
					source.namespace(),
					"current",
					CurrentVTable::new(cache.clone()),
				)?;
				targets.push((source, cache));
			}

			if let Some(interval) = config.get_config_duration_opt(domain.config_key()) {
				let actor = RefreshActor::new(targets, clock.clone(), interval);
				spawner.spawn_coordination(domain.actor_name(), actor);
			}
		}
		Ok(())
	}

	#[inline]
	fn sources_for(
		domain: RefreshDomain,
		collectors: &Collectors,
		multi_store: &MultiStore,
	) -> Vec<Arc<dyn MetricsSource>> {
		match domain {
			RefreshDomain::RuntimeMemory => vec![runtime_source(Domain::Memory, collectors)],
			RefreshDomain::RuntimeWatermarks => vec![runtime_source(Domain::Watermarks, collectors)],
			RefreshDomain::RuntimeOperators => vec![runtime_source(Domain::Operators, collectors)],
			RefreshDomain::ReadBuffer => read_buffer_sources(multi_store),
			RefreshDomain::Instruments => {
				vec![Arc::new(InstrumentsSource::new(collectors.registry.clone()))
					as Arc<dyn MetricsSource>]
			}
		}
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
}
