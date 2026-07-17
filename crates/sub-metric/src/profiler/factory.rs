// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId, util::ioc::IocContainer};
use reifydb_engine::engine::StandardEngine;
use reifydb_profiler::{
	event::{ProfilerScopeBatchEvent, ProfilerScopeClosedEvent},
	intern::DimInterner,
	sink::{NoopSink, ProfilerSink},
};
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock, sync::rwlock::RwLock};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use super::{
	accumulator::ProfilerAccumulator,
	actor::ProfilerCollectorActor,
	builder::ProfilerConfigurator,
	listener::{ProfilerScopeBatchListener, ProfilerScopeClosedListener},
	reader::ProfilerReader,
	sink::EventBusSink,
	subsystem::ProfilerSubsystem,
	vtable::ProfilerSpansVTable,
};

type Configurator = Box<dyn FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send>;

pub struct ProfilerSubsystemFactory {
	subsystem: Option<ProfilerSubsystem>,
	configurator: Option<Configurator>,
}

impl ProfilerSubsystemFactory {
	pub fn new() -> Self {
		Self {
			subsystem: None,
			configurator: None,
		}
	}

	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send + 'static,
	{
		Self {
			subsystem: None,
			configurator: Some(Box::new(configurator)),
		}
	}

	pub fn with_subsystem(subsystem: ProfilerSubsystem) -> Self {
		Self {
			subsystem: Some(subsystem),
			configurator: None,
		}
	}

	#[inline]
	fn build_subsystem(configurator: Option<Configurator>, ioc: &IocContainer) -> Result<ProfilerSubsystem> {
		let cfg = match configurator {
			Some(f) => f(ProfilerConfigurator::new()),
			None => ProfilerConfigurator::default(),
		};

		let interner = Arc::new(DimInterner::new());
		let accumulator = Arc::new(RwLock::new(ProfilerAccumulator::new(
			cfg.accumulator_capacity,
			cfg.min_calls_for_retention,
		)));
		let event_bus = ioc.resolve::<EventBus>()?;
		let clock = ioc.resolve::<Clock>()?;

		if cfg.enabled {
			let spawner = ioc.resolve::<ActorSpawner>()?;
			let actor = ProfilerCollectorActor::new(Arc::clone(&accumulator), Arc::clone(&interner));
			let handle = spawner.spawn_coordination("profile-collector", actor);
			let actor_ref = handle.actor_ref().clone();

			event_bus.register::<ProfilerScopeClosedEvent, _>(ProfilerScopeClosedListener::new(
				actor_ref.clone(),
			));
			event_bus.register::<ProfilerScopeBatchEvent, _>(ProfilerScopeBatchListener::new(actor_ref));
		}

		let sink: Arc<dyn ProfilerSink> = if cfg.enabled {
			Arc::new(EventBusSink::new(event_bus))
		} else {
			Arc::new(NoopSink)
		};

		Ok(ProfilerSubsystem::new(cfg.enabled, cfg.categories, interner, accumulator, sink, clock.clone()))
	}
}

impl Default for ProfilerSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for ProfilerSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let subsystem = match self.subsystem {
			Some(subsystem) => subsystem,
			None => Self::build_subsystem(self.configurator, ioc)?,
		};

		let engine = ioc.resolve::<StandardEngine>()?;
		register_spans_vtable(&engine, &subsystem.reader())?;

		Ok(Box::new(subsystem))
	}
}

fn register_spans_vtable(engine: &StandardEngine, reader: &ProfilerReader) -> Result<()> {
	engine.register_virtual_table(
		NamespaceId::SYSTEM_METRICS_PROFILER_SPANS,
		"current",
		ProfilerSpansVTable::new(reader.clone()),
	)?;
	Ok(())
}
