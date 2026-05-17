// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use parking_lot::RwLock;
use reifydb_core::{event::EventBus, util::ioc::IocContainer};
use reifydb_profiler::{
	event::{ProfileScopeBatchEvent, ProfileScopeClosedEvent},
	intern::DimInterner,
	sink::{NoopSink, ProfileSink},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{
	accumulator::ProfileAccumulator,
	actor::ProfileCollectorActor,
	builder::ProfilerConfigurator,
	listener::{ProfileScopeBatchListener, ProfileScopeClosedListener},
	sink::EventBusSink,
	subsystem::ProfilerSubsystem,
};

pub struct ProfilerSubsystemFactory {
	subsystem: Option<ProfilerSubsystem>,
	configurator: Option<Box<dyn FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send>>,
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
}

impl Default for ProfilerSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for ProfilerSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		if let Some(subsystem) = self.subsystem {
			return Ok(Box::new(subsystem));
		}

		let cfg = match self.configurator {
			Some(f) => f(ProfilerConfigurator::new()),
			None => ProfilerConfigurator::default(),
		};

		let interner = Arc::new(DimInterner::new());
		let accumulator = Arc::new(RwLock::new(ProfileAccumulator::new(
			cfg.accumulator_capacity,
			cfg.min_calls_for_retention,
		)));
		let event_bus = ioc.resolve::<EventBus>()?;
		let runtime = ioc.resolve::<SharedRuntime>()?;

		if cfg.enabled {
			let actor = ProfileCollectorActor::new(Arc::clone(&accumulator), Arc::clone(&interner));
			let handle = runtime.actor_system().spawn_system("profile-collector", actor);
			let actor_ref = handle.actor_ref().clone();

			event_bus.register::<ProfileScopeClosedEvent, _>(ProfileScopeClosedListener::new(
				actor_ref.clone(),
			));
			event_bus.register::<ProfileScopeBatchEvent, _>(ProfileScopeBatchListener::new(actor_ref));
		}

		let sink: Arc<dyn ProfileSink> = if cfg.enabled {
			Arc::new(EventBusSink::new(event_bus))
		} else {
			Arc::new(NoopSink)
		};

		Ok(Box::new(ProfilerSubsystem::new(cfg.enabled, cfg.categories, interner, accumulator, sink)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_profiler::category::CategorySet;

	use super::*;

	#[test]
	fn with_subsystem_returns_provided() {
		let interner = Arc::new(DimInterner::new());
		let accumulator = Arc::new(RwLock::new(ProfileAccumulator::new(16, 0)));
		let sink: Arc<dyn ProfileSink> = Arc::new(NoopSink);
		let subsystem = ProfilerSubsystem::new(false, CategorySet::empty(), interner, accumulator, sink);

		let factory = Box::new(ProfilerSubsystemFactory::with_subsystem(subsystem));
		let ioc = IocContainer::new();
		let result = factory.create(&ioc).expect("create should succeed without IoC resolution");
		let downcast = result.as_any().downcast_ref::<ProfilerSubsystem>();
		assert!(downcast.is_some(), "returned subsystem must be ProfilerSubsystem");
		assert!(!downcast.unwrap().is_running());
	}
}
