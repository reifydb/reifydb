// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::HashSet;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{Runtime, actor::system::ActorSpawner, context::clock::Clock};
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::{Result, reifydb_assertions};

use crate::{
	actor::RuntimeSamplerActor, collect::Collectors, domain::Domain, subsystem::RuntimeSubsystem,
	vtable::RuntimeVTable,
};

type RuntimeDependencies = (StandardEngine, ActorSpawner, Clock, Collectors);

pub struct RuntimeSubsystemFactory {
	runtime: Runtime,
}

impl RuntimeSubsystemFactory {
	pub fn new(runtime: Runtime) -> Self {
		Self {
			runtime,
		}
	}

	#[inline]
	fn resolve_dependencies(ioc: &IocContainer) -> Result<RuntimeDependencies> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let clock = ioc.resolve::<Clock>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			multi_store,
		};

		Ok((engine, spawner, clock, collectors))
	}

	#[inline]
	fn register_per_domain_vtables(engine: &StandardEngine, clock: &Clock, collectors: &Collectors) -> Result<()> {
		reifydb_assertions! {
			let mut namespaces = HashSet::new();
			for domain in Domain::ALL {
				assert!(
					namespaces.insert(domain.namespace()),
					"Domain::ALL maps two domains to the same namespace {:?}; \
					 each per-domain `current` vtable is registered under domain.namespace(), \
					 so a collision silently overwrites the first registration",
					domain.namespace()
				);
			}
		}

		for domain in Domain::ALL {
			let vtable = RuntimeVTable::new(collectors.clone(), clock.clone(), domain);
			engine.register_virtual_table(domain.namespace(), "current", vtable)?;
		}
		Ok(())
	}

	#[inline]
	fn maybe_spawn_sampler(
		engine: StandardEngine,
		spawner: &ActorSpawner,
		collectors: Collectors,
	) -> Option<ActorSpawner> {
		let interval = engine.catalog().get_config_duration_opt(ConfigKey::RuntimeMetricsInterval);
		interval.map(|interval| {
			let scope = spawner.scope();
			let actor = RuntimeSamplerActor::new(collectors, engine, interval);
			scope.spawn_background("runtime-sampler", actor);
			scope
		})
	}
}

impl SubsystemFactory for RuntimeSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let (engine, spawner, clock, collectors) = Self::resolve_dependencies(ioc)?;

		Self::register_per_domain_vtables(&engine, &clock, &collectors)?;

		let sampler_scope = Self::maybe_spawn_sampler(engine, &spawner, collectors);

		Ok(Box::new(RuntimeSubsystem::new(sampler_scope, self.runtime)))
	}
}
