// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{Runtime, actor::system::ActorSpawner, context::clock::Clock};
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use crate::{
	actor::RuntimeSamplerActor, collect::Collectors, domain::Domain, subsystem::RuntimeSubsystem,
	vtable::RuntimeVTable,
};

pub struct RuntimeSubsystemFactory {
	runtime: Runtime,
}

impl RuntimeSubsystemFactory {
	pub fn new(runtime: Runtime) -> Self {
		Self {
			runtime,
		}
	}
}

impl SubsystemFactory for RuntimeSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let clock = ioc.resolve::<Clock>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			multi_store,
		};

		for domain in Domain::ALL {
			let vtable = RuntimeVTable::new(collectors.clone(), clock.clone(), domain);
			engine.register_virtual_table(domain.namespace(), "current", vtable)?;
		}

		let interval = engine.catalog().get_config_duration_opt(ConfigKey::RuntimeMetricsInterval);
		let sampler_scope = interval.map(|interval| {
			let scope = spawner.scope();
			let actor = RuntimeSamplerActor::new(collectors, engine, interval);
			scope.spawn_background("runtime-sampler", actor);
			scope
		});

		Ok(Box::new(RuntimeSubsystem::new(sampler_scope, self.runtime)))
	}
}
