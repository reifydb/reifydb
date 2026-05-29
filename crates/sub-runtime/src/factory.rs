// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use crate::{
	actor::RuntimeSamplerActor, collect::Collectors, domain::Domain, subsystem::RuntimeSubsystem,
	vtable::RuntimeVTable,
};

pub struct RuntimeSubsystemFactory;

impl RuntimeSubsystemFactory {
	pub fn new() -> Self {
		Self
	}
}

impl Default for RuntimeSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for RuntimeSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let runtime = ioc.resolve::<SharedRuntime>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			multi_store,
		};

		for domain in Domain::ALL {
			let vtable = RuntimeVTable::new(collectors.clone(), runtime.clock().clone(), domain);
			engine.register_virtual_table(domain.namespace(), "current", vtable)?;
		}

		let interval = engine.catalog().get_config_duration_opt(ConfigKey::RuntimeMetricsInterval);
		if let Some(interval) = interval {
			let actor = RuntimeSamplerActor::new(collectors, engine, interval);
			runtime.actor_system().spawn_background("runtime-sampler", actor);
		}

		Ok(Box::new(RuntimeSubsystem::new(interval.is_some())))
	}
}
