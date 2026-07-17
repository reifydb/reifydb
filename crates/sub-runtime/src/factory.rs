// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::HashSet;

use reifydb_core::util::{ioc::IocContainer, memory::MemoryRegistry};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{Runtime, context::clock::Clock};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::{Result, reifydb_assertions};

use crate::{collect::Collectors, domain::Domain, subsystem::RuntimeSubsystem, vtable::RuntimeVTable};

type RuntimeDependencies = (StandardEngine, Clock, Collectors);

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
		let registry = ioc.resolve::<MemoryRegistry>()?;
		let clock = ioc.resolve::<Clock>()?;

		let collectors = Collectors {
			engine: engine.clone(),
			registry,
		};

		Ok((engine, clock, collectors))
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
}

impl SubsystemFactory for RuntimeSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let (engine, clock, collectors) = Self::resolve_dependencies(ioc)?;

		Self::register_per_domain_vtables(&engine, &clock, &collectors)?;

		Ok(Box::new(RuntimeSubsystem::new(self.runtime, collectors)))
	}
}
