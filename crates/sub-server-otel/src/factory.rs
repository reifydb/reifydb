// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Factory for creating OpenTelemetry subsystem instances.

use reifydb_core::util::ioc::IocContainer;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{
	config::{OtelConfig, OtelConfigurator},
	subsystem::OtelSubsystem,
};

/// Factory for creating OpenTelemetry subsystem instances.
pub struct OtelSubsystemFactory {
	subsystem: Option<OtelSubsystem>,
	config_fn: Option<Box<dyn FnOnce() -> OtelConfig + Send>>,
}

impl OtelSubsystemFactory {
	/// Create a new OpenTelemetry subsystem factory with the given configurator.
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static,
	{
		Self {
			subsystem: None,
			config_fn: Some(Box::new(move || configurator(OtelConfigurator::new()).configure())),
		}
	}

	/// Create a factory that wraps an already-initialized subsystem.
	/// Used by `with_tracing_otel()` builder method.
	pub fn with_subsystem(subsystem: OtelSubsystem) -> Self {
		Self {
			subsystem: Some(subsystem),
			config_fn: None,
		}
	}
}

impl SubsystemFactory for OtelSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		if let Some(subsystem) = self.subsystem {
			// Subsystem already created and started
			Ok(Box::new(subsystem))
		} else if let Some(config_fn) = self.config_fn {
			// Normal path: create new subsystem
			let runtime = ioc.resolve::<SharedRuntime>()?;
			let config = config_fn();
			let subsystem = OtelSubsystem::new(config, runtime);
			Ok(Box::new(subsystem))
		} else {
			unreachable!("OtelSubsystemFactory must have either subsystem or config_fn")
		}
	}
}
