// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating OpenTelemetry subsystem instances.

use reifydb_core::ioc::IocContainer;
use reifydb_core::SharedRuntime;
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use crate::{config::OtelConfig, subsystem::OtelSubsystem};

/// Factory for creating OpenTelemetry subsystem instances.
pub struct OtelSubsystemFactory {
	subsystem: Option<OtelSubsystem>,
	config: Option<OtelConfig>,
}

impl OtelSubsystemFactory {
	/// Create a new OpenTelemetry subsystem factory with the given configuration.
	pub fn new(config: OtelConfig) -> Self {
		Self {
			subsystem: None,
			config: Some(config),
		}
	}

	/// Create a factory that wraps an already-initialized subsystem.
	/// Used by `with_tracing_otel()` builder method.
	pub fn with_subsystem(subsystem: OtelSubsystem) -> Self {
		Self {
			subsystem: Some(subsystem),
			config: None,
		}
	}
}

impl SubsystemFactory for OtelSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		if let Some(subsystem) = self.subsystem {
			// Subsystem already created and started
			Ok(Box::new(subsystem))
		} else if let Some(config) = self.config {
			// Normal path: create new subsystem
			let runtime = ioc.resolve::<SharedRuntime>()?;
			let subsystem = OtelSubsystem::new(config, runtime);
			Ok(Box::new(subsystem))
		} else {
			unreachable!("OtelSubsystemFactory must have either subsystem or config")
		}
	}
}
