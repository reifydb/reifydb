// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Factory for creating OpenTelemetry subsystem instances.

use reifydb_core::ioc::IocContainer;
use reifydb_engine::StandardCommandTransaction;
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

impl SubsystemFactory<StandardCommandTransaction> for OtelSubsystemFactory {
	fn create(self: Box<Self>, _ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		if let Some(subsystem) = self.subsystem {
			// Subsystem already created and started
			Ok(Box::new(subsystem))
		} else if let Some(config) = self.config {
			// Normal path: create new subsystem
			let subsystem = OtelSubsystem::new(config);
			Ok(Box::new(subsystem))
		} else {
			unreachable!("OtelSubsystemFactory must have either subsystem or config")
		}
	}
}
