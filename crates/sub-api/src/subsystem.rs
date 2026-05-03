// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::any::Any;

use reifydb_core::{interface::version::HasVersion, util::ioc::IocContainer};
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_type::Result;

pub trait Subsystem: Any + HasVersion {
	fn name(&self) -> &'static str;

	fn start(&mut self) -> Result<()>;

	fn shutdown(&mut self) -> Result<()>;

	fn is_running(&self) -> bool;

	fn health_status(&self) -> HealthStatus;

	fn as_any(&self) -> &dyn Any;

	fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait SubsystemFactory: Send {
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
	Healthy,
	Warning {
		description: String,
	},
	Degraded {
		description: String,
	},
	Failed {
		description: String,
	},
	Unknown,
}

impl HealthStatus {
	pub fn is_healthy(&self) -> bool {
		matches!(self, HealthStatus::Healthy)
	}

	pub fn is_failed(&self) -> bool {
		matches!(self, HealthStatus::Failed { .. })
	}

	pub fn description(&self) -> &str {
		match self {
			HealthStatus::Healthy => "Healthy",
			HealthStatus::Warning {
				description: message,
			} => message,
			HealthStatus::Degraded {
				description: message,
			} => message,
			HealthStatus::Failed {
				description: message,
			} => message,
			HealthStatus::Unknown => "Unknown",
		}
	}
}
