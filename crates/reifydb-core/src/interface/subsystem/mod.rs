// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;
use crate::interceptor::StandardInterceptorBuilder;
use crate::interface::Transaction;
use crate::ioc::IocContainer;

pub mod logging;

/// Uniform interface that all subsystems must implement
///
/// This trait provides a consistent lifecycle and monitoring interface
/// for all subsystems managed by the Database.
pub trait Subsystem: Send + Sync + Any {
	/// Get the unique name of this subsystem
	fn name(&self) -> &'static str;
	/// Start the subsystem
	///
	/// This method should initialize the subsystem and start any background
	/// threads or processes. It should be idempotent - calling start() on
	/// an already running subsystem should succeed without side effects.
	fn start(&mut self) -> crate::Result<()>;
	/// Shutdown the subsystem
	///
	/// This method should gracefully shut down the subsystem and clean up
	/// any resources. This is a terminal operation - once shutdown, the
	/// subsystem cannot be restarted. It should be idempotent - calling 
	/// shutdown() on an already shutdown subsystem should succeed without 
	/// side effects.
	fn shutdown(&mut self) -> crate::Result<()>;

	/// Check if the subsystem is currently running
	fn is_running(&self) -> bool;

	/// Get the current health status of the subsystem
	///
	/// This should provide information about the subsystem's operational
	/// status and any errors or warnings.
	fn health_status(&self) -> HealthStatus;

	/// Get a reference to self as Any for downcasting
	fn as_any(&self) -> &dyn Any;

	/// Get a mutable reference to self as Any for downcasting
	fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Factory trait for creating subsystems with IoC support
pub trait SubsystemFactory<T: Transaction> {
	
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T>{
		builder
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> crate::Result<Box<dyn Subsystem>>;
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
