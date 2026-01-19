use reifydb_core::util::ioc::IocContainer;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::builder::StandardInterceptorBuilder;

use crate::{subsystem::TaskSubsystem, task::ScheduledTask};

/// Configuration for the task scheduler subsystem
#[derive(Default)]
pub struct TaskConfig {
	/// Tasks to register at startup
	tasks: Vec<ScheduledTask>,
}

impl TaskConfig {
	/// Create a new task configuration
	pub fn new(tasks: Vec<ScheduledTask>) -> Self {
		Self {
			tasks,
		}
	}
}

/// Factory for creating TaskSubsystem instances
pub struct TaskSubsystemFactory {
	config: TaskConfig,
}

impl TaskSubsystemFactory {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self {
			config: TaskConfig::default(),
		}
	}

	/// Create a factory with custom configuration
	pub fn with_config(config: TaskConfig) -> Self {
		Self {
			config,
		}
	}
}

impl Default for TaskSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for TaskSubsystemFactory {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder {
		// Task subsystem doesn't need any special interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		Ok(Box::new(TaskSubsystem::new(ioc, self.config.tasks)))
	}
}
