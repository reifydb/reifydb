// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_type::Result;

use crate::{subsystem::TaskSubsystem, task::ScheduledTask};

#[derive(Default)]
pub struct TaskConfig {
	tasks: Vec<ScheduledTask>,
}

impl TaskConfig {
	pub fn new(tasks: Vec<ScheduledTask>) -> Self {
		Self {
			tasks,
		}
	}
}

pub struct TaskSubsystemFactory {
	config: TaskConfig,
}

impl TaskSubsystemFactory {
	pub fn new() -> Self {
		Self {
			config: TaskConfig::default(),
		}
	}

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
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		Ok(Box::new(TaskSubsystem::new(ioc, self.config.tasks)))
	}
}
