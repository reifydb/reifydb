// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{
		ConsumerId, Transaction,
		subsystem::{
			Subsystem, SubsystemFactory, workerpool::Priority,
		},
	},
	ioc::IocContainer,
};
use reifydb_engine::StandardCommandTransaction;

use super::{
	FlowSubsystem, FlowSubsystemConfig,
	intercept::TransactionalFlowInterceptor,
};

/// Factory for creating FlowSubsystem with proper interceptor registration
#[derive()]
pub struct FlowSubsystemFactory {
	config: FlowSubsystemConfig,
}

impl FlowSubsystemFactory {
	pub fn new() -> Self {
		Self {
			config: FlowSubsystemConfig {
				consumer_id: ConsumerId::flow_consumer(),
				poll_interval: Duration::from_millis(1),
				priority: Priority::Normal,
			},
		}
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>>
	for FlowSubsystemFactory
{
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<
			StandardCommandTransaction<T>,
		>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<StandardCommandTransaction<T>> {
		let ioc = ioc.clone();
		builder.add_factory(move |interceptors| {
			interceptors.register(
				TransactionalFlowInterceptor::<T>::new(
					ioc.clone(),
				),
			);
		})
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> crate::Result<Box<dyn Subsystem>> {
		Ok(Box::new(FlowSubsystem::<T>::new(self.config, ioc)?))
	}
}
