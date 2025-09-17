// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{
		Transaction,
		subsystem::{Subsystem, SubsystemFactory},
	},
	ioc::IocContainer,
};
use reifydb_engine::StandardCommandTransaction;

use super::{FlowSubsystem, intercept::TransactionalFlowInterceptor};
use crate::builder::FlowBuilder;

/// Configuration function for the flow subsystem
pub type FlowConfigurator<T> = Box<dyn FnOnce(FlowBuilder<T>) -> FlowBuilder<T> + Send>;

/// Factory for creating FlowSubsystem with proper interceptor registration
pub struct FlowSubsystemFactory<T: Transaction> {
	configurator: Option<FlowConfigurator<T>>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> FlowSubsystemFactory<T> {
	pub fn new() -> Self {
		Self {
			configurator: None,
			_phantom: PhantomData,
		}
	}

	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder<T>) -> FlowBuilder<T> + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> Default for FlowSubsystemFactory<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>> for FlowSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<StandardCommandTransaction<T>> {
		let ioc = ioc.clone();
		builder.add_factory(move |interceptors| {
			interceptors.register(TransactionalFlowInterceptor::<T>::new(ioc.clone()));
		})
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> crate::Result<Box<dyn Subsystem>> {
		let builder = if let Some(configurator) = self.configurator {
			configurator(FlowBuilder::new())
		} else {
			FlowBuilder::default()
		};
		let config = builder.build_config();
		Ok(Box::new(FlowSubsystem::<T>::new(config, ioc)?))
	}
}
