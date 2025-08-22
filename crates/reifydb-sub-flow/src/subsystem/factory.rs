// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, time::Duration};

use reifydb_cdc::PollConsumerConfig;
use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{
		ConsumerId, Transaction,
		subsystem::{Subsystem, SubsystemFactory},
	},
	ioc::IocContainer,
	transaction::StandardCommandTransaction,
};
use reifydb_engine::StandardEngine;

use super::{FlowSubsystem, intercept::TransactionalFlowInterceptor};

/// Factory for creating FlowSubsystem with proper interceptor registration
#[derive()]
pub struct FlowSubsystemFactory<T: Transaction> {
	consumer_id: ConsumerId,
	poll_interval: Duration,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> FlowSubsystemFactory<T> {
	pub fn new() -> Self {
		Self {
			consumer_id: ConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			_phantom: PhantomData,
		}
	}

	pub fn with_poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
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
			interceptors.register(
				TransactionalFlowInterceptor::<T>::new(ioc.clone()),
			);
		})
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;
		let poll_config = PollConsumerConfig::new(
			self.consumer_id.clone(),
			self.poll_interval,
		);
		Ok(Box::new(FlowSubsystem::with_config(
			engine,
			self.consumer_id,
			poll_config,
		)))
	}
}
