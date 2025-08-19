// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{ConsumerId, Transaction},
};
use reifydb_engine::StandardEngine;

use super::{FlowSubsystem, intercept::TransactionalFlowInterceptor};
use crate::{
	ioc::IocContainer,
	subsystem::{Subsystem, factory::SubsystemFactory},
};

/// Factory for creating FlowSubsystem with proper interceptor registration
#[derive()]
pub struct FlowSubsystemFactory<T: Transaction> {
	consumer_id: ConsumerId,
	poll_interval: Duration,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> FlowSubsystemFactory<T> {
	pub fn new() -> Self {
		Self {
			consumer_id: ConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			_phantom: std::marker::PhantomData,
		}
	}

	pub fn with_poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}
}

impl<T: Transaction> SubsystemFactory<T> for FlowSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// Create interceptor with IoC container
		let interceptor =
			TransactionalFlowInterceptor::new(ioc.clone());

		builder.add_table_post_insert(interceptor.clone())
			.add_table_post_update(interceptor.clone())
			.add_table_post_delete(interceptor.clone())
			.add_pre_commit(interceptor)
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Box<dyn Subsystem> {
		// Get engine from IoC
		let engine = ioc
			.resolve::<StandardEngine<T>>()
			.expect("StandardEngine must be registered in IoC");

		Box::new(FlowSubsystem::new(
			engine,
			self.consumer_id,
			self.poll_interval,
		))
	}
}
