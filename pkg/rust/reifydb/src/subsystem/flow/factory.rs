// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, time::Duration};

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{ConsumerId, Transaction},
	ioc::IocContainer,
};
use reifydb_engine::StandardEngine;

use super::{FlowSubsystem, intercept::TransactionalFlowInterceptor};
use crate::subsystem::{Subsystem, factory::SubsystemFactory};

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

impl<T: Transaction> SubsystemFactory<T> for FlowSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		let interceptor =
			TransactionalFlowInterceptor::new(ioc.clone());

		builder.add_table_post_insert(interceptor.clone())
			.add_table_post_update(interceptor.clone())
			.add_table_post_delete(interceptor.clone())
			.add_pre_commit(interceptor)
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		Ok(Box::new(FlowSubsystem::new(
			engine,
			self.consumer_id,
			self.poll_interval,
		)))
	}
}
